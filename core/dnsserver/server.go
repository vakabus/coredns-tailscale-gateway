// Package dnsserver implements all the interfaces from Caddy, so that CoreDNS can be a servertype plugin.
package dnsserver

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/metrics/vars"
	"github.com/coredns/coredns/plugin/pkg/edns"
	"github.com/coredns/coredns/plugin/pkg/log"
	"github.com/coredns/coredns/plugin/pkg/rcode"
	"github.com/coredns/coredns/plugin/pkg/trace"
	"github.com/coredns/coredns/plugin/pkg/transport"
	"github.com/coredns/coredns/plugin/tailscale"
	"github.com/coredns/coredns/request"

	"github.com/miekg/dns"
	ot "github.com/opentracing/opentracing-go"
)

// Server represents an instance of a server, which serves
// DNS requests at a particular address (host and port). A
// server is capable of serving numerous zones on
// the same address and the listener may be stopped for
// graceful termination (POSIX only).
type Server struct {
	Addr string // Address we listen on

	server [2]*dns.Server // 0 is a net.Listener, 1 is a net.PacketConn (a *UDPConn) in our case.
	m      sync.Mutex     // protects the servers

	zones        map[string][]*Config // zones keyed by their address
	dnsWg        sync.WaitGroup       // used to wait on outstanding connections
	graceTimeout time.Duration        // the maximum duration of a graceful shutdown
	trace        trace.Trace          // the trace plugin for the server
	debug        bool                 // disable recover()
	stacktrace   bool                 // enable stacktrace in recover error log
	classChaos   bool                 // allow non-INET class queries
	idleTimeout  time.Duration        // Idle timeout for TCP
	readTimeout  time.Duration        // Read timeout for TCP
	writeTimeout time.Duration        // Write timeout for TCP

	tsigSecret map[string]string
}

// MetadataCollector is a plugin that can retrieve metadata functions from all metadata providing plugins
type MetadataCollector interface {
	Collect(context.Context, request.Request) context.Context
}

// NewServer returns a new CoreDNS server and compiles all plugins in to it. By default CH class
// queries are blocked unless queries from enableChaos are loaded.
func NewServer(addr string, group []*Config) (*Server, error) {
	s := &Server{
		Addr:         addr,
		zones:        make(map[string][]*Config),
		graceTimeout: 5 * time.Second,
		idleTimeout:  10 * time.Second,
		readTimeout:  3 * time.Second,
		writeTimeout: 5 * time.Second,
		tsigSecret:   make(map[string]string),
	}

	// We have to bound our wg with one increment
	// to prevent a "race condition" that is hard-coded
	// into sync.WaitGroup.Wait() - basically, an add
	// with a positive delta must be guaranteed to
	// occur before Wait() is called on the wg.
	// In a way, this kind of acts as a safety barrier.
	s.dnsWg.Add(1)

	for _, site := range group {
		if site.Debug {
			s.debug = true
			log.D.Set()
		}
		s.stacktrace = site.Stacktrace

		// append the config to the zone's configs
		s.zones[site.Zone] = append(s.zones[site.Zone], site)

		// set timeouts
		if site.ReadTimeout != 0 {
			s.readTimeout = site.ReadTimeout
		}
		if site.WriteTimeout != 0 {
			s.writeTimeout = site.WriteTimeout
		}
		if site.IdleTimeout != 0 {
			s.idleTimeout = site.IdleTimeout
		}

		// copy tsig secrets
		for key, secret := range site.TsigSecret {
			s.tsigSecret[key] = secret
		}

		// compile custom plugin for everything
		var stack plugin.Handler
		for i := len(site.Plugin) - 1; i >= 0; i-- {
			stack = site.Plugin[i](stack)

			// register the *handler* also
			site.registerHandler(stack)

			// If the current plugin is a MetadataCollector, bookmark it for later use. This loop traverses the plugin
			// list backwards, so the first MetadataCollector plugin wins.
			if mdc, ok := stack.(MetadataCollector); ok {
				site.metaCollector = mdc
			}

			if s.trace == nil && stack.Name() == "trace" {
				// we have to stash away the plugin, not the
				// Tracer object, because the Tracer won't be initialized yet
				if t, ok := stack.(trace.Trace); ok {
					s.trace = t
				}
			}
			// Unblock CH class queries when any of these plugins are loaded.
			if _, ok := EnableChaos[stack.Name()]; ok {
				s.classChaos = true
			}
		}
		site.pluginChain = stack
	}

	if !s.debug {
		// When reloading we need to explicitly disable debug logging if it is now disabled.
		log.D.Clear()
	}

	return s, nil
}

// Compile-time check to ensure Server implements the caddy.GracefulServer interface
var _ caddy.GracefulServer = &Server{}

// Serve starts the server with an existing listener. It blocks until the server stops.
// This implements caddy.TCPServer interface.
func (s *Server) Serve(l net.Listener) error {
	s.m.Lock()

	s.server[tcp] = &dns.Server{Listener: l,
		Net:           "tcp",
		TsigSecret:    s.tsigSecret,
		MaxTCPQueries: tcpMaxQueries,
		ReadTimeout:   s.readTimeout,
		WriteTimeout:  s.writeTimeout,
		IdleTimeout: func() time.Duration {
			return s.idleTimeout
		},
		Handler: dns.HandlerFunc(func(w dns.ResponseWriter, r *dns.Msg) {
			ctx := context.WithValue(context.Background(), Key{}, s)
			ctx = context.WithValue(ctx, LoopKey{}, 0)
			s.ServeDNS(ctx, w, r)
		})}

	s.m.Unlock()

	return s.server[tcp].ActivateAndServe()
}

// ServePacket starts the server with an existing packetconn. It blocks until the server stops.
// This implements caddy.UDPServer interface.
func (s *Server) ServePacket(p net.PacketConn) error {
	s.m.Lock()
	s.server[udp] = &dns.Server{PacketConn: p, Net: "udp", Handler: dns.HandlerFunc(func(w dns.ResponseWriter, r *dns.Msg) {
		ctx := context.WithValue(context.Background(), Key{}, s)
		ctx = context.WithValue(ctx, LoopKey{}, 0)
		s.ServeDNS(ctx, w, r)
	}), TsigSecret: s.tsigSecret}
	s.m.Unlock()

	return s.server[udp].ActivateAndServe()
}

// Listen implements caddy.TCPServer interface.
func (s *Server) Listen() (net.Listener, error) {
	if tailscale.Tailscale == nil {
		return nil, fmt.Errorf("server: tailscale plugin not initialized and already trying to listen on %s", s.Addr)
	}

	l, err := tailscale.Tailscale.Listen("tcp", s.Addr[len(transport.DNS+"://"):])
	if err != nil {
		return nil, err
	}
	return l, nil
}

// WrapListener Listen implements caddy.GracefulServer interface.
func (s *Server) WrapListener(ln net.Listener) net.Listener {
	return ln
}

// ListenPacket implements caddy.UDPServer interface.
func (s *Server) ListenPacket() (net.PacketConn, error) {
	if tailscale.Tailscale == nil {
		return nil, fmt.Errorf("server: tailscale plugin not initialized")
	}

	p, err := tailscale.Tailscale.ListenPacket("udp", s.Addr[len(transport.DNS+"://"):])
	if err != nil {
		return nil, err
	}

	return p, nil
}

// Stop stops the server. It blocks until the server is
// totally stopped. On POSIX systems, it will wait for
// connections to close (up to a max timeout of a few
// seconds); on Windows it will close the listener
// immediately.
// This implements Caddy.Stopper interface.
func (s *Server) Stop() (err error) {
	if runtime.GOOS != "windows" {
		// force connections to close after timeout
		done := make(chan struct{})
		go func() {
			s.dnsWg.Done() // decrement our initial increment used as a barrier
			s.dnsWg.Wait()
			close(done)
		}()

		// Wait for remaining connections to finish or
		// force them all to close after timeout
		select {
		case <-time.After(s.graceTimeout):
		case <-done:
		}
	}

	// Close the listener now; this stops the server without delay
	s.m.Lock()
	for _, s1 := range s.server {
		// We might not have started and initialized the full set of servers
		if s1 != nil {
			err = s1.Shutdown()
		}
	}
	s.m.Unlock()
	return
}

// Address together with Stop() implement caddy.GracefulServer.
func (s *Server) Address() string { return s.Addr }

// ServeDNS is the entry point for every request to the address that
// is bound to. It acts as a multiplexer for the requests zonename as
// defined in the request so that the correct zone
// (configuration and plugin stack) will handle the request.
func (s *Server) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) {
	// The default dns.Mux checks the question section size, but we have our
	// own mux here. Check if we have a question section. If not drop them here.
	if r == nil || len(r.Question) == 0 {
		errorAndMetricsFunc(s.Addr, w, r, dns.RcodeServerFailure)
		return
	}

	if !s.debug {
		defer func() {
			// In case the user doesn't enable error plugin, we still
			// need to make sure that we stay alive up here
			if rec := recover(); rec != nil {
				if s.stacktrace {
					log.Errorf("Recovered from panic in server: %q %v\n%s", s.Addr, rec, string(debug.Stack()))
				} else {
					log.Errorf("Recovered from panic in server: %q %v", s.Addr, rec)
				}
				vars.Panic.Inc()
				errorAndMetricsFunc(s.Addr, w, r, dns.RcodeServerFailure)
			}
		}()
	}

	if !s.classChaos && r.Question[0].Qclass != dns.ClassINET {
		errorAndMetricsFunc(s.Addr, w, r, dns.RcodeRefused)
		return
	}

	if m, err := edns.Version(r); err != nil { // Wrong EDNS version, return at once.
		w.WriteMsg(m)
		return
	}

	// Wrap the response writer in a ScrubWriter so we automatically make the reply fit in the client's buffer.
	w = request.NewScrubWriter(r, w)

	q := strings.ToLower(r.Question[0].Name)
	var (
		off       int
		end       bool
		dshandler *Config
	)

	for {
		if z, ok := s.zones[q[off:]]; ok {
			for _, h := range z {
				if h.pluginChain == nil { // zone defined, but has not got any plugins
					errorAndMetricsFunc(s.Addr, w, r, dns.RcodeRefused)
					return
				}

				if h.metaCollector != nil {
					// Collect metadata now, so it can be used before we send a request down the plugin chain.
					ctx = h.metaCollector.Collect(ctx, request.Request{Req: r, W: w})
				}

				// If all filter funcs pass, use this config.
				if passAllFilterFuncs(ctx, h.FilterFuncs, &request.Request{Req: r, W: w}) {
					if h.ViewName != "" {
						// if there was a view defined for this Config, set the view name in the context
						ctx = context.WithValue(ctx, ViewKey{}, h.ViewName)
					}
					if r.Question[0].Qtype != dns.TypeDS {
						rcode, _ := h.pluginChain.ServeDNS(ctx, w, r)
						if !plugin.ClientWrite(rcode) {
							errorFunc(s.Addr, w, r, rcode)
						}
						return
					}
					// The type is DS, keep the handler, but keep on searching as maybe we are serving
					// the parent as well and the DS should be routed to it - this will probably *misroute* DS
					// queries to a possibly grand parent, but there is no way for us to know at this point
					// if there is an actual delegation from grandparent -> parent -> zone.
					// In all fairness: direct DS queries should not be needed.
					dshandler = h
				}
			}
		}
		off, end = dns.NextLabel(q, off)
		if end {
			break
		}
	}

	if r.Question[0].Qtype == dns.TypeDS && dshandler != nil && dshandler.pluginChain != nil {
		// DS request, and we found a zone, use the handler for the query.
		rcode, _ := dshandler.pluginChain.ServeDNS(ctx, w, r)
		if !plugin.ClientWrite(rcode) {
			errorFunc(s.Addr, w, r, rcode)
		}
		return
	}

	// Wildcard match, if we have found nothing try the root zone as a last resort.
	if z, ok := s.zones["."]; ok {
		for _, h := range z {
			if h.pluginChain == nil {
				continue
			}

			if h.metaCollector != nil {
				// Collect metadata now, so it can be used before we send a request down the plugin chain.
				ctx = h.metaCollector.Collect(ctx, request.Request{Req: r, W: w})
			}

			// If all filter funcs pass, use this config.
			if passAllFilterFuncs(ctx, h.FilterFuncs, &request.Request{Req: r, W: w}) {
				if h.ViewName != "" {
					// if there was a view defined for this Config, set the view name in the context
					ctx = context.WithValue(ctx, ViewKey{}, h.ViewName)
				}
				rcode, _ := h.pluginChain.ServeDNS(ctx, w, r)
				if !plugin.ClientWrite(rcode) {
					errorFunc(s.Addr, w, r, rcode)
				}
				return
			}
		}
	}

	// Still here? Error out with REFUSED.
	errorAndMetricsFunc(s.Addr, w, r, dns.RcodeRefused)
}

// passAllFilterFuncs returns true if all filter funcs evaluate to true for the given request
func passAllFilterFuncs(ctx context.Context, filterFuncs []FilterFunc, req *request.Request) bool {
	for _, ff := range filterFuncs {
		if !ff(ctx, req) {
			return false
		}
	}
	return true
}

// OnStartupComplete lists the sites served by this server
// and any relevant information, assuming Quiet is false.
func (s *Server) OnStartupComplete() {
	if Quiet {
		return
	}

	out := startUpZones("", s.Addr, s.zones)
	if out != "" {
		fmt.Print(out)
	}
}

// Tracer returns the tracer in the server if defined.
func (s *Server) Tracer() ot.Tracer {
	if s.trace == nil {
		return nil
	}

	return s.trace.Tracer()
}

// errorFunc responds to an DNS request with an error.
func errorFunc(server string, w dns.ResponseWriter, r *dns.Msg, rc int) {
	state := request.Request{W: w, Req: r}

	answer := new(dns.Msg)
	answer.SetRcode(r, rc)
	state.SizeAndDo(answer)

	w.WriteMsg(answer)
}

func errorAndMetricsFunc(server string, w dns.ResponseWriter, r *dns.Msg, rc int) {
	state := request.Request{W: w, Req: r}

	answer := new(dns.Msg)
	answer.SetRcode(r, rc)
	state.SizeAndDo(answer)

	vars.Report(server, state, vars.Dropped, "", rcode.ToString(rc), "" /* plugin */, answer.Len(), time.Now())

	w.WriteMsg(answer)
}

const (
	tcp = 0
	udp = 1

	tcpMaxQueries = -1
)

type (
	// Key is the context key for the current server added to the context.
	Key struct{}

	// LoopKey is the context key to detect server wide loops.
	LoopKey struct{}

	// ViewKey is the context key for the current view, if defined
	ViewKey struct{}
)

// EnableChaos is a map with plugin names for which we should open CH class queries as we block these by default.
var EnableChaos = map[string]struct{}{
	"chaos":   {},
	"forward": {},
	"proxy":   {},
}

// Quiet mode will not show any informative output on initialization.
var Quiet bool
