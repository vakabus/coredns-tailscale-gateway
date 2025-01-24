package tsproxy

import (
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"tailscale.com/tsnet"
)

type UdpProxy struct {
	ts      *tsnet.Server
	srcPort int
	dst     string
	quit    chan struct{}

	downstream downstreamProxy
	upstream   map[string]upstreamProxy
}

func NewUdpProxy(ts *tsnet.Server, srcPort int, dstAddr string, dstPort int) *UdpProxy {
	var proxy UdpProxy

	proxy.ts = ts
	proxy.srcPort = srcPort
	proxy.dst = fmt.Sprintf("%s:%d", dstAddr, dstPort)
	proxy.quit = make(chan struct{})
	go proxy.serve()
	return &proxy
}

func (proxy *UdpProxy) serve() {
	// open downstream listener
	listener, err := net.ListenUDP("udp", &net.UDPAddr{Port: proxy.srcPort})
	if err != nil {
		panic(err)
	}

	// prepare upstream
	proxy.upstream = make(map[string]upstreamProxy)
	defer func() {
		for key, ch := range proxy.upstream {
			ch.close()
			delete(proxy.upstream, key)
		}
	}()

	// start downstream
	proxy.downstream.in = listener
	proxy.downstream.toDownstream = make(chan msg)
	proxy.downstream.toUpstream = make(chan msg)
	proxy.downstream.start()
	defer proxy.downstream.close()

	// prepare upstream gc
	cleanupUpstream := func(now time.Time) {
		deadline := now.Add(-90 * time.Second)
		for key, ch := range proxy.upstream {
			if ch.lastUsed.Before(deadline) {
				ch.close()
				delete(proxy.upstream, key)
			}
		}
	}
	gcTicker := time.NewTicker(90 * time.Second)

	// start main loop
	for {
		select {
		case <-proxy.quit:
			return
		case now := <-gcTicker.C:
			cleanupUpstream(now)
		case msg, ok := <-proxy.downstream.toUpstream:
			if !ok {
				return
			}

			proxy.send(msg)
		}
	}
}

func (proxy *UdpProxy) send(m msg) {
	up, ok := proxy.upstream[m.addr.String()]

	if !ok {
		var conn *net.UDPConn
		var err error
		if proxy.ts != nil {
			udpLog.Error("using the in-process tailscale tunnel is currently unsupported with UDP, falling back to normal system connection")
			// The following technicly works, but it doesn't give us the proper message-based UDP socket, but rather
			// a stream based connection:
			// conn, err = proxy.ts.Dial(context.Background(), "udp", proxy.dst)
		}

		addr, err := net.ResolveUDPAddr("udp", proxy.dst)
		if err != nil {
			udpLog.Errorf("failed to resolve target UDP address '%v' of the proxy: %v", proxy.dst, err)
			return
		}

		conn, err = net.DialUDP("udp", nil, addr)
		if err != nil {
			udpLog.Errorf("udp dial error: %v", err)
			return
		}

		newUpstream := upstreamProxy{
			toDowstream:       proxy.downstream.toDownstream,
			toUpstream:        make(chan msg),
			conn:              conn,
			downstreamAddress: m.addr,
		}
		newUpstream.start()
		proxy.upstream[m.addr.String()] = newUpstream

		up = newUpstream
	}

	up.toUpstream <- m
}

type msg struct {
	data []byte
	addr *net.UDPAddr
}

type downstreamProxy struct {
	in           *net.UDPConn
	toUpstream   chan msg
	toDownstream chan msg
	quit         chan struct{}
}

func (proxy *downstreamProxy) writer() {
	for {
		select {
		case <-proxy.quit:
			proxy.in.SetDeadline(time.Now())
			return
		case pkt := <-proxy.toDownstream:
			n, _, err := proxy.in.WriteMsgUDP(pkt.data, nil, pkt.addr)
			if n != len(pkt.data) {
				udpLog.Errorf("wrote only %d out of %d bytes to downstream", n, len(pkt.data))
			}
			if err != nil {
				udpLog.Errorf("downstream send error: %v", err)
			}
		}
	}
}

func (proxy *downstreamProxy) reader() {
	defer proxy.in.Close()

	for {
		buffer := make([]byte, 16*1024)
		n, _, _, addr, err := proxy.in.ReadMsgUDP(buffer, nil)
		if n > 0 {
			proxy.toUpstream <- msg{data: buffer[:n], addr: addr}
		}
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				return
			}
			udpLog.Error("downstream UDP reading failed", err)
		}
	}
}

func (proxy *downstreamProxy) close() {
	close(proxy.quit)
}

func (proxy *downstreamProxy) start() {
	proxy.quit = make(chan struct{})

	go proxy.reader()
	go proxy.writer()
}

type upstreamProxy struct {
	toDowstream       chan msg
	toUpstream        chan msg
	quit              chan struct{}
	conn              *net.UDPConn
	downstreamAddress *net.UDPAddr
	lastUsed          time.Time
}

func (proxy *upstreamProxy) reader() {
	defer proxy.conn.Close()

	for {
		proxy.lastUsed = time.Now()
		buffer := make([]byte, 16*1024)
		n, _, _, _, err := proxy.conn.ReadMsgUDP(buffer, nil)
		if n > 0 {
			proxy.toDowstream <- msg{data: buffer[:n], addr: proxy.downstreamAddress}
		}
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				return
			}
			udpLog.Errorf("upstream UDP reading failed: %v", err)
		}
	}
}

func (proxy *upstreamProxy) writer() {
	for {
		proxy.lastUsed = time.Now()

		select {
		case <-proxy.quit:
			proxy.conn.SetDeadline(time.Now())
			return
		case pkt := <-proxy.toUpstream:
			n, _, err := proxy.conn.WriteMsgUDP(pkt.data, nil, nil)
			if n != len(pkt.data) {
				udpLog.Errorf("wrote only %d out of %d bytes to upstream", n, len(pkt.data))
			}
			if err != nil {
				udpLog.Errorf("upstream send error: %v", err)
			}
		}
	}
}

func (proxy *upstreamProxy) start() {
	proxy.lastUsed = time.Now()
	proxy.quit = make(chan struct{})

	go proxy.reader()
	go proxy.writer()
}

func (proxy *upstreamProxy) close() {
	close(proxy.quit)
}

func (proxy *UdpProxy) Close() {
	close(proxy.quit)
}
