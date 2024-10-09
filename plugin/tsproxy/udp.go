package tsproxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/coredns/coredns/plugin/pkg/reuseport"
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
	listener, err := reuseport.ListenPacket("udp", fmt.Sprintf(":%d", proxy.srcPort))
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
		conn, err := proxy.ts.Dial(context.Background(), "udp", proxy.dst)
		if err != nil {
			udpLog.Error("udp dial error", err)
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
	addr net.Addr
}

type downstreamProxy struct {
	in           net.PacketConn
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
			n, err := proxy.in.WriteTo(pkt.data, pkt.addr)
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
		n, addr, err := proxy.in.ReadFrom(buffer)
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
	conn              net.Conn
	downstreamAddress net.Addr
	lastUsed          time.Time
}

func (proxy *upstreamProxy) reader() {
	defer proxy.conn.Close()

	for {
		proxy.lastUsed = time.Now()
		buffer := make([]byte, 16*1024)
		n, err := proxy.conn.Read(buffer)
		if n > 0 {
			proxy.toDowstream <- msg{data: buffer[:n], addr: proxy.downstreamAddress}
		}
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				return
			}
			udpLog.Error("upstream UDP reading failed", err)
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
			n, err := proxy.conn.Write(pkt.data)
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
