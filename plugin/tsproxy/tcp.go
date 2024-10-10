package tsproxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/coredns/coredns/plugin/pkg/reuseport"
	"tailscale.com/tsnet"
)

type TcpProxy struct {
	listener net.Listener
	wg       sync.WaitGroup
	quit     chan interface{}
	ts       *tsnet.Server
	dst      string
}

func NewTcpProxy(ts *tsnet.Server, srcPort int, dstAddr string, dstPort int) *TcpProxy {
	var proxy TcpProxy

	listener, err := reuseport.Listen("tcp", fmt.Sprintf(":%d", srcPort))
	if err != nil {
		panic(err)
	}

	proxy.listener = listener
	proxy.wg.Add(1)
	proxy.ts = ts
	proxy.dst = fmt.Sprintf("%s:%d", dstAddr, dstPort)
	proxy.quit = make(chan interface{})
	go proxy.serve()
	return &proxy
}

func (proxy *TcpProxy) serve() {
	defer proxy.wg.Done()

	for {
		conn, err := proxy.listener.Accept()
		if err != nil {
			// an error happens
			select {
			case <-proxy.quit:
				return
			default:
				tcpLog.Errorf("accept error: %v", err)
			}
		} else {
			// normal connection accepted, spawn a handler goroutine
			proxy.wg.Add(1)
			go func() {
				proxy.handleConection(conn)
				proxy.wg.Done()
			}()
			tcpLog.Debugf("incomming connection from '%s' will be proxied to '%s'", conn.LocalAddr().String(), conn.RemoteAddr().String())
		}
	}
}

func (proxy *TcpProxy) Close() {
	close(proxy.quit)
	proxy.listener.Close()
	proxy.wg.Wait()
}

func (proxy *TcpProxy) handleConection(downstream net.Conn) {
	defer downstream.Close()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(4*time.Second))
	defer cancel()

	var upstream net.Conn
	var err error
	if proxy.ts == nil {
		upstream, err = net.Dial("tcp", proxy.dst)
	} else {
		upstream, err = proxy.ts.Dial(ctx, "tcp", proxy.dst)
	}
	if err != nil {
		tcpLog.Errorf("error dialing remote addr: %v", err)
		return
	}
	defer upstream.Close()

	// Start threads for copying both ways
	closerChannel := make(chan struct{}, 2)
	var iowg sync.WaitGroup
	iowg.Add(2)
	go copy(&iowg, closerChannel, true, upstream, downstream)
	go copy(&iowg, closerChannel, false, downstream, upstream)

	// Wait until one of:
	//  - both threads end
	//  - a stop is requested from the outside
	select {
	case <-closerChannel:
		// both connections ended, we just cleanly exit
	case <-proxy.quit:
		// a stop was requested from outside
		// give the connection 10 more seconds to stop on it's own, then terminate

		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
			tcpLog.Infof("terminating proxy connection from %s", downstream.RemoteAddr())
		case <-closerChannel:
		}
	}

	tcpLog.Debugf("connection from %s closed", downstream.RemoteAddr().String())
}

func copy(wg *sync.WaitGroup, closer chan struct{}, shouldClose bool, dst io.Writer, src io.Reader) {
	_, _ = io.Copy(dst, src)

	wg.Done()
	wg.Wait()
	if shouldClose {
		// a channel can be closed only once, that's why we have to check
		close(closer)
	}
}
