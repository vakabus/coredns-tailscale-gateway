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
				proxy.handleConnection(conn)
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

func (proxy *TcpProxy) handleConnection(downstream net.Conn) {
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
	closerChannel := make(chan struct{})
	defer close(closerChannel)

	var iowg sync.WaitGroup
	iowg.Add(2)
	go copy(&iowg, closerChannel, upstream, downstream)
	go copy(&iowg, closerChannel, downstream, upstream)

	// Wait until one of:
	//  - one thread ends
	//  - a stop is requested from the outside
	select {
	case <-closerChannel:
	case <-proxy.quit:
	}

	// and then close the connection in 90 seconds also for the other side, if they don't close it themselves
	now := time.Now()
	downstream.SetDeadline(now.Add(90 * time.Second))
	upstream.SetDeadline(now.Add(90 * time.Second))

	// wait for both copy threads to finish
	iowg.Wait()
	tcpLog.Debugf("connection from %s closed", downstream.RemoteAddr().String())
}

func copy(wg *sync.WaitGroup, closer chan struct{}, dst io.Writer, src io.Reader) {
	_, _ = io.Copy(dst, src)

	// notify the parent that we should start closing
	// Note that in TCP, the connection can be closed on one side and the data stream
	// still continue from the other side.
	select {
	case closer <- struct{}{}:
		// this is blocking, so it only works if someone is actively receiving
	default:
		// if there is nobody receiving, we can continue
	}

	wg.Done()
}
