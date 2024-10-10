package tailscale

import (
	"net"

	"github.com/coredns/coredns/plugin/pkg/reuseport"
	"tailscale.com/client/tailscale"
	"tailscale.com/tsnet"
)

type TailscaleServer struct {
	Server *tsnet.Server
	Client *tailscale.LocalClient
}

func (ts *TailscaleServer) Listen(network string, addr string) (net.Listener, error) {
	if ts.Server != nil {
		return ts.Server.Listen(network, addr)
	} else {
		return reuseport.Listen(network, addr)
	}
}

func (ts *TailscaleServer) ListenPacket(network string, addr string) (net.PacketConn, error) {
	if ts.Server != nil {
		return ts.Server.ListenPacket(network, addr)
	} else {
		return reuseport.ListenPacket(network, addr)
	}
}

// Name implements plugin.Handler.
func (b *TailscaleServer) Name() string { return "tailscale" }
