package tailscale

import (
	"tailscale.com/client/tailscale"
	"tailscale.com/tsnet"
)

type TailscaleServer struct {
	Server *tsnet.Server
	Client *tailscale.LocalClient
}

// Name implements plugin.Handler.
func (b *TailscaleServer) Name() string { return "tailscale" }
