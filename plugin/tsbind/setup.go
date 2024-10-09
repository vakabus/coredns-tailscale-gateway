package bind

import (
	"context"
	"fmt"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/tailscale"
)

func init() { plugin.Register("tsbind", setup) }

func setup(c *caddy.Controller) error {
	config := dnsserver.GetConfig(c)

	if tailscale.Tailscale == nil {
		return fmt.Errorf("tsbind: tailscale not initialized")
	}

	// collect all address from tailscale
	all := []string{}
	status, err := tailscale.Tailscale.Client.StatusWithoutPeers(context.Background())
	if err != nil {
		return fmt.Errorf("tailscale status: %w", err)
	}

	for _, ip := range status.TailscaleIPs {
		all = append(all, ip.String())
	}

	config.ListenHosts = all
	return nil
}
