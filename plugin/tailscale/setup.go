package tailscale

import (
	"context"
	"fmt"
	"time"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/plugin"
	clog "github.com/coredns/coredns/plugin/pkg/log"
	"tailscale.com/tsnet"
)

var log = clog.NewWithPlugin("tailscale")

var Tailscale *TailscaleServer = nil

func init() { plugin.Register("tailscale", setup) }

func setup(c *caddy.Controller) error {
	var hostname string
	if c.Next() {
		if !c.Args(&hostname) {
			return c.ArgErr()
		}
	} else {
		return fmt.Errorf("missing hostname")
	}

	err := start(hostname)
	if err != nil {
		return err
	}

	c.OnStartup(func() error {
		return nil
	})

	c.OnShutdown(func() error {
		err := Tailscale.Server.Close()
		if err != nil {
			return err
		}

		return nil
	})

	return nil
}

func start(hostname string) error {
	Tailscale = &TailscaleServer{}
	Tailscale.Server = &tsnet.Server{
		Hostname:     hostname,
		Logf:         log.Debugf,
		RunWebClient: true,
		//Ephemeral:    true,
	}
	err := Tailscale.Server.Start()
	if err != nil {
		return err
	}

	Tailscale.Client, err = Tailscale.Server.LocalClient()
	if err != nil {
		return err
	}

	for {
		status, err := Tailscale.Client.Status(context.Background())
		if err != nil {
			return err
		}
		if status.BackendState == "Running" {
			break
		} else {
			log.Info("waiting for tailscale")
			time.Sleep(1 * time.Second)
		}
	}

	return nil
}
