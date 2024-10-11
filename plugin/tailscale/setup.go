package tailscale

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/plugin"
	clog "github.com/coredns/coredns/plugin/pkg/log"
	"tailscale.com/client/tailscale"
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
		if Tailscale.Server != nil {
			err := Tailscale.Server.Close()
			if err != nil {
				return err
			}
		}

		return nil
	})

	return nil
}

func systemTailscaleRunning() bool {
	client := &tailscale.LocalClient{}
	_, err := client.Status(context.Background())
	return err == nil
}

func start(hostname string) error {
	Tailscale = &TailscaleServer{}

	if systemTailscaleRunning() {
		Tailscale.Server = nil
		Tailscale.Client = &tailscale.LocalClient{}
	} else {
		// Create a unique config directory for this instance based on the hostname
		globalConfigDir, err := os.UserConfigDir()
		if err != nil {
			return fmt.Errorf("failed to obtain user config dir: %w", err)
		}
		configDir := filepath.Join(globalConfigDir, "coredns-tailscale", hostname)
		err = os.MkdirAll(configDir, fs.FileMode(0700))
		if err != nil {
			return fmt.Errorf("failed to create config directory: %w", err)
		}

		// Start the local tailscale instance
		Tailscale = &TailscaleServer{}
		Tailscale.Server = &tsnet.Server{
			Dir:          configDir,
			Hostname:     hostname,
			UserLogf:     log.Infof,
			Logf:         log.Debugf,
			RunWebClient: true,
		}
		err = Tailscale.Server.Start()
		if err != nil {
			return err
		}

		Tailscale.Client, err = Tailscale.Server.LocalClient()
		if err != nil {
			return err
		}
	}

	// Wait for tailscale to properly initialize
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
