package observability

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// SignalContext returns a context cancelled on SIGINT/SIGTERM.
func SignalContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}

// Probe GETs the local health endpoint at addr (a listen address like ":8080")
// and errors unless it returns 200. Backs the `healthcheck` subcommand so a
// distroless image (no shell/curl) can be health-checked by the runtime.
func Probe(addr, path string) error {
	if addr == "" {
		addr = ":8080"
	}
	url := "http://127.0.0.1" + addr + path
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check %s returned %d", url, resp.StatusCode)
	}
	return nil
}
