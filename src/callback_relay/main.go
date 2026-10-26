// Command callback_relay receives docling-serve's task-completion callbacks
// and signals the Temporal workflow waiting on each task (see package
// relay). Stateless, so unlike the orchestration worker it can run any
// number of replicas.
//
// Configuration, all from the environment:
//
//	RELAY_CALLBACK_TOKEN      required; the worker's ORCH_CALLBACK_TOKEN
//	RELAY_LISTEN_ADDR         default ":8080"
//	RELAY_TEMPORAL_ADDRESS    default "localhost:7233"
//	RELAY_TEMPORAL_NAMESPACE  default "default"
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"
	tlog "go.temporal.io/sdk/log"

	"github.com/k-sparrow/docling-serve-ray/src/callback_relay/relay"
)

const shutdownGrace = 15 * time.Second

type config struct {
	listenAddr        string
	temporalAddress   string
	temporalNamespace string
	callbackToken     string
}

func main() {
	healthcheck := flag.Bool("healthcheck", false,
		"probe this relay's /healthz and exit 0 if it answers -- the container healthcheck, since the image has no shell or curl")
	flag.Parse()

	if *healthcheck {
		// Needs only the port, so it works without the rest of the config.
		os.Exit(probe(listenAddr(os.Getenv)))
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	if err := run(cfg, logger); err != nil {
		logger.Error("callback relay stopped", "error", err)
		os.Exit(1)
	}
}

// loadConfig reads the configuration through getenv (os.Getenv outside
// tests). Without a callback token the relay would reject every callback,
// so that's an error rather than a relay that silently does nothing.
func loadConfig(getenv func(string) string) (config, error) {
	cfg := config{
		listenAddr:        listenAddr(getenv),
		temporalAddress:   valueOr(getenv, "RELAY_TEMPORAL_ADDRESS", "localhost:7233"),
		temporalNamespace: valueOr(getenv, "RELAY_TEMPORAL_NAMESPACE", "default"),
		callbackToken:     getenv("RELAY_CALLBACK_TOKEN"),
	}
	if cfg.callbackToken == "" {
		return config{}, errors.New("RELAY_CALLBACK_TOKEN must be set (the worker's ORCH_CALLBACK_TOKEN)")
	}
	return cfg, nil
}

func listenAddr(getenv func(string) string) string {
	return valueOr(getenv, "RELAY_LISTEN_ADDR", ":8080")
}

func run(cfg config, logger *slog.Logger) error {
	temporal, err := client.Dial(client.Options{
		HostPort:  cfg.temporalAddress,
		Namespace: cfg.temporalNamespace,
		Logger:    tlog.NewStructuredLogger(logger),
	})
	if err != nil {
		return fmt.Errorf("connecting to Temporal at %s: %w", cfg.temporalAddress, err)
	}
	defer temporal.Close()

	srv := &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           relay.NewHandler(temporal, cfg.callbackToken, logger),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	served := make(chan error, 1)
	go func() { served <- srv.ListenAndServe() }()
	logger.Info("callback relay up",
		"listen", cfg.listenAddr, "temporal", cfg.temporalAddress, "namespace", cfg.temporalNamespace)

	select {
	case err := <-served:
		return err
	case <-ctx.Done():
	}
	// Finish callbacks already being signaled before exiting.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func probe(listenAddr string) int {
	_, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://" + net.JoinHostPort("127.0.0.1", port) + "/healthz")
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		fmt.Fprintln(os.Stderr, "healthcheck: status", resp.StatusCode)
		return 1
	}
	return 0
}

func valueOr(getenv func(string) string, key, fallback string) string {
	if v := getenv(key); v != "" {
		return v
	}
	return fallback
}
