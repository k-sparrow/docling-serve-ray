package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func env(vars map[string]string) func(string) string {
	return func(key string) string { return vars[key] }
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{"RELAY_CALLBACK_TOKEN": "tok"}))
	if err != nil {
		t.Fatal(err)
	}
	want := config{
		listenAddr:        ":8080",
		temporalAddress:   "localhost:7233",
		temporalNamespace: "default",
		callbackToken:     "tok",
	}
	if cfg != want {
		t.Errorf("got %+v, want %+v", cfg, want)
	}
}

func TestLoadConfigFromEnvironment(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{
		"RELAY_CALLBACK_TOKEN":     "tok",
		"RELAY_LISTEN_ADDR":        ":9090",
		"RELAY_TEMPORAL_ADDRESS":   "temporal:7233",
		"RELAY_TEMPORAL_NAMESPACE": "docling",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := config{
		listenAddr:        ":9090",
		temporalAddress:   "temporal:7233",
		temporalNamespace: "docling",
		callbackToken:     "tok",
	}
	if cfg != want {
		t.Errorf("got %+v, want %+v", cfg, want)
	}
}

func TestLoadConfigRequiresTheCallbackToken(t *testing.T) {
	if _, err := loadConfig(env(nil)); err == nil {
		t.Fatal("no error without RELAY_CALLBACK_TOKEN")
	}
}

// The container healthcheck: probe hits 127.0.0.1 on the listen address's
// port, whatever host that address names.
func TestProbe(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		want   int
	}{
		{"healthy", http.StatusNoContent, 0},
		{"unhealthy", http.StatusServiceUnavailable, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/healthz" {
					http.NotFound(w, r)
					return
				}
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()
			_, port, _ := net.SplitHostPort(srv.Listener.Addr().String())

			if got := probe("relay.invalid:" + port); got != tc.want {
				t.Errorf("probe = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestProbeFailsWhenNothingListens(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(l.Addr().String())
	_ = l.Close()

	if got := probe(":" + port); got != 1 {
		t.Errorf("probe = %d, want 1", got)
	}
}

func TestProbeRejectsAnAddressWithoutAPort(t *testing.T) {
	if got := probe("8080"); got != 1 {
		t.Errorf("probe = %d, want 1", got)
	}
}
