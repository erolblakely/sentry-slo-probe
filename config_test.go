package main

import (
	"testing"
	"time"
)

func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

func TestConfigFromEnvDefaults(t *testing.T) {
	setEnv(t, map[string]string{
		"SENTRY_DSN": "d", "SENTRY_AUTH_TOKEN": "t",
		"SENTRY_ORG": "o", "SENTRY_PROJECT": "p", "DD_API_KEY": "k",
	})
	cfg, err := configFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.batchSize != 100 {
		t.Errorf("batchSize = %d, want 100", cfg.batchSize)
	}
	if cfg.interval != 120*time.Second {
		t.Errorf("interval = %s, want 2m", cfg.interval)
	}
	if cfg.sendWindow != 100*time.Second {
		t.Errorf("sendWindow = %s, want 100s", cfg.sendWindow)
	}
	if cfg.spanSample != 20 {
		t.Errorf("spanSample = %d, want 20", cfg.spanSample)
	}
}

func TestConfigFromEnvOverrides(t *testing.T) {
	setEnv(t, map[string]string{
		"SENTRY_DSN": "d", "SENTRY_AUTH_TOKEN": "t",
		"SENTRY_ORG": "o", "SENTRY_PROJECT": "p", "DD_API_KEY": "k",
		"PROBE_BATCH_SIZE": "1", "PROBE_INTERVAL_SECONDS": "30",
		"PROBE_SEND_WINDOW_SECONDS": "10", "SPAN_COMPLETENESS_SAMPLE": "5",
	})
	cfg, err := configFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.batchSize != 1 || cfg.interval != 30*time.Second ||
		cfg.sendWindow != 10*time.Second || cfg.spanSample != 5 {
		t.Errorf("overrides not applied: %+v", cfg)
	}
}
