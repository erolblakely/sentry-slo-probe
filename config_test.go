package main

import (
	"strings"
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

// TestConfigFromEnvRejectsOversizedBatch pins the Sentry ceiling on
// PROBE_BATCH_SIZE.
//
// findBatch retrieves the whole batch in ONE request and reads only
// result.Data — there is no pagination and no Link-header handling — so per_page
// is the entire retrieval mechanism, and it is set to the batch size. Sentry
// documents per_page on this endpoint as "Default and maximum allowed is 100".
// Above that the request either 400s, leaving received at 0 for a total false
// breach, or is clamped to 100, capping success_rate at 100/batchSize. Neither
// is detectable from the metrics, and PROBE_BATCH_SIZE is advertised as a free
// tunable in the README, .env.example and docker-compose.yml — so it has to fail
// loudly at startup instead.
func TestConfigFromEnvRejectsOversizedBatch(t *testing.T) {
	base := map[string]string{
		"SENTRY_DSN": "d", "SENTRY_AUTH_TOKEN": "t",
		"SENTRY_ORG": "o", "SENTRY_PROJECT": "p", "DD_API_KEY": "k",
	}

	t.Run("above the ceiling is rejected", func(t *testing.T) {
		setEnv(t, base)
		t.Setenv("PROBE_BATCH_SIZE", "101")
		_, err := configFromEnv()
		if err == nil {
			t.Fatal("configFromEnv accepted PROBE_BATCH_SIZE=101; the batch cannot be retrieved in one page and the SLO silently under-reports")
		}
		// The operator has to be able to act on this without reading the source,
		// so the message must name the variable and the limit.
		if !strings.Contains(err.Error(), "PROBE_BATCH_SIZE") || !strings.Contains(err.Error(), "100") {
			t.Errorf("error %q does not name PROBE_BATCH_SIZE and the limit of 100", err)
		}
	})

	t.Run("exactly the ceiling is accepted", func(t *testing.T) {
		setEnv(t, base)
		t.Setenv("PROBE_BATCH_SIZE", "100")
		cfg, err := configFromEnv()
		if err != nil {
			t.Fatalf("configFromEnv rejected the documented maximum of 100: %v", err)
		}
		if cfg.batchSize != 100 {
			t.Errorf("batchSize = %d, want 100", cfg.batchSize)
		}
	})
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
