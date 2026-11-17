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

var requiredEnv = map[string]string{
	"SENTRY_DSN": "d", "SENTRY_AUTH_TOKEN": "t",
	"SENTRY_ORG": "o", "SENTRY_PROJECT": "p", "DD_API_KEY": "k",
}

// streamEnv returns the required variables with every TRACE_STREAM_* knob
// explicitly empty (configFromEnv treats empty as unset), so a developer who
// has sourced .env with the stream tuned or switched off still gets the
// documented defaults.
func streamEnv(t *testing.T) {
	t.Helper()
	setEnv(t, requiredEnv)
	for _, k := range []string{"TRACE_STREAM_ENABLED", "TRACE_STREAM_OTLP_ENABLED", "TRACE_STREAM_POLL_SECONDS", "TRACE_STREAM_TIMEOUT_SECONDS"} {
		t.Setenv(k, "")
	}
}

// TestConfigFromEnvTraceStream: the stream is on unless switched off, polls
// every 2s, gives a trace 300s to arrive, and every knob fails loudly on input
// it cannot use.
func TestConfigFromEnvTraceStream(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		streamEnv(t)
		cfg, err := configFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !cfg.streamEnabled || !cfg.streamOTLPEnabled {
			t.Errorf("streamEnabled = %v, streamOTLPEnabled = %v by default, want both true", cfg.streamEnabled, cfg.streamOTLPEnabled)
		}
		if cfg.streamPollInterval != 2*time.Second {
			t.Errorf("streamPollInterval = %s, want 2s", cfg.streamPollInterval)
		}
		// Well above the 120s trace latency alert threshold: a timeout at the
		// threshold would cap measurable latency right where it starts to matter
		// and report a slow Sentry as a total loss.
		if cfg.streamTimeout != 300*time.Second {
			t.Errorf("streamTimeout = %s, want 5m", cfg.streamTimeout)
		}
	})

	t.Run("timeout of exactly 30 minutes is accepted", func(t *testing.T) {
		streamEnv(t)
		t.Setenv("TRACE_STREAM_TIMEOUT_SECONDS", "1800")
		cfg, err := configFromEnv()
		if err != nil || cfg.streamTimeout != 30*time.Minute {
			t.Errorf("streamTimeout = %s, err = %v, want 30m and no error", cfg.streamTimeout, err)
		}
	})

	t.Run("overrides", func(t *testing.T) {
		streamEnv(t)
		t.Setenv("TRACE_STREAM_ENABLED", "false")
		t.Setenv("TRACE_STREAM_OTLP_ENABLED", "0")
		t.Setenv("TRACE_STREAM_POLL_SECONDS", "5")
		t.Setenv("TRACE_STREAM_TIMEOUT_SECONDS", "200")
		t.Setenv("POLL_TIMEOUT_SECONDS", "45") // the batch timeout must not leak into the stream
		cfg, err := configFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.streamOTLPEnabled {
			t.Error("streamOTLPEnabled = true with TRACE_STREAM_OTLP_ENABLED=0")
		}
		if cfg.streamEnabled || cfg.streamPollInterval != 5*time.Second || cfg.streamTimeout != 200*time.Second {
			t.Errorf("streamEnabled = %v, streamPollInterval = %s, streamTimeout = %s, want false, 5s, 3m20s",
				cfg.streamEnabled, cfg.streamPollInterval, cfg.streamTimeout)
		}
	})

	// A typo must not silently leave the stream running, sending 86,400 traces
	// a day the operator thought they had switched off. And a seconds knob that
	// silently falls back to its default hides the mistake: "2s" or "abc" would
	// run at a value nobody chose. Zero is rejected because time.NewTicker
	// panics on a non-positive interval (taking the batch probes down too), and
	// a zero timeout would declare every trace lost on its first look.
	for _, c := range []struct{ key, value string }{
		{"TRACE_STREAM_ENABLED", "flase"},
		{"TRACE_STREAM_OTLP_ENABLED", "yes please"},
		{"TRACE_STREAM_POLL_SECONDS", "0"},
		{"TRACE_STREAM_POLL_SECONDS", "abc"},
		{"TRACE_STREAM_POLL_SECONDS", "2s"},
		{"TRACE_STREAM_TIMEOUT_SECONDS", "0"},
		{"TRACE_STREAM_TIMEOUT_SECONDS", "-5"},
		{"TRACE_STREAM_TIMEOUT_SECONDS", "five minutes"},
		// findTraces searches the last hour (statsPeriod=1h). Past 30 minutes,
		// timeout plus grace and polling slack starts to approach it, and a
		// slow trace could fall out of the search window and read as lost.
		{"TRACE_STREAM_TIMEOUT_SECONDS", "1801"},
	} {
		t.Run(c.key+"="+c.value+" is rejected", func(t *testing.T) {
			streamEnv(t)
			t.Setenv(c.key, c.value)
			_, err := configFromEnv()
			if err == nil || !strings.Contains(err.Error(), c.key) {
				t.Errorf("err = %v, want an error naming %s", err, c.key)
			}
		})
	}
}
