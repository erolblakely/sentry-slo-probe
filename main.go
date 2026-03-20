package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

func main() {
	cfg, err := configFromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx := context.Background()

	tp, err := initTracer(ctx, cfg.ddAPIKey, cfg.ddSite)
	if err != nil {
		log.Fatalf("init tracer: %v", err)
	}
	defer tp.Shutdown(ctx)

	s := newSentryProbe(cfg.sentryDSN, cfg.sentryAuthToken, cfg.sentryOrg, cfg.sentryProject)
	dd := newDatadogClient(cfg.ddAPIKey, cfg.ddSite)

	baseTags := []string{
		fmt.Sprintf("sentry_org:%s", cfg.sentryOrg),
		fmt.Sprintf("sentry_project:%s", cfg.sentryProject),
	}

	log.Printf("Starting SLO probes (interval=%s)", cfg.interval)

	var wg sync.WaitGroup
	wg.Add(3)

	// Probe 1: trace ingestion latency
	go runProbeLoop("trace_ingestion", cfg.interval, &wg, func(ctx context.Context) {
		result, err := probe(ctx, s, cfg.pollTimeout, cfg.pollInterval)
		if err != nil {
			log.Printf("[trace_ingestion] ERROR: %v", err)
			dd.postMetric("sentry.ingestion.error", 1, append(baseTags, "probe:trace_ingestion"))
			return
		}
		log.Printf("[trace_ingestion] latency=%s", result.latency)
		dd.postMetric("sentry.ingestion.latency_ms", float64(result.latency.Milliseconds()), append(baseTags, "probe:trace_ingestion"))
	})

	// Probe 2: error ingestion latency
	go runProbeLoop("error_ingestion", cfg.interval, &wg, func(ctx context.Context) {
		result, err := probeError(ctx, s, cfg.pollTimeout, cfg.pollInterval)
		if err != nil {
			log.Printf("[error_ingestion] ERROR: %v", err)
			dd.postMetric("sentry.ingestion.error", 1, append(baseTags, "probe:error_ingestion"))
			return
		}
		log.Printf("[error_ingestion] latency=%s", result.latency)
		dd.postMetric("sentry.error_ingestion.latency_ms", float64(result.latency.Milliseconds()), append(baseTags, "probe:error_ingestion"))
	})

	// Probe 3: span completeness
	go runProbeLoop("span_completeness", cfg.interval, &wg, func(ctx context.Context) {
		result, err := probeSpans(ctx, s, cfg.pollTimeout, cfg.pollInterval)
		if err != nil {
			log.Printf("[span_completeness] ERROR: %v", err)
			dd.postMetric("sentry.ingestion.error", 1, append(baseTags, "probe:span_completeness"))
			return
		}
		log.Printf("[span_completeness] received=%d/%d (%.0f%%)", result.receivedSpans, result.sentSpans, result.pct)
		dd.postMetric("sentry.span_completeness.received_pct", result.pct, append(baseTags, "probe:span_completeness"))
	})

	wg.Wait()
}

func runProbeLoop(name string, interval time.Duration, wg *sync.WaitGroup, fn func(context.Context)) {
	defer wg.Done()
	log.Printf("[%s] starting (interval=%s)", name, interval)
	fn(context.Background())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		fn(context.Background())
	}
}

type config struct {
	sentryDSN       string
	sentryAuthToken string
	sentryOrg       string
	sentryProject   string
	ddAPIKey        string
	ddSite          string
	interval        time.Duration
	pollTimeout     time.Duration
	pollInterval    time.Duration
}

func configFromEnv() (config, error) {
	required := map[string]string{
		"SENTRY_DSN":        os.Getenv("SENTRY_DSN"),
		"SENTRY_AUTH_TOKEN": os.Getenv("SENTRY_AUTH_TOKEN"),
		"SENTRY_ORG":        os.Getenv("SENTRY_ORG"),
		"SENTRY_PROJECT":    os.Getenv("SENTRY_PROJECT"),
		"DD_API_KEY":        os.Getenv("DD_API_KEY"),
	}
	for k, v := range required {
		if v == "" {
			return config{}, fmt.Errorf("required env var %s is not set", k)
		}
	}

	ddSite := os.Getenv("DD_SITE")
	if ddSite == "" {
		ddSite = "datadoghq.com"
	}

	return config{
		sentryDSN:       required["SENTRY_DSN"],
		sentryAuthToken: required["SENTRY_AUTH_TOKEN"],
		sentryOrg:       required["SENTRY_ORG"],
		sentryProject:   required["SENTRY_PROJECT"],
		ddAPIKey:        required["DD_API_KEY"],
		ddSite:          ddSite,
		interval:        envDuration("PROBE_INTERVAL_SECONDS", 60),
		pollTimeout:     envDuration("POLL_TIMEOUT_SECONDS", 120),
		pollInterval:    envDuration("POLL_INTERVAL_SECONDS", 5),
	}, nil
}

func envDuration(key string, defaultSeconds int) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return time.Duration(n) * time.Second
		}
	}
	return time.Duration(defaultSeconds) * time.Second
}
