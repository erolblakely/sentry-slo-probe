package main

import (
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

	s := newSentryProbe(cfg.sentryDSN, cfg.sentryAuthToken, cfg.sentryOrg, cfg.sentryProject)
	dd := newDatadogClient(cfg.ddAPIKey, cfg.ddSite)

	ws := newAlertWebhookServer(cfg.alertWebhookPort)
	if err := ws.start(); err != nil {
		log.Fatalf("webhook server: %v", err)
	}

	baseTags := []string{
		fmt.Sprintf("sentry_org:%s", cfg.sentryOrg),
		fmt.Sprintf("sentry_project:%s", cfg.sentryProject),
	}

	log.Printf("Starting SLO probes (interval=%s)", cfg.interval)

	var wg sync.WaitGroup
	wg.Add(4)

	// Probe 1: trace ingestion latency
	go runProbeLoop("trace_ingestion", cfg.interval, &wg, func() {
		result, err := probe(s, cfg.pollTimeout, cfg.pollInterval)
		if err != nil {
			log.Printf("[trace_ingestion] ERROR: %v", err)
			dd.postMetric("sentry.ingestion.error", 1, append(baseTags, "probe:trace_ingestion"))
			return
		}
		ms := float64(result.latency.Milliseconds())
		log.Printf("[trace_ingestion] latency=%s", result.latency)
		dd.postMetric("sentry.ingestion.latency_ms", ms, append(baseTags, "probe:trace_ingestion"))
	})

	// Probe 2: error ingestion latency
	go runProbeLoop("error_ingestion", cfg.interval, &wg, func() {
		result, err := probeError(s, cfg.pollTimeout, cfg.pollInterval)
		if err != nil {
			log.Printf("[error_ingestion] ERROR: %v", err)
			dd.postMetric("sentry.ingestion.error", 1, append(baseTags, "probe:error_ingestion"))
			return
		}
		ms := float64(result.latency.Milliseconds())
		log.Printf("[error_ingestion] latency=%s", result.latency)
		dd.postMetric("sentry.error_ingestion.latency_ms", ms, append(baseTags, "probe:error_ingestion"))
	})

	// Probe 3: span completeness
	go runProbeLoop("span_completeness", cfg.interval, &wg, func() {
		result, err := probeSpans(s, cfg.pollTimeout, cfg.pollInterval)
		if err != nil {
			log.Printf("[span_completeness] ERROR: %v", err)
			dd.postMetric("sentry.ingestion.error", 1, append(baseTags, "probe:span_completeness"))
			return
		}
		log.Printf("[span_completeness] received=%d/%d (%.0f%%)", result.receivedSpans, result.sentSpans, result.pct)
		dd.postMetric("sentry.span_completeness.received_pct", result.pct, append(baseTags, "probe:span_completeness"))
	})

	// Probe 4: alert firing latency
	go runProbeLoop("alert_firing", cfg.interval, &wg, func() {
		result, err := probeAlert(s, ws, cfg.alertWebhookTimeout)
		if err != nil {
			log.Printf("[alert_firing] ERROR: %v", err)
			dd.postMetric("sentry.ingestion.error", 1, append(baseTags, "probe:alert_firing"))
			return
		}
		ms := float64(result.latency.Milliseconds())
		log.Printf("[alert_firing] latency=%s", result.latency)
		dd.postMetric("sentry.alert_firing.latency_ms", ms, append(baseTags, "probe:alert_firing"))
	})

	wg.Wait()
}

func runProbeLoop(name string, interval time.Duration, wg *sync.WaitGroup, fn func()) {
	defer wg.Done()
	log.Printf("[%s] starting (interval=%s)", name, interval)
	fn() // run immediately on start
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		fn()
	}
}

type config struct {
	sentryDSN           string
	sentryAuthToken     string
	sentryOrg           string
	sentryProject       string
	ddAPIKey            string
	ddSite              string
	interval            time.Duration
	pollTimeout         time.Duration
	pollInterval        time.Duration
	alertWebhookPort    int
	alertWebhookTimeout time.Duration
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
		sentryDSN:           required["SENTRY_DSN"],
		sentryAuthToken:     required["SENTRY_AUTH_TOKEN"],
		sentryOrg:           required["SENTRY_ORG"],
		sentryProject:       required["SENTRY_PROJECT"],
		ddAPIKey:            required["DD_API_KEY"],
		ddSite:              ddSite,
		interval:            envDuration("PROBE_INTERVAL_SECONDS", 60),
		pollTimeout:         envDuration("POLL_TIMEOUT_SECONDS", 120),
		pollInterval:        envDuration("POLL_INTERVAL_SECONDS", 5),
		alertWebhookPort:    envInt("ALERT_WEBHOOK_PORT", 8080),
		alertWebhookTimeout: envDuration("ALERT_WEBHOOK_TIMEOUT_SECONDS", 300),
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

func envInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return defaultVal
}
