package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

func main() {
	cfg, err := configFromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	sentry := newSentryProbe(cfg.sentryDSN, cfg.sentryAuthToken, cfg.sentryOrg, cfg.sentryProject)
	dd := newDatadogClient(cfg.ddAPIKey, cfg.ddSite)

	log.Printf("Starting Sentry ingestion probe (interval=%s, timeout=%s)", cfg.interval, cfg.pollTimeout)

	for {
		result, err := probe(sentry, cfg.pollTimeout, cfg.pollInterval)
		if err != nil {
			log.Printf("[probe] ERROR: %v", err)
			if postErr := dd.postMetric("sentry.ingestion.error", 1, nil); postErr != nil {
				log.Printf("[datadog] failed to post error metric: %v", postErr)
			}
		} else {
			ms := float64(result.latency.Milliseconds())
			tags := []string{
				"env:probe",
				fmt.Sprintf("sentry_project:%s", cfg.sentryProject),
			}
			log.Printf("[probe] ingestion latency: %s (trace_id=%s)", result.latency, result.traceID)

			if postErr := dd.postMetric("sentry.ingestion.latency_ms", ms, tags); postErr != nil {
				log.Printf("[datadog] failed to post metric: %v", postErr)
			} else {
				log.Printf("[datadog] posted sentry.ingestion.latency_ms=%.0fms", ms)
			}
		}

		time.Sleep(cfg.interval)
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

	interval := envDuration("PROBE_INTERVAL_SECONDS", 60)
	pollTimeout := envDuration("POLL_TIMEOUT_SECONDS", 120)
	pollInterval := envDuration("POLL_INTERVAL_SECONDS", 5)

	return config{
		sentryDSN:       required["SENTRY_DSN"],
		sentryAuthToken: required["SENTRY_AUTH_TOKEN"],
		sentryOrg:       required["SENTRY_ORG"],
		sentryProject:   required["SENTRY_PROJECT"],
		ddAPIKey:        required["DD_API_KEY"],
		ddSite:          ddSite,
		interval:        interval,
		pollTimeout:     pollTimeout,
		pollInterval:    pollInterval,
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
