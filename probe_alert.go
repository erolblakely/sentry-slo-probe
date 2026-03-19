package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

type alertWebhookServer struct {
	fired chan time.Time
	port  int
}

func newAlertWebhookServer(port int) *alertWebhookServer {
	return &alertWebhookServer{
		fired: make(chan time.Time, 1),
		port:  port,
	}
}

func (a *alertWebhookServer) start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/webhook", a.handler())

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", a.port),
		Handler: mux,
	}

	go func() {
		log.Printf("[alert-webhook] listening on :%d — configure your Sentry alert rule to POST here", a.port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[alert-webhook] server error: %v", err)
		}
	}()
	return nil
}

func (a *alertWebhookServer) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		io.Copy(io.Discard, r.Body) // drain to avoid broken pipe
		arrivedAt := time.Now()
		select {
		case a.fired <- arrivedAt:
		default:
			// drop duplicate fires within one probe cycle
		}
		w.WriteHeader(http.StatusOK)
	}
}

// probeAlert sends a Sentry error event, then waits for the configured Sentry
// alert rule to fire a webhook. Returns the measured alert firing latency.
func probeAlert(s *sentryProbe, ws *alertWebhookServer, timeout time.Duration) (*probeResult, error) {
	// Drain any stale signal from a previous cycle.
	select {
	case <-ws.fired:
	default:
	}

	_, sentAt, err := s.sendError()
	if err != nil {
		return nil, fmt.Errorf("send error: %w", err)
	}

	select {
	case firedAt := <-ws.fired:
		return &probeResult{latency: firedAt.Sub(sentAt)}, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timed out waiting for Sentry alert webhook after %s — is the alert rule configured?", timeout)
	}
}
