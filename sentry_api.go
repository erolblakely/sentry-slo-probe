package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

var sentryHTTP = &http.Client{Timeout: 10 * time.Second}

// traceExists checks whether a transaction with the given trace ID has been ingested.
func (s *sentryProbe) traceExists(traceID string) (bool, error) {
	_, eventID, err := s.findTrace(traceID)
	return eventID != "", err
}

// traceExistsWithEventID returns true and the Sentry event ID once the trace is ingested.
func (s *sentryProbe) traceExistsWithEventID(traceID string) (bool, string, error) {
	return s.findTrace(traceID)
}

func (s *sentryProbe) findTrace(traceID string) (bool, string, error) {
	q := url.Values{}
	q.Set("dataset", "transactions")
	q.Set("query", fmt.Sprintf("trace:%s", traceID))
	q.Set("field", "id")
	q.Set("field", "trace")
	q.Set("per_page", "1")

	endpoint := fmt.Sprintf("https://sentry.io/api/0/organizations/%s/events/?%s",
		url.PathEscape(s.org), q.Encode())

	var result struct {
		Data []struct {
			ID    string `json:"id"`
			Trace string `json:"trace"`
		} `json:"data"`
	}
	if err := s.get(endpoint, &result); err != nil {
		return false, "", err
	}
	if len(result.Data) == 0 {
		return false, "", nil
	}
	return true, result.Data[0].ID, nil
}

// errorExists polls the Sentry Issues API for an event with the given probe ID tag.
func (s *sentryProbe) errorExists(probeID string) (bool, error) {
	q := url.Values{}
	q.Set("query", fmt.Sprintf("probe_id:%s", probeID))
	q.Set("limit", "1")

	endpoint := fmt.Sprintf("https://sentry.io/api/0/projects/%s/%s/issues/?%s",
		url.PathEscape(s.org), url.PathEscape(s.project), q.Encode())

	var result []json.RawMessage
	if err := s.get(endpoint, &result); err != nil {
		return false, err
	}
	return len(result) > 0, nil
}

// fetchEventSpanCount fetches a transaction event and returns the number of child spans.
func (s *sentryProbe) fetchEventSpanCount(eventID string) (int, error) {
	endpoint := fmt.Sprintf("https://sentry.io/api/0/projects/%s/%s/events/%s/",
		url.PathEscape(s.org), url.PathEscape(s.project), url.PathEscape(eventID))

	var event struct {
		Spans []json.RawMessage `json:"spans"`
	}
	if err := s.get(endpoint, &event); err != nil {
		return 0, err
	}
	return len(event.Spans), nil
}

func (s *sentryProbe) get(endpoint string, out any) error {
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.authToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := sentryHTTP.Do(req)
	if err != nil {
		return fmt.Errorf("sentry api: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("sentry api %d: %s", resp.StatusCode, string(body))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
