package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// traceExists queries the Sentry Events API to check whether a given trace ID
// has been ingested. Returns true once the trace appears.
func (s *sentryProbe) traceExists(traceID string) (bool, error) {
	// Sentry's performance dataset lets us search by trace ID.
	query := url.Values{}
	query.Set("dataset", "transactions")
	query.Set("query", fmt.Sprintf("trace:%s", traceID))
	query.Set("field", "id")
	query.Set("field", "trace")
	query.Set("per_page", "1")

	endpoint := fmt.Sprintf(
		"https://sentry.io/api/0/organizations/%s/events/?%s",
		url.PathEscape(s.org),
		query.Encode(),
	)

	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+s.authToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("sentry api request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("sentry api returned %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("decode sentry response: %w", err)
	}

	return len(result.Data) > 0, nil
}
