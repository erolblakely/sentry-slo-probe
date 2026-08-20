package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

var sentryHTTP = &http.Client{Timeout: 10 * time.Second}

// apiError is a non-2xx response from the Sentry API.
type apiError struct {
	StatusCode int
	Body       string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("sentry api %d: %s", e.StatusCode, e.Body)
}

// fetchEventSpanCount fetches a transaction event and returns the number of child spans.
func (s *sentryProbe) fetchEventSpanCount(eventID string) (int, error) {
	endpoint := fmt.Sprintf("%s/api/0/projects/%s/%s/events/%s/",
		s.baseURL, url.PathEscape(s.org), url.PathEscape(s.project), url.PathEscape(eventID))

	// A transaction event's child spans live under entries[type=="spans"].data,
	// not a top-level "spans" field.
	var event struct {
		Entries []struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		} `json:"entries"`
	}
	if err := s.get(endpoint, &event); err != nil {
		return 0, err
	}
	for _, e := range event.Entries {
		if e.Type == "spans" {
			var spans []json.RawMessage
			if err := json.Unmarshal(e.Data, &spans); err != nil {
				return 0, fmt.Errorf("decode spans entry: %w", err)
			}
			return len(spans), nil
		}
	}
	return 0, nil
}

// findBatch queries Discover for events tagged probe_batch:batchID and returns
// the set of distinct idField values seen. One request covers the whole batch,
// so polling cost stays constant no matter how many events a cycle sends.
func (s *sentryProbe) findBatch(dataset, batchID, idField string, limit int) (map[string]bool, error) {
	q := url.Values{}
	q.Set("dataset", dataset)
	q.Set("statsPeriod", "1h")
	q.Set("query", fmt.Sprintf("probe_batch:%s", batchID))
	q.Set("field", idField)
	q.Set("per_page", strconv.Itoa(limit))

	endpoint := fmt.Sprintf("%s/api/0/organizations/%s/events/?%s",
		s.baseURL, url.PathEscape(s.org), q.Encode())

	var result struct {
		Data []map[string]any `json:"data"`
	}
	if err := s.get(endpoint, &result); err != nil {
		return nil, err
	}
	// Callers treat the result as a set: only ids actually present are inserted,
	// always true. A false entry would read as "arrived" and skew the SLO.
	found := make(map[string]bool, len(result.Data))
	for _, row := range result.Data {
		if v, ok := row[idField]; ok && v != nil {
			found[fmt.Sprint(v)] = true
		}
	}
	return found, nil
}

// findBatchEventIDs returns traceID->eventID for transactions in the batch.
// The event ID is what the span-count check needs to fetch the stored event.
func (s *sentryProbe) findBatchEventIDs(batchID string, limit int) (map[string]string, error) {
	q := url.Values{}
	q.Set("dataset", "transactions")
	q.Set("statsPeriod", "1h")
	q.Set("query", fmt.Sprintf("probe_batch:%s", batchID))
	q.Set("field", "trace")
	q.Add("field", "id")
	q.Set("per_page", strconv.Itoa(limit))

	endpoint := fmt.Sprintf("%s/api/0/organizations/%s/events/?%s",
		s.baseURL, url.PathEscape(s.org), q.Encode())

	var result struct {
		Data []struct {
			Trace string `json:"trace"`
			ID    string `json:"id"`
		} `json:"data"`
	}
	if err := s.get(endpoint, &result); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(result.Data))
	for _, row := range result.Data {
		if row.Trace != "" && row.ID != "" {
			out[row.Trace] = row.ID
		}
	}
	return out, nil
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
		return &apiError{StatusCode: resp.StatusCode, Body: string(body)}
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
