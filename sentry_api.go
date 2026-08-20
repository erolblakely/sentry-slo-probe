package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
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

// traceDataset is the Discover dataset holding this org's transaction data.
//
// Sentry migrated transactions to the spans (EAP) dataset; dataset=transactions
// now returns zero rows for this org for any query at any stats period, so a
// trace probe pointed there reports received=0 with no send error and no query
// error. Both trace probes must read this constant — the errors probe keeps
// dataset=errors, which is still populated.
const traceDataset = "spans"

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

// findTraceBatch returns the set of trace ids from the batch that Sentry has
// ingested. Both trace probes go through here so the dataset is chosen once:
// the ingestion poll and the completeness sample cannot drift onto different
// datasets and disagree about which traces arrived.
func (s *sentryProbe) findTraceBatch(batchKey string, limit int) (map[string]bool, error) {
	return s.findBatch(traceDataset, batchKey, "trace", limit)
}

// findBatchSpanCounts returns traceID -> number of child spans stored, for the
// given traces, in ONE request. is_transaction:false excludes each trace's root
// transaction span, so the count is children only and comparable to the number
// of child spans the probe sent.
//
// A trace that arrived with no children at all is simply absent from the
// result: the caller must read a missing trace as zero children (incomplete),
// which it is, and is free to do so precisely because a failed census is
// reported as an error instead.
//
// limit caps per_page. One row comes back per trace (count() aggregates the
// children away), so len(traceIDs) is the natural bound.
func (s *sentryProbe) findBatchSpanCounts(traceIDs []string, limit int) (map[string]int, error) {
	// No traces sampled is not a failure, and must not become a request with an
	// empty trace:[] filter — that would match every trace in the org.
	if len(traceIDs) == 0 {
		return map[string]int{}, nil
	}

	q := url.Values{}
	q.Set("dataset", traceDataset)
	q.Set("statsPeriod", "1h")
	q.Set("query", fmt.Sprintf("trace:[%s] is_transaction:false", strings.Join(traceIDs, ",")))
	q.Set("field", "trace")
	q.Add("field", "count()")
	q.Set("per_page", strconv.Itoa(limit))

	endpoint := fmt.Sprintf("%s/api/0/organizations/%s/events/?%s",
		s.baseURL, url.PathEscape(s.org), q.Encode())

	var result struct {
		Data []map[string]any `json:"data"`
	}
	if err := s.get(endpoint, &result); err != nil {
		// Never a partial map alongside an error: an empty or half-filled map
		// reads as "these traces lost their spans" and publishes a low
		// completeness that says nothing about Sentry's span storage.
		return nil, err
	}

	counts := make(map[string]int, len(result.Data))
	for _, row := range result.Data {
		trace, _ := row["trace"].(string)
		if trace == "" {
			continue
		}
		raw, ok := row["count()"]
		if !ok {
			return nil, fmt.Errorf("span count for trace %s: no count() in row", trace)
		}
		// encoding/json decodes every JSON number into float64. Counts are
		// small integers, exactly representable, so the conversion is lossless
		// — but assert the type rather than assume it: a silent skip here would
		// leave the trace absent and score it as span loss.
		n, ok := raw.(float64)
		if !ok {
			return nil, fmt.Errorf("span count for trace %s: count() is %T, want number", trace, raw)
		}
		counts[trace] = int(n)
	}
	return counts, nil
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
