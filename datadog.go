package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type datadogClient struct {
	apiKey string
	site   string
	http   *http.Client
}

func newDatadogClient(apiKey, site string) *datadogClient {
	return &datadogClient{
		apiKey: apiKey,
		site:   site,
		http:   &http.Client{Timeout: 10 * time.Second},
	}
}

// postMetric sends a single gauge metric to the Datadog API (v2 series).
func (d *datadogClient) postMetric(name string, value float64, tags []string) error {
	if tags == nil {
		tags = []string{}
	}

	payload := map[string]any{
		"series": []map[string]any{
			{
				"metric": name,
				"type":   3, // 3 = gauge
				"points": []map[string]any{
					{
						"timestamp": time.Now().Unix(),
						"value":     value,
					},
				},
				"tags": tags,
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("https://api.%s/api/v2/series", d.site)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("DD-API-KEY", d.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.http.Do(req)
	if err != nil {
		return fmt.Errorf("datadog api request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("datadog api returned %d", resp.StatusCode)
	}
	return nil
}
