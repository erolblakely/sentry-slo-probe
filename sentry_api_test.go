package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFindBatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("query"); got != "probe_batch:B1" {
			t.Errorf("query = %q, want probe_batch:B1", got)
		}
		if got := r.URL.Query().Get("dataset"); got != "errors" {
			t.Errorf("dataset = %q, want errors", got)
		}
		// per_page carries the batch limit; if it were dropped the poll would
		// see only a page of arrived events and under-report the SLO.
		if got := r.URL.Query().Get("per_page"); got != "100" {
			t.Errorf("per_page = %q, want 100", got)
		}
		w.WriteHeader(200)
		w.Write([]byte(`{"data":[{"probe_seq":"0"},{"probe_seq":"3"},{"probe_seq":"3"}]}`))
	}))
	defer srv.Close()

	s := newSentryProbe("dsn", "tok", "org", "proj")
	s.baseURL = srv.URL
	found, err := s.findBatch("errors", "B1", "probe_seq", 100)
	if err != nil {
		t.Fatalf("findBatch: %v", err)
	}
	if len(found) != 2 || !found["0"] || !found["3"] {
		t.Errorf("found = %v, want {0,3}", found)
	}
}

func TestFindBatchEventIDs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("query"); got != "probe_batch:B1" {
			t.Errorf("query = %q, want probe_batch:B1", got)
		}
		if got := r.URL.Query().Get("dataset"); got != "transactions" {
			t.Errorf("dataset = %q, want transactions", got)
		}
		// Both fields must be requested: without "trace" the rows come back
		// keyed only by id and the returned map is silently empty.
		if got := r.URL.Query()["field"]; len(got) != 2 || got[0] != "trace" || got[1] != "id" {
			t.Errorf("field = %v, want [trace id]", got)
		}
		if got := r.URL.Query().Get("per_page"); got != "100" {
			t.Errorf("per_page = %q, want 100", got)
		}
		w.WriteHeader(200)
		w.Write([]byte(`{"data":[{"trace":"T1","id":"E1"},{"trace":"T2","id":"E2"}]}`))
	}))
	defer srv.Close()

	s := newSentryProbe("dsn", "tok", "org", "proj")
	s.baseURL = srv.URL
	m, err := s.findBatchEventIDs("B1", 100)
	if err != nil {
		t.Fatalf("findBatchEventIDs: %v", err)
	}
	if m["T1"] != "E1" || m["T2"] != "E2" {
		t.Errorf("map = %v, want T1->E1,T2->E2", m)
	}
}
