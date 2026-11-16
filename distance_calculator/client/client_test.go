package client

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Tanmoy095/FleetToll-Engine/types"
)

func TestAggregateInvoiceSendsJSONToHTTPService(t *testing.T) {
	want := types.Distance{TenantID: types.LegacyTenantID, EventID: "obudata:0:1", OBUID: 42, Value: 3.25, Unix: 1700000000}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
			http.Error(w, "JSON content type required", http.StatusUnsupportedMediaType)
			return
		}
		var actual types.Distance
		if err := json.NewDecoder(r.Body).Decode(&actual); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if actual != want {
			t.Errorf("request = %+v, want %+v", actual, want)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	if err := NewClient(server.URL).AggregateInvoice(want); err != nil {
		t.Fatalf("AggregateInvoice() error = %v", err)
	}
}

type closeTrackingBody struct {
	io.Reader
	closed bool
}

func (b *closeTrackingBody) Close() error {
	b.closed = true
	return nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAggregateInvoiceClosesResponseBody(t *testing.T) {
	body := &closeTrackingBody{Reader: strings.NewReader(`{"status":"ok"}`)}
	c := NewClient("http://aggregator.test/aggregate")
	c.HTTPClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: body, Header: make(http.Header)}, nil
	})}
	if err := c.AggregateInvoice(types.Distance{TenantID: types.LegacyTenantID, EventID: "obudata:0:2", OBUID: 3, Value: 1, Unix: 1}); err != nil {
		t.Fatal(err)
	}
	if !body.closed {
		t.Error("response body was not closed")
	}
}

func TestAggregateInvoiceUsesHTTPClientTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	c := NewClient(server.URL)
	c.HTTPClient = &http.Client{Timeout: 10 * time.Millisecond}
	if err := c.AggregateInvoice(types.Distance{TenantID: types.LegacyTenantID, EventID: "obudata:0:3", OBUID: 3, Value: 1, Unix: 1}); err == nil {
		t.Fatal("expected request timeout")
	}
}
