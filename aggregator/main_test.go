package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Tanmoy095/FleetToll-Engine/internal/tenantauth"
	"github.com/Tanmoy095/FleetToll-Engine/types"
)

const (
	testTenantToken  = "tenant-test-token-0123456789abcdef"
	testDeviceToken  = "device-test-token-0123456789abcdef"
	testServiceToken = "service-test-token-0123456789abcdef"
)

func newTestHTTPHandler(t *testing.T, svc Aggregator) http.Handler {
	t.Helper()
	keys, err := tenantauth.New([]tenantauth.CredentialSpec{
		{Token: testTenantToken, TenantID: 1, Role: tenantauth.RoleTenant},
		{Token: "tenant-two-test-token-0123456789abcdef", TenantID: 2, Role: tenantauth.RoleTenant},
		{Token: testDeviceToken, TenantID: 1, Role: tenantauth.RoleDevice, OBUIDs: []int{42}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return newHTTPHandler(svc, keys, testServiceToken)
}

func setServiceAuth(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+testServiceToken)
}

func setTenantAuth(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+testTenantToken)
}

func TestAggregatorHTTPBoundary(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{
			name:       "aggregate accepts valid distance",
			method:     http.MethodPost,
			path:       "/aggregate",
			body:       `{"tenantID":1,"eventID":"obudata:0:1","value":2.5,"obuID":42,"unix":1700000000}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "aggregate rejects wrong method",
			method:     http.MethodGet,
			path:       "/aggregate",
			body:       `{"tenantID":1,"eventID":"obudata:0:1","value":2.5,"obuID":42,"unix":1700000000}`,
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "aggregate rejects malformed json",
			method:     http.MethodPost,
			path:       "/aggregate",
			body:       `{"value":`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "aggregate rejects unknown fields",
			method:     http.MethodPost,
			path:       "/aggregate",
			body:       `{"tenantID":1,"eventID":"obudata:0:1","value":2.5,"obuID":42,"unix":1700000000,"extra":99}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "aggregate rejects trailing json values",
			method:     http.MethodPost,
			path:       "/aggregate",
			body:       `{"tenantID":1,"eventID":"obudata:0:1","value":2.5,"obuID":42,"unix":1700000000} {}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "aggregate rejects unsupported content type",
			method:     http.MethodPost,
			path:       "/aggregate",
			body:       `{"tenantID":1,"eventID":"obudata:0:1","value":2.5,"obuID":42,"unix":1700000000}`,
			wantStatus: http.StatusUnsupportedMediaType,
		},
		{
			name:       "aggregate rejects missing content type",
			method:     http.MethodPost,
			path:       "/aggregate",
			body:       `{"tenantID":1,"eventID":"obudata:0:1","value":2.5,"obuID":42,"unix":1700000000}`,
			wantStatus: http.StatusUnsupportedMediaType,
		},
		{
			name:       "aggregate rejects invalid business data",
			method:     http.MethodPost,
			path:       "/aggregate",
			body:       `{"tenantID":1,"eventID":"obudata:0:1","value":-1,"obuID":42,"unix":1700000000}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "aggregate requires event identity",
			method:     http.MethodPost,
			path:       "/aggregate",
			body:       `{"tenantID":1,"value":2.5,"obuID":42,"unix":1700000000}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "aggregate limits request size",
			method:     http.MethodPost,
			path:       "/aggregate",
			body:       `{"tenantID":1,"eventID":"obudata:0:1","value":2.5,"obuID":42,"unix":1700000000,"extra":"` + strings.Repeat("x", 1<<20) + `"}`,
			wantStatus: http.StatusRequestEntityTooLarge,
		},
		{
			name:       "invoice rejects wrong method",
			method:     http.MethodPost,
			path:       "/invoice?obu=42",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "invoice rejects zero id",
			method:     http.MethodGet,
			path:       "/invoice?obu=0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invoice rejects repeated id parameter",
			method:     http.MethodGet,
			path:       "/invoice?obu=42&obu=43",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invoice returns not found for unknown id",
			method:     http.MethodGet,
			path:       "/invoice?obu=42",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := NewInvoiceAggregator(NewMemoryStore())
			server := httptest.NewServer(newTestHTTPHandler(t, svc))
			defer server.Close()

			req, err := http.NewRequest(tt.method, server.URL+tt.path, strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			if tt.name != "aggregate rejects unsupported content type" && tt.name != "aggregate rejects missing content type" && strings.Contains(tt.path, "/aggregate") {
				req.Header.Set("Content-Type", "application/json")
			}
			if strings.Contains(tt.path, "/aggregate") {
				setServiceAuth(req)
			} else {
				setTenantAuth(req)
			}
			resp, err := server.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
			if tt.wantStatus == http.StatusMethodNotAllowed && resp.Header.Get("Allow") == "" {
				t.Error("405 response must include Allow header")
			}
		})
	}
}

func TestAggregatorHTTPReturnsStorageFailure(t *testing.T) {
	svc := NewInvoiceAggregator(failingStore{})
	server := httptest.NewServer(newTestHTTPHandler(t, svc))
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/aggregate", strings.NewReader(`{"tenantID":1,"eventID":"obudata:0:1","value":1,"obuID":42,"unix":1700000000}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	setServiceAuth(req)
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
}

func TestAggregatorHTTPRejectsConflictingEventIDReuse(t *testing.T) {
	server := httptest.NewServer(newTestHTTPHandler(t, NewInvoiceAggregator(NewMemoryStore())))
	defer server.Close()
	for _, value := range []string{"1", "2"} {
		body := `{"tenantID":1,"eventID":"obudata:0:91","value":` + value + `,"obuID":42,"unix":1700000000}`
		req, err := http.NewRequest(http.MethodPost, server.URL+"/aggregate", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		setServiceAuth(req)
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		want := http.StatusOK
		if value == "2" {
			want = http.StatusConflict
		}
		if resp.StatusCode != want {
			t.Errorf("value %s status = %d, want %d", value, resp.StatusCode, want)
		}
	}
}

func TestAggregatorHTTPConcurrentRequestsAccumulate(t *testing.T) {
	svc := NewInvoiceAggregator(NewMemoryStore())
	server := httptest.NewServer(newTestHTTPHandler(t, svc))
	defer server.Close()

	const requests = 40
	var wg sync.WaitGroup
	errCh := make(chan error, requests)
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"tenantID":1,"eventID":"obudata:0:%d","value":0.5,"obuID":77,"unix":1700000000}`, i)
			req, err := http.NewRequest(http.MethodPost, server.URL+"/aggregate", strings.NewReader(body))
			if err != nil {
				errCh <- err
				return
			}
			req.Header.Set("Content-Type", "application/json")
			setServiceAuth(req)
			resp, err := server.Client().Do(req)
			if err != nil {
				errCh <- err
				return
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errCh <- fmt.Errorf("aggregate status %d, want 200", resp.StatusCode)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	req, err := http.NewRequest(http.MethodGet, server.URL+"/invoice?obu=77", nil)
	if err != nil {
		t.Fatal(err)
	}
	setTenantAuth(req)
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body string
	if _, err := fmt.Fscan(resp.Body, &body); err != nil {
		t.Fatal(err)
	}
	wantDistance := fmt.Sprintf(`"totalDistance":%g`, requests*0.5)
	if !strings.Contains(body, wantDistance) {
		t.Errorf("invoice %s missing expected distance %s", body, wantDistance)
	}
}

func TestAggregatorDeduplicatesKafkaEventIdentity(t *testing.T) {
	store := NewMemoryStore()
	svc := NewInvoiceAggregator(store)
	event := types.Distance{TenantID: types.LegacyTenantID, EventID: "obudata:0:17", Value: 4.25, OBUID: 77, Unix: 1700000000}
	if err := svc.AggregateDistance(event); err != nil {
		t.Fatalf("first aggregation: %v", err)
	}
	if err := svc.AggregateDistance(event); err != nil {
		t.Fatalf("replayed aggregation: %v", err)
	}
	invoice, err := svc.Calculate_Invoice(1, 77)
	if err != nil {
		t.Fatalf("calculate invoice: %v", err)
	}
	if invoice.TotalDistance != event.Value {
		t.Fatalf("total distance = %v, want %v after duplicate delivery", invoice.TotalDistance, event.Value)
	}
}

func TestAggregatorRejectsEventIdentityReuseWithDifferentDistance(t *testing.T) {
	svc := NewInvoiceAggregator(NewMemoryStore())
	first := types.Distance{TenantID: types.LegacyTenantID, EventID: "obudata:0:18", Value: 4.25, OBUID: 77, Unix: 1700000000}
	second := first
	second.Value = 99
	if err := svc.AggregateDistance(first); err != nil {
		t.Fatal(err)
	}
	if err := svc.AggregateDistance(second); !errors.Is(err, ErrEventIDConflict) {
		t.Fatalf("conflicting replay error = %v, want %v", err, ErrEventIDConflict)
	}
}

func TestAggregatorRejectsInvalidEventIdentity(t *testing.T) {
	for name, eventID := range map[string]string{
		"empty":      "",
		"whitespace": "  event  ",
		"oversized":  strings.Repeat("x", 513),
	} {
		t.Run(name, func(t *testing.T) {
			svc := NewInvoiceAggregator(NewMemoryStore())
			err := svc.AggregateDistance(types.Distance{TenantID: types.LegacyTenantID, EventID: eventID, OBUID: 1, Value: 1, Unix: 1})
			if err == nil {
				t.Fatal("expected invalid event identity to be rejected")
			}
		})
	}
}

func TestAggregatorConcurrentReplayAppliesOnce(t *testing.T) {
	svc := NewInvoiceAggregator(NewMemoryStore())
	event := types.Distance{TenantID: types.LegacyTenantID, EventID: "obudata:2:900", Value: 1.5, OBUID: 88, Unix: 1700000000}
	const deliveries = 64
	var wg sync.WaitGroup
	errCh := make(chan error, deliveries)
	for i := 0; i < deliveries; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errCh <- svc.AggregateDistance(event) }()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Errorf("aggregate replay: %v", err)
		}
	}
	invoice, err := svc.Calculate_Invoice(1, event.OBUID)
	if err != nil {
		t.Fatal(err)
	}
	if invoice.TotalDistance != event.Value {
		t.Fatalf("distance = %v, want %v after %d deliveries", invoice.TotalDistance, event.Value, deliveries)
	}
}

func TestAggregatorHTTPFlowPersistsAndReturnsInvoice(t *testing.T) {
	svc := NewInvoiceAggregator(NewMemoryStore())
	server := httptest.NewServer(newTestHTTPHandler(t, svc))
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/aggregate", strings.NewReader(`{"tenantID":1,"eventID":"obudata:0:1","value":2.5,"obuID":42,"unix":1700000000}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	setServiceAuth(req)
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("aggregate status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("aggregate content type = %q, want application/json", got)
	}

	invoiceReq, err := http.NewRequest(http.MethodGet, server.URL+"/invoice?obu=42", nil)
	if err != nil {
		t.Fatal(err)
	}
	setTenantAuth(invoiceReq)
	resp, err = server.Client().Do(invoiceReq)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("invoice status = %d, want 200", resp.StatusCode)
	}
	var invoiceBody string
	if _, err := fmt.Fscan(resp.Body, &invoiceBody); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(invoiceBody, `"totalDistance":2.5`) || !strings.Contains(invoiceBody, `"totalAmount":25`) {
		t.Errorf("invoice response does not contain expected totals: %q", invoiceBody)
	}
}

type failingStore struct{}

func (failingStore) Insert(types.Distance) error     { return errors.New("storage unavailable") }
func (failingStore) Get(int64, int) (float64, error) { return 0, errors.New("storage unavailable") }

func TestAggregatorHTTPAuthorizationAndTenantIsolation(t *testing.T) {
	svc := NewInvoiceAggregator(NewMemoryStore())
	for _, event := range []types.Distance{
		{TenantID: 1, EventID: "same:0:1", Value: 2, OBUID: 42, Unix: 1700000000},
		{TenantID: 2, EventID: "same:0:1", Value: 9, OBUID: 42, Unix: 1700000000},
	} {
		if err := svc.AggregateDistance(event); err != nil {
			t.Fatal(err)
		}
	}
	handler := newTestHTTPHandler(t, svc)
	for _, tc := range []struct {
		name, token string
		wantStatus  int
		wantTotal   string
	}{
		{name: "missing token", wantStatus: http.StatusUnauthorized},
		{name: "unknown token", token: "not-a-valid-token-0123456789abcdef", wantStatus: http.StatusUnauthorized},
		{name: "device cannot query invoices", token: testDeviceToken, wantStatus: http.StatusForbidden},
		{name: "tenant one sees only own ledger", token: testTenantToken, wantStatus: http.StatusOK, wantTotal: `"totalDistance":2`},
		{name: "tenant two sees only own ledger", token: "tenant-two-test-token-0123456789abcdef", wantStatus: http.StatusOK, wantTotal: `"totalDistance":9`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/invoice?obu=42", nil)
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			if resp.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.Code, tc.wantStatus)
			}
			if tc.wantTotal != "" && !strings.Contains(resp.Body.String(), tc.wantTotal) {
				t.Fatalf("body %s missing %q", resp.Body.String(), tc.wantTotal)
			}
		})
	}
}
