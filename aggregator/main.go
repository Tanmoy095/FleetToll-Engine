package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"mime"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Tanmoy095/FleetToll-Engine/internal/postgres"
	"github.com/Tanmoy095/FleetToll-Engine/types"
)

const maxAggregateBodyBytes = 1 << 20

func main() {
	listenAddr := flag.String("listenaddr", ":3000", "the listen address of the HTTP server")
	flag.Parse()

	var store storer = NewMemoryStore()
	if databaseURL := os.Getenv("DATABASE_URL"); databaseURL != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		pool, err := postgres.Open(ctx, databaseURL)
		cancel()
		if err != nil {
			log.Fatalf("open PostgreSQL: %v", err)
		}
		defer pool.Close()
		store = NewPostgresStore(pool)
	} else {
		log.Print("DATABASE_URL is unset; using volatile in-memory aggregation")
	}
	svc := NewLoggingMiddleware(NewInvoiceAggregator(store))
	if err := makeHttpTransport(*listenAddr, svc); err != nil {
		log.Fatalf("aggregator HTTP server failed: %v", err)
	}
}

func makeHttpTransport(listenAddr string, svc Aggregator) error {
	fmt.Println("HTTP transport running on port ", listenAddr)
	return http.ListenAndServe(listenAddr, newHTTPHandler(svc))
}

func newHTTPHandler(svc Aggregator) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/aggregate", handleAggregate(svc))
	mux.HandleFunc("/invoice", handleGetInvoice(svc))
	return mux
}

func handleGetInvoice(svc Aggregator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, http.MethodGet)
			return
		}

		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid query string"})
			return
		}
		values, ok := query["obu"]
		if !ok || len(values) != 1 || values[0] == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "exactly one OBU ID is required"})
			return
		}
		obuID, err := strconv.Atoi(values[0])
		if err != nil || obuID <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "OBU ID must be a positive integer"})
			return
		}

		invoice, err := svc.Calculate_Invoice(obuID)
		if err != nil {
			if errors.Is(err, ErrInvoiceNotFound) {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "invoice not found"})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not retrieve invoice"})
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, http.StatusOK, invoice)
	}
}

func handleAggregate(svc Aggregator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w, http.MethodPost)
			return
		}
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || !strings.EqualFold(mediaType, "application/json") {
			writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "Content-Type must be application/json"})
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, maxAggregateBodyBytes)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var distance types.Distance
		if err := decoder.Decode(&distance); err != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
				return
			}
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON request body"})
			return
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body must contain one JSON object"})
			return
		}
		if !validEventID(distance.EventID) || distance.OBUID <= 0 || math.IsNaN(distance.Value) || math.IsInf(distance.Value, 0) || distance.Value < 0 || distance.Unix <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "distance must include an event ID, positive OBU ID, non-negative finite value, and positive timestamp"})
			return
		}
		if err := svc.AggregateDistance(distance); err != nil {
			if errors.Is(err, ErrEventIDConflict) {
				writeJSON(w, http.StatusConflict, map[string]string{"error": "event ID conflicts with an existing distance"})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not aggregate distance"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "aggregated"})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(v)
}

func writeMethodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
}
