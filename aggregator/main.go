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
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Tanmoy095/FleetToll-Engine/internal/postgres"
	"github.com/Tanmoy095/FleetToll-Engine/types"
)

const maxAggregateBodyBytes = 1 << 20

func main() {
	listenAddr := flag.String("listenaddr", ":3000", "the listen address of the HTTP server")
	grpcListenAddr := flag.String("grpc-listenaddr", "127.0.0.1:3001", "the listen address of the internal gRPC server")
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runTransports(ctx, *listenAddr, *grpcListenAddr, svc); err != nil {
		log.Fatalf("aggregator server failed: %v", err)
	}
}

func runTransports(ctx context.Context, httpAddr, grpcAddr string, svc Aggregator) error {
	httpListener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return fmt.Errorf("listen for HTTP: %w", err)
	}
	defer httpListener.Close()
	grpcListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("listen for gRPC: %w", err)
	}
	defer grpcListener.Close()
	httpServer := &http.Server{Handler: newHTTPHandler(svc), ReadHeaderTimeout: 5 * time.Second}
	grpcServer := newGRPCServer(svc)
	httpErrors := make(chan error, 1)
	grpcErrors := make(chan error, 1)
	go func() { httpErrors <- httpServer.Serve(httpListener) }()
	go func() { grpcErrors <- grpcServer.Serve(grpcListener) }()
	fmt.Printf("aggregator HTTP listening on %s; gRPC listening on %s\n", httpAddr, grpcAddr)

	select {
	case err := <-httpErrors:
		grpcServer.Stop()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("HTTP server: %w", err)
	case err := <-grpcErrors:
		_ = httpServer.Close()
		if err != nil {
			return fmt.Errorf("gRPC server: %w", err)
		}
		return nil
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownErr := httpServer.Shutdown(shutdownCtx)
		grpcStopped := make(chan struct{})
		go func() { grpcServer.GracefulStop(); close(grpcStopped) }()
		select {
		case <-grpcStopped:
		case <-shutdownCtx.Done():
			grpcServer.Stop()
		}
		if shutdownErr != nil {
			return fmt.Errorf("shut down HTTP server: %w", shutdownErr)
		}
		return nil
	}
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
