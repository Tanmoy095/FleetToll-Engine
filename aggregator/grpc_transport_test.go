package main

import (
	"context"
	"math"
	"net"
	"strings"
	"testing"

	fleettollv1 "github.com/Tanmoy095/FleetToll-Engine/api/fleettoll/v1"
	"github.com/Tanmoy095/FleetToll-Engine/internal/tenantauth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestGRPCAggregatorRoundTripAndReplay(t *testing.T) {
	store := NewMemoryStore()
	listener, server, conn := startGRPCTestServer(t, NewInvoiceAggregator(store))
	defer listener.Close()
	defer server.Stop()
	defer conn.Close()
	client := fleettollv1.NewAggregatorServiceClient(conn)
	ctx := tenantauth.WithServiceToken(context.Background(), testServiceToken)

	const largeOBUID int64 = 1<<31 + 7
	request := &fleettollv1.AggregateDistanceRequest{EventId: "topic:0:12", ObuId: largeOBUID, DistanceKm: 4.25, EventUnixNano: 1700000000000000000}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := client.AggregateDistance(ctx, request); err != nil {
			t.Fatalf("AggregateDistance() error = %v", err)
		}
	}
	invoice, err := client.GetInvoice(ctx, &fleettollv1.GetInvoiceRequest{ObuId: largeOBUID})
	if err != nil {
		t.Fatalf("GetInvoice() error = %v", err)
	}
	if invoice.GetTotalDistanceKm() != 4.25 || invoice.GetObuId() != largeOBUID {
		t.Fatalf("invoice = %+v, want OBU %d with 4.25 km", invoice, largeOBUID)
	}
}

func TestGRPCAggregatorMapsInvalidConflictAndNotFound(t *testing.T) {
	listener, server, conn := startGRPCTestServer(t, NewInvoiceAggregator(NewMemoryStore()))
	defer listener.Close()
	defer server.Stop()
	defer conn.Close()
	client := fleettollv1.NewAggregatorServiceClient(conn)
	ctx := tenantauth.WithServiceToken(context.Background(), testServiceToken)

	for _, invalid := range []*fleettollv1.AggregateDistanceRequest{
		{ObuId: 1},
		{EventId: "topic:0:1", ObuId: 1, DistanceKm: -1, EventUnixNano: 1700000000000000000},
		{EventId: "topic:0:1", ObuId: 1, DistanceKm: math.NaN(), EventUnixNano: 1700000000000000000},
		{EventId: "topic:0:1", ObuId: 0, DistanceKm: 1, EventUnixNano: 1700000000000000000},
	} {
		_, err := client.AggregateDistance(ctx, invalid)
		assertGRPCCode(t, err, codes.InvalidArgument)
	}

	valid := &fleettollv1.AggregateDistanceRequest{EventId: "topic:0:1", ObuId: 1, DistanceKm: 2, EventUnixNano: 1700000000000000000}
	if _, err := client.AggregateDistance(ctx, valid); err != nil {
		t.Fatal(err)
	}
	conflict := &fleettollv1.AggregateDistanceRequest{EventId: valid.GetEventId(), ObuId: valid.GetObuId(), DistanceKm: 3, EventUnixNano: valid.GetEventUnixNano()}
	_, err := client.AggregateDistance(ctx, conflict)
	assertGRPCCode(t, err, codes.AlreadyExists)

	_, err = client.GetInvoice(ctx, &fleettollv1.GetInvoiceRequest{ObuId: 99})
	assertGRPCCode(t, err, codes.NotFound)
}

func TestGRPCAggregatorRequiresServiceCredential(t *testing.T) {
	_, server, conn := startGRPCTestServer(t, NewInvoiceAggregator(NewMemoryStore()))
	defer server.Stop()
	defer conn.Close()
	client := fleettollv1.NewAggregatorServiceClient(conn)
	request := &fleettollv1.AggregateDistanceRequest{TenantId: 1, EventId: "topic:0:1", ObuId: 1, DistanceKm: 1, EventUnixNano: 1700000000000000000}
	for name, ctx := range map[string]context.Context{
		"missing": context.Background(),
		"wrong":   tenantauth.WithServiceToken(context.Background(), "wrong-service-token-0123456789abcdef"),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := client.AggregateDistance(ctx, request)
			assertGRPCCode(t, err, codes.Unauthenticated)
		})
	}
	if _, err := client.AggregateDistance(tenantauth.WithServiceToken(context.Background(), testServiceToken), request); err != nil {
		t.Fatalf("valid service token: %v", err)
	}
}

func TestGRPCAggregatorRejectsOversizedMessage(t *testing.T) {
	_, server, conn := startGRPCTestServer(t, NewInvoiceAggregator(NewMemoryStore()))
	defer server.Stop()
	defer conn.Close()
	client := fleettollv1.NewAggregatorServiceClient(conn)
	_, err := client.AggregateDistance(tenantauth.WithServiceToken(context.Background(), testServiceToken), &fleettollv1.AggregateDistanceRequest{EventId: strings.Repeat("x", maxAggregateBodyBytes), ObuId: 1, DistanceKm: 1, EventUnixNano: 1700000000000000000})
	assertGRPCCode(t, err, codes.ResourceExhausted)
}

func TestGRPCAggregatorDoesNotLeakStorageErrors(t *testing.T) {
	listener, server, conn := startGRPCTestServer(t, NewInvoiceAggregator(failingStore{}))
	defer listener.Close()
	defer server.Stop()
	defer conn.Close()
	client := fleettollv1.NewAggregatorServiceClient(conn)
	_, err := client.AggregateDistance(tenantauth.WithServiceToken(context.Background(), testServiceToken), &fleettollv1.AggregateDistanceRequest{EventId: "topic:0:1", ObuId: 1, DistanceKm: 2, EventUnixNano: 1700000000000000000})
	assertGRPCCode(t, err, codes.Internal)
	if got := status.Convert(err).Message(); got != "could not aggregate distance" {
		t.Fatalf("error message = %q, want sanitized message", got)
	}
}

func startGRPCTestServer(t *testing.T, svc Aggregator) (net.Listener, *grpc.Server, *grpc.ClientConn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := newGRPCServer(svc, testServiceToken)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		server.Stop()
		listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(); server.Stop(); _ = listener.Close() })
	return listener, server, conn
}

func assertGRPCCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("status.Code(%v) = %s, want %s", err, got, want)
	}
}
