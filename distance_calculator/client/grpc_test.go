package client

import (
	"context"
	"net"
	"testing"
	"time"

	fleettollv1 "github.com/Tanmoy095/FleetToll-Engine/api/fleettoll/v1"
	"github.com/Tanmoy095/FleetToll-Engine/types"
	"google.golang.org/grpc"
)

type recordingGRPCAggregator struct {
	fleettollv1.UnimplementedAggregatorServiceServer
	got chan *fleettollv1.AggregateDistanceRequest
}

func (s *recordingGRPCAggregator) AggregateDistance(_ context.Context, request *fleettollv1.AggregateDistanceRequest) (*fleettollv1.AggregateDistanceResponse, error) {
	s.got <- request
	return &fleettollv1.AggregateDistanceResponse{Accepted: true}, nil
}

func TestGRPCClientSendsTypedDistanceAndHonorsContext(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	service := &recordingGRPCAggregator{got: make(chan *fleettollv1.AggregateDistanceRequest, 1)}
	fleettollv1.RegisterAggregatorServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := DialGRPC(ctx, listener.Addr().String())
	if err != nil {
		t.Fatalf("DialGRPC() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	want := types.Distance{EventID: "obudata:2:8", OBUID: 17, Value: 12.75, Unix: 1700000000000000000}
	if err := client.AggregateInvoiceContext(ctx, want); err != nil {
		t.Fatalf("AggregateInvoiceContext() error = %v", err)
	}
	got := <-service.got
	if got.GetEventId() != want.EventID || got.GetObuId() != int64(want.OBUID) || got.GetDistanceKm() != want.Value || got.GetEventUnixNano() != want.Unix {
		t.Fatalf("RPC request = %+v, want %+v", got, want)
	}
}

func TestDialGRPCDoesNotReportUnavailableTargetAsConnected(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	target := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	conn, err := DialGRPC(ctx, target)
	if conn != nil {
		_ = conn.Close()
	}
	if err == nil {
		t.Fatal("DialGRPC() returned success for an unavailable target")
	}
}
