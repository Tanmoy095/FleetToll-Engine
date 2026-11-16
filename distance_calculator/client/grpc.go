package client

import (
	"context"
	"fmt"
	"time"

	fleettollv1 "github.com/Tanmoy095/FleetToll-Engine/api/fleettoll/v1"
	"github.com/Tanmoy095/FleetToll-Engine/internal/tenantauth"
	"github.com/Tanmoy095/FleetToll-Engine/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCClient is the typed internal client used by the Kafka processing path.
// Its transport security is intentionally limited to local development today.
type GRPCClient struct {
	conn   *grpc.ClientConn
	client fleettollv1.AggregatorServiceClient
	token  string
}

func DialGRPC(ctx context.Context, target, serviceToken string) (*GRPCClient, error) {
	if err := tenantauth.ValidateServiceToken(serviceToken); err != nil {
		return nil, err
	}
	conn, err := grpc.DialContext(ctx, target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("create aggregator gRPC connection: %w", err)
	}
	return &GRPCClient{conn: conn, client: fleettollv1.NewAggregatorServiceClient(conn), token: serviceToken}, nil
}

func (c *GRPCClient) Close() error { return c.conn.Close() }

func (c *GRPCClient) AggregateInvoice(distance types.Distance) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.AggregateInvoiceContext(ctx, distance)
}

func (c *GRPCClient) AggregateInvoiceContext(ctx context.Context, distance types.Distance) error {
	ctx = tenantauth.WithServiceToken(ctx, c.token)
	_, err := c.client.AggregateDistance(ctx, &fleettollv1.AggregateDistanceRequest{
		TenantId: distance.TenantID, EventId: distance.EventID, ObuId: int64(distance.OBUID), DistanceKm: distance.Value, EventUnixNano: distance.Unix,
	})
	if err != nil {
		return fmt.Errorf("aggregator gRPC AggregateDistance: %w", err)
	}
	return nil
}
