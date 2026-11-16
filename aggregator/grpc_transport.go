package main

import (
	"context"
	"errors"
	"math"

	fleettollv1 "github.com/Tanmoy095/FleetToll-Engine/api/fleettoll/v1"
	"github.com/Tanmoy095/FleetToll-Engine/internal/tenantauth"
	"github.com/Tanmoy095/FleetToll-Engine/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type grpcAggregatorServer struct {
	fleettollv1.UnimplementedAggregatorServiceServer
	svc Aggregator
}

func newGRPCServer(svc Aggregator, serviceToken string) *grpc.Server {
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxAggregateBodyBytes),
		grpc.UnaryInterceptor(tenantauth.ServiceTokenUnaryInterceptor(serviceToken)),
	)
	fleettollv1.RegisterAggregatorServiceServer(server, &grpcAggregatorServer{svc: svc})
	return server
}

func (s *grpcAggregatorServer) AggregateDistance(_ context.Context, request *fleettollv1.AggregateDistanceRequest) (*fleettollv1.AggregateDistanceResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	tenantID := effectiveGRPCTenantID(request.GetTenantId())
	if tenantID <= 0 || !validEventID(request.GetEventId()) || !validGRPCOBUID(request.GetObuId()) || math.IsNaN(request.GetDistanceKm()) || math.IsInf(request.GetDistanceKm(), 0) || request.GetDistanceKm() < 0 || request.GetEventUnixNano() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "distance must include a valid event ID, positive OBU ID, non-negative finite value, and positive timestamp")
	}
	distance := types.Distance{TenantID: tenantID, EventID: request.GetEventId(), OBUID: int(request.GetObuId()), Value: request.GetDistanceKm(), Unix: request.GetEventUnixNano()}
	if err := s.svc.AggregateDistance(distance); err != nil {
		if errors.Is(err, ErrEventIDConflict) {
			return nil, status.Error(codes.AlreadyExists, "event ID conflicts with an existing distance")
		}
		return nil, status.Error(codes.Internal, "could not aggregate distance")
	}
	return &fleettollv1.AggregateDistanceResponse{Accepted: true}, nil
}

func (s *grpcAggregatorServer) GetInvoice(_ context.Context, request *fleettollv1.GetInvoiceRequest) (*fleettollv1.GetInvoiceResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	tenantID := effectiveGRPCTenantID(request.GetTenantId())
	if tenantID <= 0 || !validGRPCOBUID(request.GetObuId()) {
		return nil, status.Error(codes.InvalidArgument, "OBU ID must be a positive integer")
	}
	invoice, err := s.svc.Calculate_Invoice(tenantID, int(request.GetObuId()))
	if err != nil {
		if errors.Is(err, ErrInvoiceNotFound) {
			return nil, status.Error(codes.NotFound, "invoice not found")
		}
		return nil, status.Error(codes.Internal, "could not retrieve invoice")
	}
	return &fleettollv1.GetInvoiceResponse{ObuId: int64(invoice.OBUID), TotalDistanceKm: invoice.TotalDistance, TotalAmount: invoice.TotalAmount, TenantId: invoice.TenantID}, nil
}

func effectiveGRPCTenantID(tenantID int64) int64 {
	if tenantID == 0 {
		return types.LegacyTenantID
	}
	return tenantID
}

func validGRPCOBUID(obuID int64) bool {
	return obuID > 0 && obuID <= int64(^uint(0)>>1)
}
