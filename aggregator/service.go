package main

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/Tanmoy095/FleetToll-Engine/types"
)

const basePrice = 10.0

var ErrInvoiceNotFound = errors.New("invoice not found")
var ErrEventIDConflict = errors.New("event identity already used for different distance data")

type Aggregator interface {
	AggregateDistance(types.Distance) error
	Calculate_Invoice(tenantID int64, obuID int) (*types.Invoice, error)
}
type storer interface {
	Insert(types.Distance) error
	Get(int64, int) (float64, error)
}

type InvoiceAggregator struct {
	store storer
}

func NewInvoiceAggregator(store storer) Aggregator {
	return &InvoiceAggregator{
		store: store,
	}
}

func (i *InvoiceAggregator) AggregateDistance(distance types.Distance) error {
	if distance.TenantID <= 0 || !validEventID(distance.EventID) || distance.OBUID <= 0 || math.IsNaN(distance.Value) || math.IsInf(distance.Value, 0) || distance.Value < 0 || distance.Unix <= 0 {
		return fmt.Errorf("invalid distance record")
	}
	if err := i.store.Insert(distance); err != nil {
		return fmt.Errorf("insert distance: %w", err)
	}
	return nil
}

func validEventID(eventID string) bool {
	return eventID != "" && len(eventID) <= 512 && strings.TrimSpace(eventID) == eventID
}
func (i *InvoiceAggregator) Calculate_Invoice(tenantID int64, obuID int) (*types.Invoice, error) {
	if tenantID <= 0 || obuID <= 0 {
		return nil, fmt.Errorf("tenant and OBU IDs must be positive")
	}

	dist, err := i.store.Get(tenantID, obuID)
	if err != nil {
		return nil, fmt.Errorf("OBU ID %d: %w", obuID, err)

	}
	inv := &types.Invoice{
		TenantID:      tenantID,
		OBUID:         obuID,
		TotalDistance: dist,
		TotalAmount:   basePrice * dist, // 10 cents per km
	}
	return inv, nil

}
