package main

import (
	"errors"
	"fmt"
	"math"

	"github.com/Tanmoy095/FleetToll-Engine/types"
)

const basePrice = 10.0

var ErrInvoiceNotFound = errors.New("invoice not found")

type Aggregator interface {
	AggregateDistance(types.Distance) error
	Calculate_Invoice(obuID int) (*types.Invoice, error)
}
type storer interface {
	Insert(types.Distance) error
	Get(int) (float64, error)
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
	if distance.OBUID <= 0 || math.IsNaN(distance.Value) || math.IsInf(distance.Value, 0) || distance.Value < 0 || distance.Unix <= 0 {
		return fmt.Errorf("invalid distance record")
	}
	if err := i.store.Insert(distance); err != nil {
		return fmt.Errorf("insert distance: %w", err)
	}
	return nil
}
func (i *InvoiceAggregator) Calculate_Invoice(obuID int) (*types.Invoice, error) {

	dist, err := i.store.Get(obuID)
	if err != nil {
		return nil, fmt.Errorf("OBU ID %d: %w", obuID, err)

	}
	inv := &types.Invoice{
		OBUID:         obuID,
		TotalDistance: dist,
		TotalAmount:   basePrice * dist, // 10 cents per km
	}
	return inv, nil

}
