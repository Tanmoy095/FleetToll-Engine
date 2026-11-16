package main

import (
	"fmt"
	"sync"

	"github.com/Tanmoy095/FleetToll-Engine/types"
)

type MemoryStore struct {
	mu     sync.RWMutex
	data   map[tenantOBU]float64
	events map[tenantEvent]types.Distance
}

type tenantOBU struct {
	tenantID int64
	obuID    int
}

type tenantEvent struct {
	tenantID int64
	eventID  string
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data:   make(map[tenantOBU]float64),
		events: make(map[tenantEvent]types.Distance),
	}
}
func (s *MemoryStore) Insert(distance types.Distance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := tenantEvent{tenantID: distance.TenantID, eventID: distance.EventID}
	if previous, exists := s.events[key]; exists {
		if previous.OBUID != distance.OBUID || previous.Value != distance.Value || previous.Unix != distance.Unix {
			return ErrEventIDConflict
		}
		return nil
	}
	s.events[key] = distance
	s.data[tenantOBU{tenantID: distance.TenantID, obuID: distance.OBUID}] += distance.Value
	return nil

}

func (s *MemoryStore) Get(tenantID int64, id int) (float64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	dist, ok := s.data[tenantOBU{tenantID: tenantID, obuID: id}]
	if !ok {
		return 0, fmt.Errorf("%w: OBU ID %d", ErrInvoiceNotFound, id)
	}
	return dist, nil

}
