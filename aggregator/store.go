package main

import (
	"fmt"
	"sync"

	"github.com/Tanmoy095/FleetToll-Engine/types"
)

type MemoryStore struct {
	mu   sync.RWMutex
	data map[int]float64
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[int]float64),
	}
}
func (s *MemoryStore) Insert(distance types.Distance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[distance.OBUID] += distance.Value
	return nil

}

func (s *MemoryStore) Get(id int) (float64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	dist, ok := s.data[id]
	if !ok {
		return 0, fmt.Errorf("%w: OBU ID %d", ErrInvoiceNotFound, id)
	}
	return dist, nil

}
