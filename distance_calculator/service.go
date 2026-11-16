package main

import (
	"fmt"
	"math"
	"sync"

	"github.com/Tanmoy095/FleetToll-Engine/types"
)

const earthRadiusKilometers = 6371.0088

type CalculatorServicer interface {
	// Add any necessary dependencies here
	CalculateDistance(types.OBUData) (float64, error)
}

type CalculatorService struct {
	mu         sync.Mutex
	prevPoints map[vehicleKey]coordinate
}

type vehicleKey struct {
	tenantID int64
	obuID    int
}

type coordinate struct {
	latitude  float64
	longitude float64
}

func NewCalculatorService() CalculatorServicer {
	return &CalculatorService{prevPoints: make(map[vehicleKey]coordinate)}
}

func (s *CalculatorService) CalculateDistance(data types.OBUData) (float64, error) {
	if data.TenantID <= 0 || data.OBUID <= 0 {
		return 0, fmt.Errorf("tenant and OBU IDs must be positive")
	}
	if math.IsNaN(data.Latitude) || math.IsInf(data.Latitude, 0) || data.Latitude < -90 || data.Latitude > 90 {
		return 0, fmt.Errorf("latitude %v is outside valid range [-90, 90]", data.Latitude)
	}
	if math.IsNaN(data.Longitude) || math.IsInf(data.Longitude, 0) || data.Longitude < -180 || data.Longitude > 180 {
		return 0, fmt.Errorf("longitude %v is outside valid range [-180, 180]", data.Longitude)
	}

	current := coordinate{latitude: data.Latitude, longitude: data.Longitude}
	s.mu.Lock()
	defer s.mu.Unlock()

	key := vehicleKey{tenantID: data.TenantID, obuID: data.OBUID}
	previous, exists := s.prevPoints[key]
	s.prevPoints[key] = current
	if !exists {
		return 0, nil
	}
	return haversineDistance(previous, current), nil
}

func haversineDistance(from, to coordinate) float64 {
	toRadians := math.Pi / 180
	lat1 := from.latitude * toRadians
	lat2 := to.latitude * toRadians
	deltaLat := (to.latitude - from.latitude) * toRadians
	deltaLon := (to.longitude - from.longitude) * toRadians

	a := math.Sin(deltaLat/2)*math.Sin(deltaLat/2) +
		math.Cos(lat1)*math.Cos(lat2)*math.Sin(deltaLon/2)*math.Sin(deltaLon/2)
	// Floating-point rounding can move a just outside its mathematical [0, 1] range.
	a = math.Max(0, math.Min(1, a))
	return 2 * earthRadiusKilometers * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}
