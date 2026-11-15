package main

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/Tanmoy095/FleetToll-Engine/types"
)

func TestCalculatorServiceCalculatesHaversineKilometers(t *testing.T) {
	tests := []struct {
		name string
		from types.OBUData
		to   types.OBUData
		want float64
	}{
		{
			name: "first point has no distance",
			from: types.OBUData{OBUID: 1, Latitude: 0, Longitude: 0},
			to:   types.OBUData{OBUID: 1, Latitude: 0, Longitude: 0},
			want: 0,
		},
		{
			name: "one degree along equator is about 111 kilometers",
			from: types.OBUData{OBUID: 2, Latitude: 0, Longitude: 0},
			to:   types.OBUData{OBUID: 2, Latitude: 0, Longitude: 1},
			want: 111.1949266,
		},
		{
			name: "same coordinates have zero distance",
			from: types.OBUData{OBUID: 3, Latitude: 45, Longitude: 90},
			to:   types.OBUData{OBUID: 3, Latitude: 45, Longitude: 90},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := NewCalculatorService()
			if _, err := svc.CalculateDistance(tt.from); err != nil {
				t.Fatalf("first point: %v", err)
			}
			got, err := svc.CalculateDistance(tt.to)
			if err != nil {
				t.Fatalf("second point: %v", err)
			}
			if math.Abs(got-tt.want) > 0.001 {
				t.Errorf("distance = %.8f km, want %.8f km", got, tt.want)
			}
		})
	}
}

func TestCalculatorServiceKeepsIndependentVehicleState(t *testing.T) {
	svc := NewCalculatorService()
	points := []types.OBUData{
		{OBUID: 10, Latitude: 0, Longitude: 0},
		{OBUID: 20, Latitude: 0, Longitude: 10},
		{OBUID: 10, Latitude: 0, Longitude: 1},
	}
	for i, point := range points {
		got, err := svc.CalculateDistance(point)
		if err != nil {
			t.Fatalf("point %d: %v", i, err)
		}
		if i == 2 && math.Abs(got-111.1949266) > 0.001 {
			t.Errorf("vehicle 10 distance = %.8f km, want 111.1949266 km", got)
		}
	}
}

func TestCalculatorServiceRejectsInvalidCoordinates(t *testing.T) {
	tests := []struct {
		name string
		data types.OBUData
	}{
		{name: "latitude below range", data: types.OBUData{OBUID: 1, Latitude: -90.0001, Longitude: 0}},
		{name: "latitude above range", data: types.OBUData{OBUID: 1, Latitude: 90.0001, Longitude: 0}},
		{name: "longitude below range", data: types.OBUData{OBUID: 1, Latitude: 0, Longitude: -180.0001}},
		{name: "longitude above range", data: types.OBUData{OBUID: 1, Latitude: 0, Longitude: 180.0001}},
		{name: "non-finite latitude", data: types.OBUData{OBUID: 1, Latitude: math.NaN(), Longitude: 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewCalculatorService().CalculateDistance(tt.data); err == nil {
				t.Fatal("expected invalid coordinate error")
			}
		})
	}
}

func TestCalculatorServiceRejectsInvalidOBUIdentity(t *testing.T) {
	for _, obuID := range []int{0, -1} {
		t.Run(fmt.Sprintf("obu_%d", obuID), func(t *testing.T) {
			if _, err := NewCalculatorService().CalculateDistance(types.OBUData{OBUID: obuID, Latitude: 0, Longitude: 0}); err == nil {
				t.Fatal("expected invalid OBU ID error")
			}
		})
	}
}

func TestCalculatorServiceSupportsConcurrentVehicles(t *testing.T) {
	svc := NewCalculatorService()
	const vehicles = 100
	var wg sync.WaitGroup
	errCh := make(chan error, vehicles)
	for obuID := 1; obuID <= vehicles; obuID++ {
		wg.Add(1)
		go func(obuID int) {
			defer wg.Done()
			if got, err := svc.CalculateDistance(types.OBUData{OBUID: obuID, Latitude: 0, Longitude: 0}); err != nil {
				errCh <- err
			} else if got != 0 {
				errCh <- fmt.Errorf("first point for OBU %d = %v km, want 0", obuID, got)
			}
		}(obuID)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
