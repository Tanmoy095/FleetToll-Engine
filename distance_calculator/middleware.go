package main

import (
	"context"
	"time"

	"github.com/Tanmoy095/FleetToll-Engine/types"
	"github.com/sirupsen/logrus"
)

type LogMiddleware struct {
	next CalculatorServicer
}

func NewLogMiddleware(next CalculatorServicer) *LogMiddleware {
	return &LogMiddleware{
		next: next,
	}
}

func (m *LogMiddleware) CalculateEvent(ctx context.Context, eventID string, data types.OBUData) (float64, error) {
	durable, ok := m.next.(EventCalculator)
	if !ok {
		return m.next.CalculateDistance(data)
	}
	return durable.CalculateEvent(ctx, eventID, data)
}

func (m *LogMiddleware) CalculateDistance(data types.OBUData) (dist float64, err error) {
	defer func(start time.Time) {
		logrus.WithFields(logrus.Fields{
			"took":     time.Since(start),
			"error":    err,
			"obuID":    data.OBUID,
			"distance": dist,
		}).Info("Calculate distance")

	}(time.Now())
	dist, err = m.next.CalculateDistance(data)
	return

}
