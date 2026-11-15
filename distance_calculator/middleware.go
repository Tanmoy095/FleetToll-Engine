package main

import (
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
