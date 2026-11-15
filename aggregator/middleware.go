package main

import (
	"time"

	"github.com/Tanmoy095/FleetToll-Engine/types"
	"github.com/sirupsen/logrus"
)

type LoggingMiddleware struct {
	next Aggregator
}

func NewLoggingMiddleware(next Aggregator) Aggregator {
	return &LoggingMiddleware{
		next: next,
	}
}

func (l *LoggingMiddleware) AggregateDistance(distance types.Distance) (err error) {
	defer func(start time.Time) {

		logrus.WithFields(logrus.Fields{
			"took":   time.Since(start),
			"failed": err != nil,
		}).Info("Aggregate distance")
	}(time.Now())
	err = l.next.AggregateDistance(distance)
	return
}

func (l *LoggingMiddleware) Calculate_Invoice(obuID int) (invoice *types.Invoice, err error) {
	defer func(start time.Time) {
		var (
			distance float64
			amount   float64
		)
		if invoice != nil {
			distance = invoice.TotalDistance
			amount = invoice.TotalAmount
		}
		logrus.WithFields(logrus.Fields{
			"took":     time.Since(start),
			"failed":   err != nil,
			"obuID":    obuID,
			"amount":   amount,
			"distance": distance,
		}).Info("CalculateInvoice")
	}(time.Now())

	invoice, err = l.next.Calculate_Invoice(obuID)
	return
}
