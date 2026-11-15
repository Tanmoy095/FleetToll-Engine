package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Tanmoy095/FleetToll-Engine/types"
	"github.com/sirupsen/logrus"
)

func TestLoggingMiddlewareDoesNotEmitStorageErrorDetails(t *testing.T) {
	previousOutput := logrus.StandardLogger().Out
	previousLevel := logrus.GetLevel()
	var output bytes.Buffer
	logrus.SetOutput(&output)
	logrus.SetLevel(logrus.InfoLevel)
	t.Cleanup(func() { logrus.SetOutput(previousOutput); logrus.SetLevel(previousLevel) })

	svc := NewLoggingMiddleware(NewInvoiceAggregator(failingStore{}))
	_ = svc.AggregateDistance(types.Distance{EventID: "topic:0:1", OBUID: 1, Value: 1, Unix: 1})
	_, _ = svc.Calculate_Invoice(1)
	if strings.Contains(output.String(), "storage unavailable") {
		t.Fatalf("logs leaked storage error details: %s", output.String())
	}
}
