package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Tanmoy095/FleetToll-Engine/types"
)

type Client struct {
	Endpoint   string
	HTTPClient *http.Client
}

func NewClient(endpoint string) *Client {
	return &Client{
		Endpoint:   endpoint,
		HTTPClient: &http.Client{Timeout: 5 * time.Second},
	}
}

func (c *Client) AggregateInvoice(distance types.Distance) error {
	return c.AggregateInvoiceContext(context.Background(), distance)
}

func (c *Client) AggregateInvoiceContext(ctx context.Context, distance types.Distance) error {
	b, err := json.Marshal(distance)
	if err != nil {
		return fmt.Errorf("encode distance request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Endpoint, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("create distance request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send distance request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("aggregator returned status %d", resp.StatusCode)
	}
	return nil
}
