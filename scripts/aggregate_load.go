// Command aggregate_load sends one request from each concurrent virtual user.
// It is a local development probe, not a production capacity certification.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

type result struct {
	elapsed time.Duration
	err     error
}

func main() {
	url := flag.String("url", "http://127.0.0.1:3000/aggregate", "aggregate endpoint")
	users := flag.Int("users", 1000, "concurrent one-shot virtual users")
	flag.Parse()
	if *users < 1 {
		fmt.Fprintln(os.Stderr, "users must be positive")
		os.Exit(2)
	}
	token := os.Getenv("AGGREGATOR_GRPC_TOKEN")
	if len(token) < 32 {
		fmt.Fprintln(os.Stderr, "AGGREGATOR_GRPC_TOKEN must contain at least 32 bytes")
		os.Exit(2)
	}
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConns: *users, MaxIdleConnsPerHost: *users}}
	start := make(chan struct{})
	results := make(chan result, *users)
	var wg sync.WaitGroup
	for i := 0; i < *users; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			body, _ := json.Marshal(map[string]any{"tenantID": 1, "eventID": fmt.Sprintf("load:%d:%d", time.Now().UnixNano(), i), "value": 0.01, "obuID": 900001, "unix": time.Now().UnixNano()})
			request, err := http.NewRequest(http.MethodPost, *url, bytes.NewReader(body))
			if err != nil {
				results <- result{err: err}
				return
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", "Bearer "+token)
			began := time.Now()
			response, err := client.Do(request)
			elapsed := time.Since(began)
			if err != nil {
				results <- result{elapsed: elapsed, err: err}
				return
			}
			response.Body.Close()
			if response.StatusCode != http.StatusOK {
				results <- result{elapsed: elapsed, err: fmt.Errorf("HTTP %d", response.StatusCode)}
				return
			}
			results <- result{elapsed: elapsed}
		}(i)
	}
	began := time.Now()
	close(start)
	wg.Wait()
	close(results)
	total := time.Since(began)
	latencies := make([]time.Duration, 0, *users)
	failures := 0
	for outcome := range results {
		latencies = append(latencies, outcome.elapsed)
		if outcome.err != nil {
			failures++
			fmt.Fprintln(os.Stderr, outcome.err)
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(p float64) time.Duration { index := int(float64(len(latencies)-1) * p); return latencies[index] }
	fmt.Printf("requests=%d concurrency=%d elapsed=%s throughput=%.1f_req_s p50=%s p95=%s p99=%s errors=%d\n", *users, *users, total, float64(*users)/total.Seconds(), percentile(.50), percentile(.95), percentile(.99), failures)
	if failures != 0 {
		os.Exit(1)
	}
}
