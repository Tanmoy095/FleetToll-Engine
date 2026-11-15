// Command grpc_aggregate_load sends one request from each concurrent user.
// It is a local development probe, not a production capacity certification.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	fleettollv1 "github.com/Tanmoy095/FleetToll-Engine/api/fleettoll/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type result struct {
	elapsed time.Duration
	err     error
}

func main() {
	target := flag.String("target", "127.0.0.1:3001", "aggregator gRPC target")
	users := flag.Int("users", 1000, "concurrent one-shot virtual users")
	flag.Parse()
	if *users < 1 {
		fmt.Fprintln(os.Stderr, "users must be positive")
		os.Exit(2)
	}
	conn, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()
	client := fleettollv1.NewAggregatorServiceClient(conn)
	start := make(chan struct{})
	results := make(chan result, *users)
	var wg sync.WaitGroup
	for i := 0; i < *users; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			request := &fleettollv1.AggregateDistanceRequest{EventId: fmt.Sprintf("load:%d:%d", time.Now().UnixNano(), i), ObuId: 900001, DistanceKm: 0.01, EventUnixNano: time.Now().UnixNano()}
			began := time.Now()
			_, err := client.AggregateDistance(ctx, request)
			results <- result{elapsed: time.Since(began), err: err}
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
	percentile := func(p float64) time.Duration { return latencies[int(float64(len(latencies)-1)*p)] }
	fmt.Printf("requests=%d concurrency=%d elapsed=%s throughput=%.1f_req_s p50=%s p95=%s p99=%s errors=%d\n", *users, *users, total, float64(*users)/total.Seconds(), percentile(.50), percentile(.95), percentile(.99), failures)
	if failures != 0 {
		os.Exit(1)
	}
}
