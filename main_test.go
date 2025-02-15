package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func BenchmarkSessionManager(b *testing.B) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		PoolSize: 100,
	})
	defer rdb.Close()

	sm := NewSessionManager(rdb, 5*time.Minute)
	ctx := context.Background()

	// Pre-warm connection pool
	for i := 0; i < 10; i++ {
		rdb.Ping(ctx)
	}

	benchCases := []struct {
		name       string
		numWorkers int
	}{
		{"Hash/Concurrent5", 5},
		{"List/Concurrent5", 5},
	}

	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			rdb.FlushAll(ctx)
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				wg := sync.WaitGroup{}
				wg.Add(bc.numWorkers)

				// Launch concurrent workers
				for j := 0; j < bc.numWorkers; j++ {
					go func(j int) {
						defer wg.Done()
						userID := fmt.Sprintf("user_%d_%d", i, j)

						if strings.HasPrefix(bc.name, "Hash") {
							if err := sm.ProcessWithSession(ctx, userID); err != nil {
								b.Error(err)
							}
						} else {
							if err := sm.NewProcessWithSession(ctx, userID); err != nil {
								b.Error(err)
							}
						}
					}(j)
				}
				wg.Wait()
			}

			b.StopTimer()
			rdb.FlushAll(ctx)
		})
	}
}
