package tencentdocs

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestNativeRequestBudgetRedisCoordinatesIndependentClients(t *testing.T) {
	addr := os.Getenv("PROCESSING_TEST_REDIS")
	if addr == "" {
		t.Skip("isolated Redis is not configured")
	}
	require.Equal(t, "lifecycle-redis:6379", addr)
	a := redis.NewClient(&redis.Options{Addr: addr})
	b := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	key := "tdocs:request-budget:test:" + uuid.NewString()
	t.Cleanup(func() { _ = a.Del(context.Background(), key).Err() })
	var wg sync.WaitGroup
	waits := make(chan time.Duration, 12)
	failures := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			client := a
			if i%2 == 1 {
				client = b
			}
			delay, err := redisRequestBudget(context.Background(), client, key, 0)
			waits <- delay
			failures <- err
		}(i)
	}
	wg.Wait()
	close(waits)
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	winners := 0
	for delay := range waits {
		if delay == 0 {
			winners++
		} else {
			require.Greater(t, delay, time.Duration(0))
		}
	}
	require.Equal(t, 1, winners)
	_, err := redisRequestBudget(context.Background(), a, key, 5*time.Minute)
	require.NoError(t, err)
	delay, err := redisRequestBudget(context.Background(), b, key, 0)
	require.NoError(t, err)
	require.Greater(t, delay, 299*time.Second)
	_ = b.Close()
	_, err = redisRequestBudget(context.Background(), b, key, 0)
	require.Error(t, err)
}
