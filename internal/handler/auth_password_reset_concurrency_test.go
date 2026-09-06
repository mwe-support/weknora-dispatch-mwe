package handler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPasswordResetConcurrentBudgetAndOneTimeConsumption(t *testing.T) {
	h, _ := newPasswordResetHandler(t, &passwordResetUserService{})
	ctx := context.Background()
	key := "auth:password-reset:code:test"
	value := []byte("challenge-one")
	require.NoError(t, h.redisClient.Set(ctx, key, value, passwordResetCodeTTL).Err())
	var allowed atomic.Int32
	var failed atomic.Int32
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := h.reservePasswordResetAttempt(ctx, key, value)
			if err != nil {
				failed.Add(1)
			}
			if ok {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	require.Zero(t, failed.Load())
	require.EqualValues(t, passwordResetAttempts, allowed.Load())
	var consumed atomic.Int32
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := h.consumePasswordResetChallenge(ctx, key, value)
			if err != nil {
				failed.Add(1)
			}
			if ok {
				consumed.Add(1)
			}
		}()
	}
	wg.Wait()
	require.Zero(t, failed.Load())
	require.EqualValues(t, 1, consumed.Load())
	// A delayed request from the old challenge cannot affect the new one.
	fresh := []byte("challenge-two")
	require.NoError(t, h.redisClient.Set(ctx, key, fresh, passwordResetCodeTTL).Err())
	ok, err := h.reservePasswordResetAttempt(ctx, key, value)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = h.consumePasswordResetChallenge(ctx, key, value)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = h.reservePasswordResetAttempt(ctx, key, fresh)
	require.NoError(t, err)
	require.True(t, ok)
}
