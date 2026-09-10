package tencentdocs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type credentialBudget struct {
	mu       sync.Mutex
	next     time.Time
	cooldown time.Time
}

// Lite deployments have one process. Redis deployments coordinate the same
// hashed credential across all source clients and application replicas.
var credentialBudgets sync.Map
var requestBudgetRedis atomic.Pointer[redis.Client]

func SetRequestBudgetRedis(client *redis.Client) { requestBudgetRedis.Store(client) }

type MCPBudgetWaitError struct{ RetryAfter time.Duration }

func (e *MCPBudgetWaitError) Error() string {
	return fmt.Sprintf("Tencent Docs credential cooldown: retry_after_ms=%d", e.RetryAfter.Milliseconds())
}

// Use Redis TIME so replicas with different wall clocks cannot consume the
// same slot. A cooldown update can only extend the existing provider floor.
var requestBudgetScript = redis.NewScript(`
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
local next = tonumber(redis.call('GET', KEYS[1]) or '0')
local cooldown = tonumber(ARGV[1])
if cooldown > 0 then
  next = math.max(next, now + cooldown)
  redis.call('SET', KEYS[1], next, 'PX', math.max(300000, next - now + 60000))
  return next - now
end
if next > now then return next - now end
redis.call('SET', KEYS[1], now + 2000, 'PX', 300000)
return 0
`)

func redisRequestBudget(ctx context.Context, client *redis.Client, key string, cooldown time.Duration) (time.Duration, error) {
	wait, err := requestBudgetScript.Run(ctx, client, []string{key}, cooldown.Milliseconds()).Int64()
	if err != nil {
		return 0, fmt.Errorf("Tencent Docs distributed request budget unavailable: %w", err)
	}
	return time.Duration(wait) * time.Millisecond, nil
}

type budgetTransport struct {
	base   http.RoundTripper
	budget *credentialBudget
	key    string
}

func withCredentialBudget(token string, base http.RoundTripper) http.RoundTripper {
	key := sha256.Sum256([]byte(token))
	v, _ := credentialBudgets.LoadOrStore(key, &credentialBudget{})
	return &budgetTransport{base: base, budget: v.(*credentialBudget), key: fmt.Sprintf("tdocs:request-budget:%x", key)}
}

func (t *budgetTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	for {
		if client := requestBudgetRedis.Load(); client != nil {
			delay, err := redisRequestBudget(req.Context(), client, t.key, 0)
			if err != nil {
				return nil, err
			}
			if delay == 0 {
				break
			}
			if err := waitRequestBudget(req.Context(), delay); err != nil {
				return nil, err
			}
			continue
		}
		now := time.Now()
		t.budget.mu.Lock()
		at := t.budget.next
		if at.Before(t.budget.cooldown) {
			at = t.budget.cooldown
		}
		if !at.After(now) {
			t.budget.next = now.Add(2 * time.Second)
			t.budget.mu.Unlock()
			break
		}
		t.budget.mu.Unlock()
		if err := waitRequestBudget(req.Context(), time.Until(at)); err != nil {
			return nil, err
		}
	}
	response, err := t.base.RoundTrip(req)
	if err == nil && response != nil && (response.StatusCode == 429 || response.StatusCode == 503) {
		delay := retryAfter(response.Header.Get("Retry-After"), time.Now())
		delay = max(delay, initialMCPRetryDelay)
		t.budget.mu.Lock()
		until := time.Now().Add(delay)
		if until.After(t.budget.cooldown) {
			t.budget.cooldown = until
		}
		t.budget.mu.Unlock()
		if client := requestBudgetRedis.Load(); client != nil {
			if _, budgetErr := redisRequestBudget(req.Context(), client, t.key, delay); budgetErr != nil {
				_ = response.Body.Close()
				return nil, budgetErr
			}
		}
		_ = response.Body.Close()
		return nil, &mcpHTTPStatusError{StatusCode: response.StatusCode, RetryAfter: delay}
	}
	return response, err
}

func waitRequestBudget(ctx context.Context, delay time.Duration) error {
	if managed, _ := ctx.Value(managedRetriesKey{}).(bool); managed && delay > 5*time.Second {
		return &MCPBudgetWaitError{RetryAfter: delay}
	}
	return sleepWithContext(ctx, delay)
}

func retryAfter(value string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(value, 10, 32); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return 0
}
