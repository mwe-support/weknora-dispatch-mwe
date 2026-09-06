package tencentdocs

import (
	"crypto/sha256"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type credentialBudget struct {
	mu       sync.Mutex
	next     time.Time
	cooldown time.Time
}

// ponytail: shared across all credentials' clients in this single app process;
// use a Redis-backed budget when deploying multiple app replicas.
var credentialBudgets sync.Map

type budgetTransport struct {
	base   http.RoundTripper
	budget *credentialBudget
}

func withCredentialBudget(token string, base http.RoundTripper) http.RoundTripper {
	key := sha256.Sum256([]byte(token))
	v, _ := credentialBudgets.LoadOrStore(key, &credentialBudget{})
	return &budgetTransport{base: base, budget: v.(*credentialBudget)}
}

func (t *budgetTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	for {
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
		if err := sleepWithContext(req.Context(), time.Until(at)); err != nil {
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
	}
	return response, err
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
