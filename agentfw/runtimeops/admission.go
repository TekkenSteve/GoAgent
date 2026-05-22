package runtimeops

import (
	"errors"
	"maps"
	"sync"
)

// ErrAdmissionLimitExceeded is returned when account run limits are reached.
var ErrAdmissionLimitExceeded = errors.New("admission limit exceeded")

// AdmissionController manages concurrent run slots by account.
type AdmissionController struct {
	mu     sync.Mutex
	limits map[string]int
	active map[string]int
}

// NewAdmissionController creates a slot controller with per-account limits.
func NewAdmissionController(limits map[string]int) *AdmissionController {
	limitsCopy := make(map[string]int, len(limits))
	maps.Copy(limitsCopy, limits)

	return &AdmissionController{
		limits: limitsCopy,
		active: map[string]int{},
	}
}

// Acquire reserves a slot for an account.
func (c *AdmissionController) Acquire(accountID string, bypass bool) error {
	if bypass {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	limit := c.limits[accountID]
	if limit <= 0 {
		limit = 1
	}

	if c.active[accountID] >= limit {
		return ErrAdmissionLimitExceeded
	}

	c.active[accountID]++

	return nil
}

// Release frees a slot when run reaches terminal state.
func (c *AdmissionController) Release(accountID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.active[accountID] <= 0 {
		return
	}

	c.active[accountID]--
}

// Active returns active slot count.
func (c *AdmissionController) Active(accountID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.active[accountID]
}
