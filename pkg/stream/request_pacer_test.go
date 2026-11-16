package stream

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func newTestPacer(clock *fakeClock) *RequestPacer {
	return NewRequestPacer(RequestPacerConfig{
		MinInterval:    30 * time.Second,
		InitialBackoff: 10 * time.Second,
		MaxBackoff:     2 * time.Minute,
		StableWindow:   60 * time.Second,
		Now:            clock.Now,
	})
}

// acquireAfter advances the clock in 1 s steps until TryAcquire succeeds and
// returns the time waited since the last request.
func acquireAfter(t *testing.T, p *RequestPacer, clock *fakeClock) time.Duration {
	t.Helper()
	for waited := time.Duration(0); waited <= 10*time.Minute; waited += time.Second {
		if p.TryAcquire() {
			return waited
		}
		clock.Advance(time.Second)
	}
	t.Fatal("pacer never allowed a request")
	return 0
}

// The gap before each request is max(MinInterval, backoff), and the backoff
// grows x1.5 per request up to MaxBackoff.
func TestRequestPacer_GrowsToMaxBackoff(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	p := newTestPacer(clock)

	require.True(t, p.TryAcquire(), "first request is allowed at once")
	want := []time.Duration{30, 30, 30, 34, 51, 76, 114, 120, 120} // seconds, rounded up to the 1 s step
	for i, w := range want {
		got := acquireAfter(t, p, clock)
		assert.Equal(t, w*time.Second, got, "gap %d", i+1)
		p.ObserveLocal(false)
	}
	assert.Equal(t, 2*time.Minute, p.Backoff())
}

// Healthy periods shorter than StableWindow do not reset the backoff; an
// unbroken StableWindow does.
func TestRequestPacer_ResetsOnlyAfterStableWindow(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1000, 0)}
	p := newTestPacer(clock)
	for i := 0; i < 8; i++ {
		acquireAfter(t, p, clock)
	}
	require.Equal(t, 2*time.Minute, p.Backoff())

	// Flapping: 40 s healthy, 1 s down, repeated.
	for i := 0; i < 5; i++ {
		for s := 0; s < 40; s++ {
			p.ObserveLocal(true)
			clock.Advance(time.Second)
		}
		p.ObserveLocal(false)
	}
	assert.Equal(t, 2*time.Minute, p.Backoff(), "flapping must not reset the backoff")

	for s := 0; s <= 60; s++ {
		p.ObserveLocal(true)
		clock.Advance(time.Second)
	}
	p.ObserveLocal(true)
	assert.Equal(t, time.Duration(0), p.Backoff(), "stable local resets the backoff")
	clock.Advance(2 * time.Minute)
	require.True(t, p.TryAcquire())
	assert.Equal(t, 10*time.Second, p.Backoff(), "after the reset the next backoff is InitialBackoff")
}
