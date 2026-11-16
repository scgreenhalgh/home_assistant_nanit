package stream

import (
	"sync"
	"time"
)

// Default pacing for local stream requests
const (
	defaultMinInterval    = 30 * time.Second // Minimum time between local stream requests
	defaultInitialBackoff = 10 * time.Second // Backoff after the first request
	defaultMaxBackoff     = 2 * time.Minute  // Maximum backoff
	defaultStableWindow   = 60 * time.Second // Local must be healthy this long to reset the backoff
)

// RequestPacerConfig configures a RequestPacer. Zero durations take the defaults.
type RequestPacerConfig struct {
	MinInterval    time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	StableWindow   time.Duration
	Now            func() time.Time // clock; nil means time.Now
}

// RequestPacer rate-limits STARTED requests to the camera. Every request grows
// the backoff (x1.5 up to MaxBackoff); only local being healthy continuously for
// StableWindow resets it. Share one pacer across the pools of one baby so a
// websocket reconnect does not reset the pacing. Safe for concurrent use.
type RequestPacer struct {
	cfg RequestPacerConfig

	mu           sync.Mutex
	lastRequest  time.Time
	backoff      time.Duration
	healthySince time.Time
}

// NewRequestPacer creates a RequestPacer
func NewRequestPacer(cfg RequestPacerConfig) *RequestPacer {
	if cfg.MinInterval == 0 {
		cfg.MinInterval = defaultMinInterval
	}
	if cfg.InitialBackoff == 0 {
		cfg.InitialBackoff = defaultInitialBackoff
	}
	if cfg.MaxBackoff == 0 {
		cfg.MaxBackoff = defaultMaxBackoff
	}
	if cfg.StableWindow == 0 {
		cfg.StableWindow = defaultStableWindow
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &RequestPacer{cfg: cfg}
}

// TryAcquire reports whether a request may be sent now. If so it records the
// request and grows the backoff for the next one.
func (r *RequestPacer) TryAcquire() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	wait := r.cfg.MinInterval
	if r.backoff > wait {
		wait = r.backoff
	}
	now := r.cfg.Now()
	if !r.lastRequest.IsZero() && now.Sub(r.lastRequest) < wait {
		return false
	}
	r.lastRequest = now
	if r.backoff == 0 {
		r.backoff = r.cfg.InitialBackoff
	} else {
		r.backoff = time.Duration(float64(r.backoff) * 1.5)
		if r.backoff > r.cfg.MaxBackoff {
			r.backoff = r.cfg.MaxBackoff
		}
	}
	return true
}

// ObserveLocal records whether local is healthy now. The backoff resets once
// local has been healthy without a break for StableWindow.
func (r *RequestPacer) ObserveLocal(healthy bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !healthy {
		r.healthySince = time.Time{}
		return
	}
	now := r.cfg.Now()
	if r.healthySince.IsZero() {
		r.healthySince = now
	}
	if now.Sub(r.healthySince) >= r.cfg.StableWindow {
		r.backoff = 0
	}
}

// Backoff returns the wait that applies after the last request
func (r *RequestPacer) Backoff() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.backoff
}
