package stream

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/baby"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/client"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/rtmpserver"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/utils"
)

// Source IDs for hybrid pool
// Local uses rtmpserver.SourceIDLocal (1) to match what the RTMP handler uses
// when the camera publishes directly to the server
const (
	HybridSourceLocal  = 1  // Must match rtmpserver.SourceIDLocal
	HybridSourceRemote = 21 // Remote cloud relay (unique ID for remote source)
)

// Default timings
const (
	defaultStallThreshold   = 3 * time.Second  // Broadcaster stall callback threshold
	defaultHealthWindow     = 5 * time.Second  // A source is healthy if it sent a video packet this recently
	defaultWatchdogInterval = 1 * time.Second  // How often to evaluate stream health
	defaultRequestTimeout   = 30 * time.Second // How long to wait for the reply to STARTED
	defaultPublisherWait    = 15 * time.Second // How long to wait for local packets after STARTED is acknowledged
	defaultPublisherGrace   = 10 * time.Second // No STARTED this long after a local publisher connected
	defaultStopTimeout      = 3 * time.Second  // How long to wait for the reply to STOPPED
)

// StreamRequester sends a request over the camera websocket.
// *client.WebsocketConnection implements it.
type StreamRequester interface {
	SendRequest(client.RequestType, *client.Request) func(time.Duration) (*client.Response, error)
}

// HybridStreamPool manages a local stream as primary with remote relay as hot standby.
// This provides true redundancy because local and remote use different network paths.
//
// Health is derived from the broadcaster: a source is healthy when it sent a video
// packet within HealthWindow. One evaluation function (run by a watchdog ticker,
// the stall callback and the relay's max-retries callback) points the broadcaster
// filter at a healthy source, publishes StreamType/StreamState, and asks the
// camera for the local stream (rate limited) only while local is not healthy.
type HybridStreamPool struct {
	cfg HybridStreamPoolConfig

	evalMu sync.Mutex // serialises evaluate
	sendMu sync.Mutex // orders STARTED and STOPPED sends

	mu           sync.Mutex
	started      bool
	closed       bool
	ctx          context.Context
	cancel       context.CancelFunc
	remoteRelay  RemoteRelay
	stallConfig  *rtmpserver.StallConfig
	unsubscribe  func()
	watchdogDone chan struct{}

	pacer              *RequestPacer
	localRequestActive bool      // one request in flight at a time
	graceFollowUpFor   time.Time // publisher connect time a follow-up evaluation is scheduled for
}

// HybridStreamPoolConfig configuration for HybridStreamPool.
// Zero durations take the defaults above.
type HybridStreamPoolConfig struct {
	BabyUID        string
	LocalURL       string        // RTMP URL for local stream (without slot suffix)
	AuthToken      string        // Initial auth token for remote relay
	TokenProvider  TokenProvider // Optional: provides fresh tokens on reconnect (recommended)
	Conn           StreamRequester
	StateManager   *baby.StateManager
	RTMPServer     *rtmpserver.RTMPServer
	StallThreshold time.Duration
	OnAuthFailed   func(err error) // Optional: called when auth fails completely

	// NewRelay builds the remote relay. Optional: defaults to an RTMPRelay
	// with a real RTMP client.
	NewRelay func(RemoteRelayConfig) RemoteRelay

	// Pacer rate-limits STARTED requests. Optional: share one across the pools
	// of a baby so pacing survives websocket reconnects. If nil, the pool makes
	// its own from MinRetryInterval, InitialBackoff, MaxBackoff and StableWindow.
	Pacer *RequestPacer

	HealthWindow     time.Duration
	WatchdogInterval time.Duration
	MinRetryInterval time.Duration
	InitialBackoff   time.Duration
	MaxBackoff       time.Duration
	StableWindow     time.Duration
	PublisherGrace   time.Duration
	RequestTimeout   time.Duration
	PublisherWait    time.Duration
	StopTimeout      time.Duration
}

// NewHybridStreamPool creates a new hybrid stream pool
func NewHybridStreamPool(config HybridStreamPoolConfig) *HybridStreamPool {
	setDefault := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	setDefault(&config.StallThreshold, defaultStallThreshold)
	setDefault(&config.HealthWindow, defaultHealthWindow)
	setDefault(&config.WatchdogInterval, defaultWatchdogInterval)
	setDefault(&config.RequestTimeout, defaultRequestTimeout)
	setDefault(&config.PublisherWait, defaultPublisherWait)
	setDefault(&config.PublisherGrace, defaultPublisherGrace)
	setDefault(&config.StopTimeout, defaultStopTimeout)
	if config.NewRelay == nil {
		config.NewRelay = func(c RemoteRelayConfig) RemoteRelay {
			c.RTMPClient = NewRealRTMPClient()
			return NewRemoteRelay(c)
		}
	}

	pacer := config.Pacer
	if pacer == nil {
		pacer = NewRequestPacer(RequestPacerConfig{
			MinInterval:    config.MinRetryInterval,
			InitialBackoff: config.InitialBackoff,
			MaxBackoff:     config.MaxBackoff,
			StableWindow:   config.StableWindow,
		})
	}

	return &HybridStreamPool{
		cfg:          config,
		pacer:        pacer,
		watchdogDone: make(chan struct{}),
	}
}

// RequestLocalStream starts the pool: stall detection, the remote standby, and
// the health watchdog. It returns at once; the camera is asked for the local
// stream only if local packets are not already flowing.
func (p *HybridStreamPool) RequestLocalStream(ctx context.Context) error {
	p.mu.Lock()
	if p.started || p.closed {
		p.mu.Unlock()
		return nil
	}
	p.started = true
	// The pool is the only writer of StreamState until shutdown releases it.
	p.cfg.RTMPServer.ClaimStreamState(p.cfg.BabyUID)
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.stallConfig = &rtmpserver.StallConfig{
		Threshold: p.cfg.StallThreshold,
		Callback:  p.onStallDetected,
	}
	stallConfig := p.stallConfig
	// Repair StreamState at once when someone else (the RTMP handler) writes it.
	// The state manager calls subscribers on their own goroutines, without its
	// locks held, and only for updates that change something.
	p.unsubscribe = p.cfg.StateManager.Subscribe(func(babyUID string, state baby.State) {
		if babyUID == p.cfg.BabyUID && state.StreamState != nil {
			p.evaluate()
		}
	})
	go p.runHealthWatchdog(p.ctx)
	p.mu.Unlock()

	p.broadcaster().SetStallConfig(stallConfig)
	p.startRemoteRelay()
	p.evaluate()
	return nil
}

func (p *HybridStreamPool) broadcaster() *rtmpserver.PersistentBroadcaster {
	if b := p.cfg.RTMPServer.GetBroadcaster(p.cfg.BabyUID); b != nil {
		return b
	}
	p.cfg.RTMPServer.GetOrCreateBroadcaster(p.cfg.BabyUID)
	return p.cfg.RTMPServer.GetBroadcaster(p.cfg.BabyUID)
}

// startRemoteRelay starts the remote relay as hot standby
func (p *HybridStreamPool) startRemoteRelay() {
	p.mu.Lock()
	if p.closed || p.remoteRelay != nil {
		p.mu.Unlock()
		return
	}

	relay := p.cfg.NewRelay(RemoteRelayConfig{
		BabyUID:              p.cfg.BabyUID,
		AuthToken:            p.cfg.AuthToken,
		TokenProvider:        p.cfg.TokenProvider,
		PacketReceiver:       NewBroadcasterAdapter(p.broadcaster().GetBroadcastFunc(HybridSourceRemote)),
		ReconnectEnabled:     true,
		MaxReconnectAttempts: 5,
		OnConnected: func() {
			log.Info().Str("baby_uid", p.cfg.BabyUID).Msg("Remote relay connected")
		},
		OnMaxRetriesExceeded: func(lastErr error) {
			log.Warn().Err(lastErr).Str("baby_uid", p.cfg.BabyUID).Msg("Remote relay max retries exceeded")
			go p.evaluate()
		},
		OnAuthFailed: func(err error) {
			log.Error().
				Err(err).
				Str("baby_uid", p.cfg.BabyUID).
				Msg("Remote relay authentication failed - manual re-authentication may be required")
			if p.cfg.OnAuthFailed != nil {
				p.cfg.OnAuthFailed(err)
			}
		},
	})
	p.remoteRelay = relay
	ctx := p.ctx
	p.mu.Unlock()

	log.Info().Str("baby_uid", p.cfg.BabyUID).Msg("Starting remote relay as hot standby")

	// Start makes the first connection attempt before it returns, then keeps
	// relaying and reconnecting in the background until Stop.
	go func() {
		if err := relay.Start(ctx); err != nil && ctx.Err() == nil {
			log.Warn().Err(err).Str("baby_uid", p.cfg.BabyUID).Msg("Remote relay failed to start")
		}
	}()
}

// onStallDetected is called by the broadcaster when the forwarded source stalls
func (p *HybridStreamPool) onStallDetected(sourceID int) {
	p.evaluate()

	// The stall threshold can be shorter than the health window. Evaluate again
	// when the stalled source's last packet falls out of the window.
	if age, seen := p.broadcaster().SourceAge(sourceID); seen && age < p.cfg.HealthWindow {
		time.AfterFunc(p.cfg.HealthWindow-age, p.evaluate)
	}
}

func (p *HybridStreamPool) sourceHealthy(b *rtmpserver.PersistentBroadcaster, sourceID int) bool {
	age, seen := b.SourceAge(sourceID)
	return seen && age < p.cfg.HealthWindow
}

// evaluate is the single place that decides the filter, the published state,
// and whether to ask the camera for the local stream.
func (p *HybridStreamPool) evaluate() {
	p.evalMu.Lock()
	defer p.evalMu.Unlock()

	p.mu.Lock()
	running := p.started && !p.closed
	p.mu.Unlock()
	if !running {
		return
	}

	b := p.broadcaster()
	localOK := p.sourceHealthy(b, HybridSourceLocal)
	remoteOK := p.sourceHealthy(b, HybridSourceRemote)
	p.pacer.ObserveLocal(localOK)

	switch {
	case localOK:
		p.pointFilter(b, HybridSourceLocal, false)
		p.publish(baby.StreamType_Local, baby.StreamState_Alive)
	case remoteOK:
		p.pointFilter(b, HybridSourceRemote, false)
		p.publish(baby.StreamType_Remote, baby.StreamState_Alive)
		p.tryRequestLocalStream()
	default:
		// Nothing flows: forward local the moment it arrives.
		p.pointFilter(b, HybridSourceLocal, true)
		p.publish(baby.StreamType_Local, baby.StreamState_Unhealthy)
		p.tryRequestLocalStream()
	}
}

// pointFilter makes the broadcaster forward target, or switch to it at the next
// keyframe when immediate is false. It writes only when something must change,
// because the setters reset keyframe tracking.
func (p *HybridStreamPool) pointFilter(b *rtmpserver.PersistentBroadcaster, target int, immediate bool) {
	active := b.ActiveSourceID()
	pending := b.PendingSourceID()

	if active == target {
		if pending != 0 {
			b.RequestSourceSwitch(target) // cancels the pending switch away from target
			log.Info().Str("baby_uid", p.cfg.BabyUID).Int("source_id", target).Msg("Cancelled pending source switch")
		}
		return
	}
	if immediate {
		b.SetActiveSource(target)
		return
	}
	if pending == target {
		return
	}
	b.RequestSourceSwitch(target)
	log.Info().Str("baby_uid", p.cfg.BabyUID).Int("from", active).Int("to", target).Msg("Requested source switch (waiting for keyframe)")
}

// publish reconciles StreamType and StreamState with the state manager, so a
// value written elsewhere is corrected on the next evaluation.
func (p *HybridStreamPool) publish(streamType baby.StreamType, streamState baby.StreamState) {
	current := p.cfg.StateManager.GetBabyState(p.cfg.BabyUID)
	update := baby.NewState()
	changed := false
	if current == nil || current.GetStreamType() != streamType {
		update.SetStreamType(streamType)
		changed = true
	}
	if current == nil || current.GetStreamState() != streamState {
		update.SetStreamState(streamState)
		changed = true
	}
	if changed {
		p.cfg.StateManager.Update(p.cfg.BabyUID, *update)
	}
}

// tryRequestLocalStream starts a local stream request unless one is in flight
// or the pacer says it is too soon.
func (p *HybridStreamPool) tryRequestLocalStream() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed || p.localRequestActive {
		return
	}
	// A publisher that just connected is about to send video: a STARTED now
	// could open a second push. Evaluate again when the grace ends.
	if at := p.cfg.RTMPServer.LocalPublisherConnectedAt(p.cfg.BabyUID); !at.IsZero() {
		if remaining := p.cfg.PublisherGrace - time.Since(at); remaining > 0 {
			if !p.graceFollowUpFor.Equal(at) {
				p.graceFollowUpFor = at
				time.AfterFunc(remaining, p.evaluate)
			}
			return
		}
	}
	if !p.pacer.TryAcquire() {
		return
	}
	p.localRequestActive = true
	go p.requestLocalStream(p.ctx)
}

// requestLocalStream sends STARTED and waits for local video packets. The pool
// lock is not held while sending or waiting. The outcome never changes the
// filter or the backoff here: evaluate and the pacer own those.
func (p *HybridStreamPool) requestLocalStream(ctx context.Context) {
	arrived := false
	defer func() {
		p.mu.Lock()
		p.localRequestActive = false
		p.mu.Unlock()
		if arrived {
			p.evaluate() // no-op once the pool is closed
		}
	}()

	// sendMu orders this send against StopLocalStream's STOPPED, which marks
	// the pool closed before taking it: no STARTED can follow the STOPPED.
	p.sendMu.Lock()
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		p.sendMu.Unlock()
		return
	}
	awaitResponse := p.cfg.Conn.SendRequest(client.RequestType_PUT_STREAMING, &client.Request{
		Streaming: &client.Streaming{
			Id:       client.StreamIdentifier(client.StreamIdentifier_MOBILE).Enum(),
			RtmpUrl:  utils.ConstRefStr(p.cfg.LocalURL),
			Status:   client.Streaming_Status(client.Streaming_STARTED).Enum(),
			Attempts: utils.ConstRefInt32(1),
		},
	})
	p.sendMu.Unlock()
	log.Info().Str("baby_uid", p.cfg.BabyUID).Str("url", p.cfg.LocalURL).Dur("next_backoff", p.pacer.Backoff()).Msg("Requested local stream")

	if _, err := awaitResponse(p.cfg.RequestTimeout); err != nil {
		if IsMaxConnectionsError(err) {
			log.Warn().Err(err).Str("baby_uid", p.cfg.BabyUID).Msg("Camera refused local stream (too many connections)")
		} else {
			log.Warn().Err(err).Str("baby_uid", p.cfg.BabyUID).Msg("Local stream request failed")
		}
		return
	}

	// Acknowledged: wait for video packets, not for a registered publisher.
	b := p.broadcaster()
	deadline := time.NewTimer(p.cfg.PublisherWait)
	defer deadline.Stop()
	poll := time.NewTicker(p.cfg.PublisherWait / 15)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			log.Warn().Str("baby_uid", p.cfg.BabyUID).Dur("waited", p.cfg.PublisherWait).Msg("Local stream acknowledged but no packets arrived")
			return
		case <-poll.C:
			if p.sourceHealthy(b, HybridSourceLocal) {
				log.Info().Str("baby_uid", p.cfg.BabyUID).Msg("Local stream packets arriving")
				arrived = true
				return
			}
		}
	}
}

// runHealthWatchdog evaluates stream health periodically
func (p *HybridStreamPool) runHealthWatchdog(ctx context.Context) {
	defer close(p.watchdogDone)
	ticker := time.NewTicker(p.cfg.WatchdogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.evaluate()
		}
	}
}

// shutdown stops the watchdog, state subscription, stall detection and relay,
// then puts the filter on local so the camera's packets pass while no pool
// exists, and publishes the final state. It reports whether this call stopped
// a running pool. An in-flight STARTED is not waited for.
func (p *HybridStreamPool) shutdown(stopping bool) bool {
	p.mu.Lock()
	wasRunning := p.started && !p.closed
	p.closed = true
	if !wasRunning {
		p.mu.Unlock()
		return false
	}
	p.cancel()
	remoteRelay := p.remoteRelay
	p.remoteRelay = nil
	stallConfig := p.stallConfig
	unsubscribe := p.unsubscribe
	p.mu.Unlock()

	unsubscribe()
	b := p.broadcaster()
	// Only removes our own callback; a newer pool may already own the broadcaster.
	b.ClearStallConfig(stallConfig)
	if remoteRelay != nil {
		remoteRelay.Stop()
	}
	<-p.watchdogDone

	// evalMu waits out an evaluation that started before closed was set.
	p.evalMu.Lock()
	defer p.evalMu.Unlock()
	p.pointFilter(b, HybridSourceLocal, true)
	// Hand StreamState back to the RTMP handler with its own rule: it writes only
	// when a publisher connects or leaves, so a connected publisher means Alive.
	p.cfg.RTMPServer.ReleaseStreamState(p.cfg.BabyUID, func(publisherConnected bool) {
		switch {
		case stopping:
			p.publish(baby.StreamType_None, baby.StreamState_Unhealthy)
		case publisherConnected:
			p.publish(baby.StreamType_Local, baby.StreamState_Alive)
		default:
			p.publish(baby.StreamType_Local, baby.StreamState_Unhealthy)
		}
	})
	return true
}

// Close stops the pool without telling the camera. Use it when the websocket is
// gone or is being replaced: STOPPED would end a camera push the next pool keeps.
func (p *HybridStreamPool) Close() {
	if p.shutdown(false) {
		log.Info().Str("baby_uid", p.cfg.BabyUID).Msg("Hybrid stream pool closed")
	}
}

// StopLocalStream stops the pool and tells the camera to stop pushing to our URL,
// waiting for the reply up to StopTimeout or until ctx is cancelled. It is idempotent.
func (p *HybridStreamPool) StopLocalStream(ctx context.Context) error {
	if !p.shutdown(true) {
		return nil
	}

	// The pool is closed now; waiting for sendMu lets a STARTED already being
	// sent go out first, so STOPPED is the last word.
	p.sendMu.Lock()
	awaitResponse := p.cfg.Conn.SendRequest(client.RequestType_PUT_STREAMING, &client.Request{
		Streaming: &client.Streaming{
			Id:       client.StreamIdentifier(client.StreamIdentifier_MOBILE).Enum(),
			RtmpUrl:  utils.ConstRefStr(p.cfg.LocalURL),
			Status:   client.Streaming_Status(client.Streaming_STOPPED).Enum(),
			Attempts: utils.ConstRefInt32(1),
		},
	})
	p.sendMu.Unlock()
	replied := make(chan error, 1)
	go func() {
		_, err := awaitResponse(p.cfg.StopTimeout)
		replied <- err
	}()
	select {
	case err := <-replied:
		if err != nil {
			log.Warn().Err(err).Str("baby_uid", p.cfg.BabyUID).Msg("No confirmation for local stream stop")
		}
	case <-ctx.Done():
		log.Warn().Str("baby_uid", p.cfg.BabyUID).Msg("Stopped waiting for local stream stop confirmation")
	}

	log.Info().Str("baby_uid", p.cfg.BabyUID).Msg("Hybrid stream pool stopped")
	return nil
}
