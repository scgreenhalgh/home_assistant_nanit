package stream

import (
	"context"
	"sync"
	"time"

	"github.com/scgreenhalgh/home_assistant_nanit/pkg/baby"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/client"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/rtmpserver"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/utils"
	"github.com/rs/zerolog/log"
)

// Source IDs for hybrid pool
// Local uses rtmpserver.SourceIDLocal (1) to match what the RTMP handler uses
// when the camera publishes directly to the server
const (
	HybridSourceLocal  = 1  // Must match rtmpserver.SourceIDLocal
	HybridSourceRemote = 21 // Remote cloud relay (unique ID for remote source)
)

// Recovery configuration
const (
	minLocalRetryInterval  = 30 * time.Second  // Minimum time between local retry attempts
	initialLocalBackoff    = 10 * time.Second  // Initial backoff for failed local retries
	maxLocalBackoff        = 5 * time.Minute   // Maximum backoff for failed local retries
	healthCheckInterval    = 30 * time.Second  // How often to check stream health
	noStreamRecoveryDelay  = 1 * time.Minute   // How long to wait before attempting recovery when no stream
)

// HybridStreamPool manages a local stream as primary with remote relay as hot standby.
// This provides true redundancy because local and remote use different network paths.
// When local stalls, it switches to remote using keyframe-aligned switching.
type HybridStreamPool struct {
	babyUID      string
	localURL     string // RTMP URL for local stream
	conn         *client.WebsocketConnection
	stateManager *baby.StateManager
	rtmpServer   *rtmpserver.RTMPServer

	// Remote relay configuration
	authToken     string
	tokenProvider TokenProvider    // Provides fresh tokens on reconnect
	onAuthFailed  func(err error)  // Called when auth fails completely
	remoteRelay   RemoteRelay      // Interface, actual type is *RTMPRelay

	// disableRemoteRelay runs the pool local-only: the remote relay is never
	// started and the pool never switches to remote, so the camera is never asked
	// to publish to the Nanit cloud media relay. Immutable after construction.
	disableRemoteRelay bool

	mu              sync.RWMutex
	started         bool
	activeSource    int  // HybridSourceLocal or HybridSourceRemote
	localActive     bool // Whether local publisher is connected
	remoteActive    bool // Whether remote relay is connected
	ctx             context.Context
	cancel          context.CancelFunc

	// Stall detection config
	stallThreshold time.Duration

	// Recovery tracking
	lastLocalStall      time.Time
	localStallCount     int
	lastLocalRetry      time.Time     // Rate limiting for local retries
	localRetryInFlight  bool          // Prevents concurrent requestLocalStream calls
	localRetryBackoff   time.Duration // Current backoff duration for local retries
}

// HybridStreamPoolConfig configuration for HybridStreamPool
type HybridStreamPoolConfig struct {
	BabyUID        string
	LocalURL       string        // RTMP URL for local stream (without slot suffix)
	AuthToken      string        // Initial auth token for remote relay
	TokenProvider  TokenProvider // Optional: provides fresh tokens on reconnect (recommended)
	Conn           *client.WebsocketConnection
	StateManager   *baby.StateManager
	RTMPServer     *rtmpserver.RTMPServer
	StallThreshold time.Duration
	OnAuthFailed   func(err error) // Optional: called when auth fails completely

	// DisableRemoteRelay runs the pool local-only (no cloud relay hot-standby).
	DisableRemoteRelay bool
}

// NewHybridStreamPool creates a new hybrid stream pool
func NewHybridStreamPool(config HybridStreamPoolConfig) *HybridStreamPool {
	if config.StallThreshold == 0 {
		config.StallThreshold = 3 * time.Second // Default 3 second stall threshold
	}

	return &HybridStreamPool{
		babyUID:        config.BabyUID,
		localURL:       config.LocalURL,
		authToken:      config.AuthToken,
		tokenProvider:  config.TokenProvider,
		onAuthFailed:   config.OnAuthFailed,
		conn:           config.Conn,
		stateManager:   config.StateManager,
		rtmpServer:     config.RTMPServer,
		stallThreshold: config.StallThreshold,
		activeSource:   HybridSourceLocal, // Start with local as primary

		disableRemoteRelay: config.DisableRemoteRelay,
	}
}

// RequestLocalStream starts both local and remote streams (local as active, remote as standby)
func (p *HybridStreamPool) RequestLocalStream(ctx context.Context) error {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return nil
	}
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.started = true
	p.activeSource = HybridSourceLocal
	p.mu.Unlock()

	// Publish initial stream source state
	p.stateManager.Update(p.babyUID, *baby.NewState().SetStreamType(baby.StreamType_Local))

	// Configure stall detection on the broadcaster
	p.configureStallDetection()

	// Start remote relay as hot standby (relay.Start returns immediately)
	p.startRemoteRelay()

	// Request local stream (primary)
	if err := p.requestLocalStream(); err != nil {
		if p.disableRemoteRelay {
			// Local-only: no remote to fall back to. Stay on local and let the
			// rate-limited retry + health watchdog recover it.
			log.Warn().Err(err).Str("baby_uid", p.babyUID).Msg("Failed to start local stream (remote relay disabled), will keep retrying local")
			go p.tryRequestLocalStream()
		} else {
			log.Warn().Err(err).Str("baby_uid", p.babyUID).Msg("Failed to start local stream, falling back to remote")

			// Actually switch to remote since local failed
			p.mu.Lock()
			p.activeSource = HybridSourceRemote
			p.mu.Unlock()

			// Update broadcaster to accept remote packets
			if broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID); broadcaster != nil {
				broadcaster.SetActiveSource(HybridSourceRemote)
				log.Info().Str("baby_uid", p.babyUID).Msg("Switched active source to remote after local failure")
			}

			// Publish stream source change to remote
			p.stateManager.Update(p.babyUID, *baby.NewState().SetStreamType(baby.StreamType_Remote))
		}
	}

	// Start health watchdog to monitor and recover streams
	go p.runHealthWatchdog(p.ctx)

	return nil
}

// configureStallDetection sets up the stall callback on the broadcaster
func (p *HybridStreamPool) configureStallDetection() {
	broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID)
	if broadcaster == nil {
		// Create the broadcaster if it doesn't exist
		p.rtmpServer.GetOrCreateBroadcaster(p.babyUID)
		broadcaster = p.rtmpServer.GetBroadcaster(p.babyUID)
	}

	if broadcaster == nil {
		log.Warn().Str("baby_uid", p.babyUID).Msg("No broadcaster found for stall detection")
		return
	}

	broadcaster.SetStallConfig(&rtmpserver.StallConfig{
		Threshold: p.stallThreshold,
		Callback:  p.onStallDetected,
	})

	// Set initial active source to local
	broadcaster.SetActiveSource(HybridSourceLocal)

	log.Info().
		Str("baby_uid", p.babyUID).
		Dur("threshold", p.stallThreshold).
		Msg("Configured stall detection for hybrid stream pool (local primary, remote standby)")
}

// onStallDetected is called when the broadcaster detects a stall on the active source
func (p *HybridStreamPool) onStallDetected(sourceID int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started {
		return
	}

	// Ignore stalls from non-active source
	if sourceID != p.activeSource {
		log.Debug().
			Str("baby_uid", p.babyUID).
			Int("stalled_source", sourceID).
			Int("active_source", p.activeSource).
			Msg("Stall detected on non-active source, ignoring")
		return
	}

	now := time.Now()

	if sourceID == HybridSourceLocal {
		// Local stalled - switch to remote
		p.lastLocalStall = now
		p.localStallCount++

		if p.disableRemoteRelay {
			// Local-only: there is no remote to switch to. Keep the active source
			// on local and retry; recording resumes when the local push recovers.
			log.Warn().
				Str("baby_uid", p.babyUID).
				Int("stall_count", p.localStallCount).
				Msg("Local stream stalled (remote relay disabled), retrying local")
			go p.tryRequestLocalStream()
			return
		}

		log.Warn().
			Str("baby_uid", p.babyUID).
			Int("stall_count", p.localStallCount).
			Bool("remote_active", p.remoteActive).
			Msg("Local stream stalled, attempting switch to remote")

		if p.remoteActive {
			p.activeSource = HybridSourceRemote

			// Use keyframe-aligned switching for graceful transition
			if broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID); broadcaster != nil {
				broadcaster.RequestSourceSwitch(HybridSourceRemote)
			}

			// Publish stream source change
			p.stateManager.Update(p.babyUID, *baby.NewState().SetStreamType(baby.StreamType_Remote))

			log.Info().
				Str("baby_uid", p.babyUID).
				Msg("Requested switch to remote stream (waiting for keyframe)")

			// Try to restart local in background for later recovery (rate-limited)
			go p.tryRequestLocalStream()
		} else {
			log.Error().
				Str("baby_uid", p.babyUID).
				Msg("Local stalled but remote not available - stream may be interrupted")

			// Try to start remote
			go p.startRemoteRelay()
		}
	} else if sourceID == HybridSourceRemote {
		// Remote stalled - switch back to local
		log.Warn().
			Str("baby_uid", p.babyUID).
			Bool("local_active", p.localActive).
			Msg("Remote stream stalled, attempting switch to local")

		if p.localActive {
			p.activeSource = HybridSourceLocal

			// Use keyframe-aligned switching
			if broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID); broadcaster != nil {
				broadcaster.RequestSourceSwitch(HybridSourceLocal)
			}

			// Publish stream source change
			p.stateManager.Update(p.babyUID, *baby.NewState().SetStreamType(baby.StreamType_Local))

			log.Info().
				Str("baby_uid", p.babyUID).
				Msg("Requested switch to local stream (waiting for keyframe)")
		} else {
			log.Error().
				Str("baby_uid", p.babyUID).
				Msg("Remote stalled but local not available - stream may be interrupted")

			// Try to restart local (rate-limited)
			go p.tryRequestLocalStream()
		}
	}
}

// startRemoteRelay starts the remote relay as hot standby
func (p *HybridStreamPool) startRemoteRelay() {
	// Local-only mode: never start the cloud relay. This is the single chokepoint
	// for the remote connection, so leaving p.remoteRelay nil keeps remoteActive
	// false everywhere and the camera is never asked to publish to Nanit's cloud.
	// (disableRemoteRelay is immutable after construction, so reading it lock-free
	// is safe.)
	if p.disableRemoteRelay {
		return
	}

	p.mu.Lock()

	// Check if pool has been stopped
	if !p.started {
		p.mu.Unlock()
		log.Debug().Str("baby_uid", p.babyUID).Msg("Pool stopped, skipping remote relay start")
		return
	}

	if p.remoteRelay != nil {
		p.mu.Unlock()
		return // Already running
	}

	// Get the broadcaster's broadcast function for remote source
	broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID)
	if broadcaster == nil {
		p.mu.Unlock()
		log.Error().Str("baby_uid", p.babyUID).Msg("No broadcaster for remote relay")
		return
	}

	broadcastFunc := broadcaster.GetBroadcastFunc(HybridSourceRemote)
	broadcasterAdapter := NewBroadcasterAdapter(broadcastFunc)

	relay := NewRemoteRelay(RemoteRelayConfig{
		BabyUID:          p.babyUID,
		AuthToken:        p.authToken,
		TokenProvider:    p.tokenProvider,
		PacketReceiver:   broadcasterAdapter,
		RTMPClient:       NewRealRTMPClient(),
		ReconnectEnabled: true,
		MaxReconnectAttempts: 5, // After 5 consecutive failures, notify pool to try local
		OnConnected: func() {
			p.mu.Lock()
			p.remoteActive = true
			p.mu.Unlock()
			log.Info().Str("baby_uid", p.babyUID).Msg("Remote relay connected")
		},
		OnMaxRetriesExceeded: func(lastErr error) {
			p.onRemoteMaxRetriesExceeded(lastErr)
		},
		OnAuthFailed: func(err error) {
			log.Error().
				Err(err).
				Str("baby_uid", p.babyUID).
				Msg("Remote relay authentication failed - manual re-authentication may be required")
			if p.onAuthFailed != nil {
				p.onAuthFailed(err)
			}
		},
	})

	p.remoteRelay = relay
	ctx := p.ctx
	p.mu.Unlock()

	log.Info().Str("baby_uid", p.babyUID).Msg("Starting remote relay as hot standby")

	// Start the relay in background - it handles its own reconnection internally
	// Note: remoteActive is set via OnConnected callback when actually connected
	go func() {
		err := relay.Start(ctx)

		// Only log and mark inactive if it wasn't a normal context cancellation
		if err != nil && ctx.Err() == nil {
			log.Warn().Err(err).Str("baby_uid", p.babyUID).Msg("Remote relay stopped unexpectedly")
		}

		p.mu.Lock()
		p.remoteActive = false
		p.mu.Unlock()
	}()
}

// onRemoteMaxRetriesExceeded is called when remote relay has failed too many times
func (p *HybridStreamPool) onRemoteMaxRetriesExceeded(lastErr error) {
	p.mu.Lock()
	activeSource := p.activeSource
	localActive := p.localActive
	started := p.started
	p.remoteActive = false // Mark remote as unavailable
	p.mu.Unlock()

	if !started {
		return
	}

	log.Warn().
		Err(lastErr).
		Str("baby_uid", p.babyUID).
		Int("active_source", activeSource).
		Bool("local_active", localActive).
		Msg("Remote relay max retries exceeded, attempting fallback to local")

	// If we were using remote as active source, try to switch to local
	if activeSource == HybridSourceRemote {
		if localActive {
			// Local is available, switch to it
			p.mu.Lock()
			p.activeSource = HybridSourceLocal
			p.mu.Unlock()

			if broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID); broadcaster != nil {
				broadcaster.RequestSourceSwitch(HybridSourceLocal)
			}

			p.stateManager.Update(p.babyUID, *baby.NewState().SetStreamType(baby.StreamType_Local))
			log.Info().Str("baby_uid", p.babyUID).Msg("Switched to local after remote max retries")
		} else {
			// Local not active, try to start it (rate-limited)
			log.Info().Str("baby_uid", p.babyUID).Msg("Local not active, requesting local stream")
			go p.tryRequestLocalStream()
		}
	}
}

// requestLocalStream requests the local stream from the camera
func (p *HybridStreamPool) requestLocalStream() error {
	p.mu.RLock()
	if !p.started {
		p.mu.RUnlock()
		return nil
	}
	p.mu.RUnlock()

	log.Info().
		Str("baby_uid", p.babyUID).
		Str("url", p.localURL).
		Msg("Requesting local stream")

	awaitResponse := p.conn.SendRequest(client.RequestType_PUT_STREAMING, &client.Request{
		Streaming: &client.Streaming{
			Id:       client.StreamIdentifier(client.StreamIdentifier_MOBILE).Enum(),
			RtmpUrl:  utils.ConstRefStr(p.localURL),
			Status:   client.Streaming_Status(client.Streaming_STARTED).Enum(),
			Attempts: utils.ConstRefInt32(1),
		},
	})

	_, err := awaitResponse(30 * time.Second)
	if err != nil {
		log.Error().
			Err(err).
			Str("baby_uid", p.babyUID).
			Msg("Failed to request local stream")
		return err
	}

	log.Info().Str("baby_uid", p.babyUID).Msg("Local stream requested successfully")

	// Wait for publisher to connect
	timeout := 15 * time.Second
	err = p.rtmpServer.WaitForPublisher(p.babyUID, timeout)
	if err != nil {
		log.Warn().
			Err(err).
			Str("baby_uid", p.babyUID).
			Msg("Local publisher didn't connect")
		return err
	}

	p.mu.Lock()
	p.localActive = true
	wasOnRemote := p.activeSource == HybridSourceRemote
	p.mu.Unlock()

	log.Info().Str("baby_uid", p.babyUID).Msg("Local stream active")

	// If we were on remote, request switch back to local
	if wasOnRemote {
		if broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID); broadcaster != nil {
			broadcaster.RequestSourceSwitch(HybridSourceLocal)
			log.Info().
				Str("baby_uid", p.babyUID).
				Msg("Local recovered, requested switch back from remote (waiting for keyframe)")
		}

		p.mu.Lock()
		p.activeSource = HybridSourceLocal
		p.mu.Unlock()

		// Publish stream source change back to local
		p.stateManager.Update(p.babyUID, *baby.NewState().SetStreamType(baby.StreamType_Local))
	}

	p.stateManager.Update(p.babyUID, *baby.NewState().SetStreamState(baby.StreamState_Alive))

	return nil
}

// tryRequestLocalStream attempts to request a local stream with rate limiting and backoff
// This prevents spamming the camera/API when local stream is unavailable
func (p *HybridStreamPool) tryRequestLocalStream() {
	p.mu.Lock()

	// Check if pool has been stopped
	if !p.started {
		p.mu.Unlock()
		log.Debug().Str("baby_uid", p.babyUID).Msg("Pool stopped, skipping local retry")
		return
	}

	// Check if another request is already in flight
	if p.localRetryInFlight {
		p.mu.Unlock()
		log.Debug().Str("baby_uid", p.babyUID).Msg("Local retry already in flight, skipping")
		return
	}

	// Check rate limiting
	if time.Since(p.lastLocalRetry) < minLocalRetryInterval {
		p.mu.Unlock()
		log.Debug().
			Str("baby_uid", p.babyUID).
			Dur("time_since_last", time.Since(p.lastLocalRetry)).
			Dur("min_interval", minLocalRetryInterval).
			Msg("Local retry rate limited, skipping")
		return
	}

	// Check backoff
	if p.localRetryBackoff > 0 && time.Since(p.lastLocalRetry) < p.localRetryBackoff {
		p.mu.Unlock()
		log.Debug().
			Str("baby_uid", p.babyUID).
			Dur("backoff", p.localRetryBackoff).
			Msg("Local retry in backoff, skipping")
		return
	}

	p.localRetryInFlight = true
	p.lastLocalRetry = time.Now()
	p.mu.Unlock()

	log.Info().
		Str("baby_uid", p.babyUID).
		Dur("backoff", p.localRetryBackoff).
		Msg("Attempting local stream request")

	err := p.requestLocalStream()

	p.mu.Lock()
	p.localRetryInFlight = false

	if err != nil {
		// Increase backoff on failure
		if p.localRetryBackoff == 0 {
			p.localRetryBackoff = initialLocalBackoff
		} else {
			p.localRetryBackoff = time.Duration(float64(p.localRetryBackoff) * 1.5)
			if p.localRetryBackoff > maxLocalBackoff {
				p.localRetryBackoff = maxLocalBackoff
			}
		}
		log.Warn().
			Err(err).
			Str("baby_uid", p.babyUID).
			Dur("next_backoff", p.localRetryBackoff).
			Msg("Local stream request failed, increased backoff")
	} else {
		// Reset backoff on success
		p.localRetryBackoff = 0
		log.Info().Str("baby_uid", p.babyUID).Msg("Local stream request succeeded, reset backoff")
	}
	p.mu.Unlock()
}

// runHealthWatchdog periodically checks stream health and attempts recovery if needed
func (p *HybridStreamPool) runHealthWatchdog(ctx context.Context) {
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	var noStreamSince time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.mu.RLock()
			localActive := p.localActive
			remoteActive := p.remoteActive
			started := p.started
			p.mu.RUnlock()

			if !started {
				return
			}

			hasActiveStream := localActive || remoteActive

			if hasActiveStream {
				// Reset the "no stream" timer
				noStreamSince = time.Time{}
			} else {
				// No active stream
				if noStreamSince.IsZero() {
					noStreamSince = time.Now()
					log.Warn().
						Str("baby_uid", p.babyUID).
						Msg("Health watchdog: No active stream detected")
				} else if time.Since(noStreamSince) >= noStreamRecoveryDelay {
					// No stream for too long, attempt recovery
					log.Warn().
						Str("baby_uid", p.babyUID).
						Dur("no_stream_duration", time.Since(noStreamSince)).
						Msg("Health watchdog: Attempting stream recovery")

					// Try to request local stream (rate-limited)
					go p.tryRequestLocalStream()

					// Reset timer so we don't spam recovery attempts
					noStreamSince = time.Now()
				}
			}

			// Log health status periodically at debug level
			log.Debug().
				Str("baby_uid", p.babyUID).
				Bool("local_active", localActive).
				Bool("remote_active", remoteActive).
				Msg("Health watchdog status")
		}
	}
}

// WaitForPublisher implements LocalStreamer interface
func (p *HybridStreamPool) WaitForPublisher(ctx context.Context, timeout time.Duration) error {
	return p.rtmpServer.WaitForPublisher(p.babyUID, timeout)
}

// StopLocalStream stops both local and remote streams
func (p *HybridStreamPool) StopLocalStream(ctx context.Context) error {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return nil
	}
	p.started = false
	if p.cancel != nil {
		p.cancel()
	}
	remoteRelay := p.remoteRelay
	p.remoteRelay = nil
	p.mu.Unlock()

	// Stop stall detection
	broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID)
	if broadcaster != nil {
		broadcaster.SetStallConfig(nil)
	}

	// Stop remote relay
	if remoteRelay != nil {
		remoteRelay.Stop()
	}

	// Stop local stream
	p.conn.SendRequest(client.RequestType_PUT_STREAMING, &client.Request{
		Streaming: &client.Streaming{
			Id:       client.StreamIdentifier(client.StreamIdentifier_MOBILE).Enum(),
			RtmpUrl:  utils.ConstRefStr(p.localURL),
			Status:   client.Streaming_Status(client.Streaming_STOPPED).Enum(),
			Attempts: utils.ConstRefInt32(1),
		},
	})

	log.Info().Str("baby_uid", p.babyUID).Msg("Hybrid stream pool stopped")
	return nil
}

// OnPublisherConnected is called when a local publisher connects
func (p *HybridStreamPool) OnPublisherConnected(url string) {
	if url != p.localURL {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.localActive = true
	log.Debug().
		Str("baby_uid", p.babyUID).
		Msg("Local publisher connected")
}

// OnPublisherDisconnected is called when a local publisher disconnects
func (p *HybridStreamPool) OnPublisherDisconnected(url string) {
	if url != p.localURL {
		return
	}

	p.mu.Lock()
	wasActive := p.localActive
	p.localActive = false
	activeSource := p.activeSource
	started := p.started
	p.mu.Unlock()

	log.Info().
		Str("baby_uid", p.babyUID).
		Bool("was_active", wasActive).
		Msg("Local publisher disconnected")

	if !started {
		return
	}

	// If we were using local, switch to remote
	if activeSource == HybridSourceLocal {
		p.mu.Lock()
		if p.remoteActive {
			p.activeSource = HybridSourceRemote
			p.mu.Unlock()

			if broadcaster := p.rtmpServer.GetBroadcaster(p.babyUID); broadcaster != nil {
				broadcaster.RequestSourceSwitch(HybridSourceRemote)
			}

			// Publish stream source change to remote
			p.stateManager.Update(p.babyUID, *baby.NewState().SetStreamType(baby.StreamType_Remote))

			log.Info().
				Str("baby_uid", p.babyUID).
				Msg("Switched to remote after local disconnect")
		} else {
			p.mu.Unlock()
			log.Warn().
				Str("baby_uid", p.babyUID).
				Msg("Local disconnected but remote not available")
		}

		// Try to restart local (rate-limited)
		go p.tryRequestLocalStream()
	}
}

// GetActiveSource returns the currently active source
func (p *HybridStreamPool) GetActiveSource() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.activeSource
}

// IsLocalActive returns whether local stream is connected
func (p *HybridStreamPool) IsLocalActive() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.localActive
}

// IsRemoteActive returns whether remote relay is connected
func (p *HybridStreamPool) IsRemoteActive() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.remoteActive
}
