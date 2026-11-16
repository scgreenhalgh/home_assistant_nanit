package rtmpserver

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/notedit/rtmp/av"
	"github.com/rs/zerolog/log"
)

// dropLogInterval is the minimum time between "packets dropped" warnings
const dropLogInterval = 10 * time.Second

// dropLogEpoch is the base for the warning rate limit. time.Since uses the
// monotonic clock, so a wall-clock step cannot suppress the warning.
var dropLogEpoch = time.Now()

// StallCallback is called when a stream stall is detected
// sourceID indicates which source stalled
type StallCallback func(sourceID int)

// StallConfig configures stall detection behavior
type StallConfig struct {
	// Threshold is how long without packets before triggering stall callback
	Threshold time.Duration
	// Callback is called when stall is detected (only once per stall)
	Callback StallCallback
}

// PersistentBroadcaster is a broadcaster that persists across source switches.
// It handles timestamp remapping to ensure stream continuity when switching
// between local (camera) and remote (cloud) sources.
//
// Key features:
// - Subscribers are never disconnected during source switches
// - Timestamps are remapped to be monotonically increasing
// - Header packets are stored and sent to new subscribers
// - Headers are replaced when source changes
// - Stall detection with callback for hot standby switching
// - Active source filtering - only forwards packets from active source
type PersistentBroadcaster struct {
	babyUID string

	mu           sync.RWMutex
	subscribers  map[*Subscription]struct{}
	headerPkts   []av.Packet
	closed       bool

	// Timestamp remapping state
	tsMu            sync.Mutex
	lastSourceID    int
	lastTimestamp   time.Duration
	timestampOffset time.Duration
	initialized     bool

	// Active source filtering (for hot standby)
	activeSourceID    int  // Only forward packets from this source (0 = accept all)
	activeSourceSet   bool // Whether active source filtering is enabled

	// Keyframe-aligned switching
	pendingSourceID      int  // Source we want to switch to (waiting for keyframe)
	pendingSourceSwitch  bool // Whether we're waiting for a keyframe from pendingSourceID
	keyframeReceived     bool // Whether we've received a keyframe from current active source
	pendingHeadersCleared bool // Whether we've cleared headers for the pending source switch

	// Diagnostic tracking
	lastPacketWallTime time.Time
	droppedPackets     atomic.Int64 // atomic so the distribute section never takes tsMu while holding mu
	droppedSinceLog    atomic.Int64 // drops not yet reported by the rate-limited warning
	lastDropLogNanos   atomic.Int64 // time of the last drop warning, see dropLogEpoch (0 = never)

	// Stall detection
	stallMu          sync.Mutex     // Serializes stall checker start/stop operations
	stallConfig      *StallConfig
	stallDetected    bool // true if we've already called callback for current stall
	stallCheckStop   chan struct{}
	stallCheckWg     sync.WaitGroup
}

// Subscription represents a subscriber to the broadcaster
type Subscription struct {
	pktChan     chan av.Packet
	broadcaster *PersistentBroadcaster
	closed      bool
	closeMu     sync.Mutex
}

// NewPersistentBroadcaster creates a new persistent broadcaster for the given baby
func NewPersistentBroadcaster(babyUID string) *PersistentBroadcaster {
	return &PersistentBroadcaster{
		babyUID:     babyUID,
		subscribers: make(map[*Subscription]struct{}),
		headerPkts:  make([]av.Packet, 0),
	}
}

// BabyUID returns the baby UID this broadcaster is for
func (pb *PersistentBroadcaster) BabyUID() string {
	return pb.babyUID
}

// SetActiveSource sets which source ID should be forwarded to subscribers.
// Only packets from this source will be broadcast. Call with sourceID=0 to disable filtering.
// This switches immediately without waiting for a keyframe (use RequestSourceSwitch for graceful switching).
func (pb *PersistentBroadcaster) SetActiveSource(sourceID int) {
	pb.tsMu.Lock()
	defer pb.tsMu.Unlock()

	if sourceID == 0 {
		pb.activeSourceSet = false
		pb.pendingSourceSwitch = false
		log.Debug().Str("baby_uid", pb.babyUID).Msg("Active source filtering disabled")
	} else {
		pb.activeSourceID = sourceID
		pb.activeSourceSet = true
		pb.keyframeReceived = false // Reset - need keyframe from new source
		pb.pendingSourceSwitch = false
		log.Info().Str("baby_uid", pb.babyUID).Int("source_id", sourceID).Msg("Active source set (immediate)")
	}
}

// RequestSourceSwitch requests a switch to a new source, but waits for a keyframe
// from that source before actually switching. This ensures the decoder gets a clean
// starting point and prevents video artifacts.
// Returns immediately - the actual switch happens when keyframe is received.
func (pb *PersistentBroadcaster) RequestSourceSwitch(sourceID int) {
	pb.tsMu.Lock()
	defer pb.tsMu.Unlock()

	if sourceID == 0 {
		pb.activeSourceSet = false
		pb.pendingSourceSwitch = false
		log.Debug().Str("baby_uid", pb.babyUID).Msg("Active source filtering disabled")
		return
	}

	// If already on this source, nothing to do
	if pb.activeSourceSet && pb.activeSourceID == sourceID {
		log.Debug().Str("baby_uid", pb.babyUID).Int("source_id", sourceID).Msg("Already on requested source")
		return
	}

	// If no active source yet, set immediately (first source)
	if !pb.activeSourceSet {
		pb.activeSourceID = sourceID
		pb.activeSourceSet = true
		pb.keyframeReceived = false
		log.Info().Str("baby_uid", pb.babyUID).Int("source_id", sourceID).Msg("Initial active source set")
		return
	}

	// Request switch - will complete when keyframe received from new source
	pb.pendingSourceID = sourceID
	pb.pendingSourceSwitch = true
	pb.pendingHeadersCleared = false // Reset for new pending switch
	log.Info().
		Str("baby_uid", pb.babyUID).
		Int("current_source", pb.activeSourceID).
		Int("pending_source", sourceID).
		Msg("Source switch requested, waiting for keyframe")
}

// SetStallConfig configures stall detection. Call this before streaming starts.
// If config is nil, stall detection is disabled.
func (pb *PersistentBroadcaster) SetStallConfig(config *StallConfig) {
	// Serialize all stall checker start/stop operations
	pb.stallMu.Lock()
	defer pb.stallMu.Unlock()

	// Stop existing stall checker if running
	if pb.stallCheckStop != nil {
		close(pb.stallCheckStop)
		pb.stallCheckStop = nil
		pb.stallCheckWg.Wait()
	}

	pb.tsMu.Lock()
	pb.stallConfig = config
	pb.stallDetected = false
	pb.tsMu.Unlock()

	// Start stall checker if configured
	if config != nil && config.Callback != nil && config.Threshold > 0 {
		pb.stallCheckStop = make(chan struct{})
		pb.stallCheckWg.Add(1)
		go pb.stallCheckerLoop(pb.stallCheckStop) // Pass channel to avoid race
	}
}

// stallCheckerLoop periodically checks for stalls
// stopChan is passed as parameter to avoid race conditions on the struct field
func (pb *PersistentBroadcaster) stallCheckerLoop(stopChan <-chan struct{}) {
	defer pb.stallCheckWg.Done()

	// Check every 1 second
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			pb.checkForStall()
		}
	}
}

// checkForStall checks if we've stalled and calls callback if needed
func (pb *PersistentBroadcaster) checkForStall() {
	pb.tsMu.Lock()
	defer pb.tsMu.Unlock()

	if pb.stallConfig == nil || pb.stallConfig.Callback == nil {
		return
	}

	// Need to have received at least one packet
	if pb.lastPacketWallTime.IsZero() {
		return
	}

	gap := time.Since(pb.lastPacketWallTime)

	if gap >= pb.stallConfig.Threshold {
		// Stall detected!
		if !pb.stallDetected {
			pb.stallDetected = true
			sourceID := pb.lastSourceID

			log.Warn().
				Str("baby_uid", pb.babyUID).
				Dur("gap", gap).
				Int("source_id", sourceID).
				Msg("Stream stall detected, triggering callback")

			// Call callback outside of lock
			go pb.stallConfig.Callback(sourceID)
		}
	}
}

// Subscribe creates a new subscription to receive packets
func (pb *PersistentBroadcaster) Subscribe() *Subscription {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.closed {
		return nil
	}

	sub := &Subscription{
		pktChan:     make(chan av.Packet, 500), // Larger buffer to handle bursts after camera stalls
		broadcaster: pb,
	}

	pb.subscribers[sub] = struct{}{}

	// Send stored header packets to new subscriber
	for _, hdr := range pb.headerPkts {
		select {
		case sub.pktChan <- hdr:
		default:
			// Buffer full, skip (shouldn't happen for headers)
		}
	}

	return sub
}

// Packets returns the channel to receive packets from
func (s *Subscription) Packets() <-chan av.Packet {
	return s.pktChan
}

// Unsubscribe removes this subscription from the broadcaster
func (s *Subscription) Unsubscribe() {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return
	}
	s.closed = true
	close(s.pktChan) // Close inside lock to prevent double close race
	s.closeMu.Unlock()

	s.broadcaster.removeSubscriber(s)
}

func (pb *PersistentBroadcaster) removeSubscriber(sub *Subscription) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	delete(pb.subscribers, sub)
}

// Broadcast sends a packet to all subscribers with timestamp remapping
func (pb *PersistentBroadcaster) Broadcast(pkt av.Packet, sourceID int) {
	// Handle header packets (type > 2)
	if pkt.Type > 2 {
		pb.handleHeaderPacket(pkt, sourceID)
		return
	}

	// Check source filtering and handle keyframe-aligned switching
	pb.tsMu.Lock()

	// Check for pending source switch - if this is a keyframe from the pending source, complete the switch
	if pb.pendingSourceSwitch && sourceID == pb.pendingSourceID && pkt.IsKeyFrame {
		log.Info().
			Str("baby_uid", pb.babyUID).
			Int("old_source", pb.activeSourceID).
			Int("new_source", sourceID).
			Msg("Keyframe received from pending source, completing switch")

		pb.activeSourceID = sourceID
		pb.pendingSourceSwitch = false
		pb.pendingSourceID = 0
		pb.keyframeReceived = true
		pb.pendingHeadersCleared = false // Reset for future switches

		// Headers from the new source were already collected via handleHeaderPacket
		// Send them to all existing subscribers for decoder re-initialization
		pb.mu.RLock()
		headers := make([]av.Packet, len(pb.headerPkts))
		copy(headers, pb.headerPkts)
		pb.mu.RUnlock()

		pb.mu.RLock()
		for sub := range pb.subscribers {
			sub.closeMu.Lock()
			if !sub.closed {
				for _, hdr := range headers {
					select {
					case sub.pktChan <- hdr:
					default:
						// Buffer full, skip header (shouldn't happen)
					}
				}
			}
			sub.closeMu.Unlock()
		}
		pb.mu.RUnlock()
	}

	// Check if this source is active (for hot standby filtering)
	if pb.activeSourceSet && sourceID != pb.activeSourceID {
		// Not the active source - don't forward but don't update wall time either
		// (stall detection should only track the active source)
		pb.tsMu.Unlock()
		return
	}

	// Track if we've received a keyframe from active source
	if pkt.IsKeyFrame && !pb.keyframeReceived {
		pb.keyframeReceived = true
		log.Debug().
			Str("baby_uid", pb.babyUID).
			Int("source_id", sourceID).
			Msg("First keyframe received from active source")
	}

	// Don't forward until we have a keyframe (prevents decoder errors)
	// Only enforce this when source filtering is active (hot standby mode)
	if pb.activeSourceSet && !pb.keyframeReceived {
		pb.tsMu.Unlock()
		return
	}

	// Diagnostic: check for long gaps (camera stalls)
	now := time.Now()
	if !pb.lastPacketWallTime.IsZero() {
		gap := now.Sub(pb.lastPacketWallTime)
		if gap > 2*time.Second {
			log.Warn().
				Str("baby_uid", pb.babyUID).
				Dur("gap", gap).
				Bool("is_keyframe", pkt.IsKeyFrame).
				Msg("Long gap between packets detected (camera stall?)")
		}
	}
	pb.lastPacketWallTime = now
	// Reset stall detection - we're receiving packets again
	if pb.stallDetected {
		log.Info().
			Str("baby_uid", pb.babyUID).
			Int("source_id", sourceID).
			Msg("Stream resumed after stall")
		pb.stallDetected = false
	}
	pb.tsMu.Unlock()

	// Remap timestamp for continuity
	pkt = pb.remapTimestamp(pkt, sourceID)

	// Distribute to all subscribers
	pb.mu.RLock()
	defer pb.mu.RUnlock()

	droppedThisRound := 0
	for sub := range pb.subscribers {
		sub.closeMu.Lock()
		if !sub.closed {
			select {
			case sub.pktChan <- pkt:
			default:
				// Buffer full, drop packet (live stream, can't wait)
				droppedThisRound++
			}
		}
		sub.closeMu.Unlock()
	}

	if droppedThisRound > 0 {
		total := pb.droppedPackets.Add(int64(droppedThisRound))
		pb.droppedSinceLog.Add(int64(droppedThisRound))

		// Rate-limit the warning: at most one line per dropLogInterval
		// The offset makes the first drop log at once (last is 0)
		now := int64(time.Since(dropLogEpoch) + dropLogInterval)
		last := pb.lastDropLogNanos.Load()
		if now-last >= int64(dropLogInterval) && pb.lastDropLogNanos.CompareAndSwap(last, now) {
			log.Warn().
				Str("baby_uid", pb.babyUID).
				Int64("dropped_since_last_log", pb.droppedSinceLog.Swap(0)).
				Int64("total_dropped", total).
				Bool("is_keyframe", pkt.IsKeyFrame).
				Msg("Packet(s) dropped due to full subscriber buffer")
		}
	}
}

func (pb *PersistentBroadcaster) handleHeaderPacket(pkt av.Packet, sourceID int) {
	pb.tsMu.Lock()

	// Accept headers from active source OR pending source (for keyframe-aligned switching)
	acceptHeader := false
	isPendingSource := false
	if !pb.activeSourceSet {
		acceptHeader = true // No filtering, accept all
	} else if sourceID == pb.activeSourceID {
		acceptHeader = true // From active source
	} else if pb.pendingSourceSwitch && sourceID == pb.pendingSourceID {
		acceptHeader = true    // From pending source (building up headers for switch)
		isPendingSource = true // Mark this so we don't update lastSourceID yet
	}

	if !acceptHeader {
		pb.tsMu.Unlock()
		return
	}

	// Only clear headers when switching to pending source (new source's headers will replace old)
	// Don't update lastSourceID for pending source - that happens when the switch completes
	// (timestamp remapping needs lastSourceID to stay on the active source until switch)
	if isPendingSource {
		// Check if we need to clear (protected by tsMu)
		needsClear := !pb.pendingHeadersCleared
		if needsClear {
			pb.pendingHeadersCleared = true
		}

		pb.mu.Lock()
		if needsClear {
			log.Debug().Str("baby_uid", pb.babyUID).Int("source_id", sourceID).Msg("Cleared headers for pending source")
			pb.headerPkts = make([]av.Packet, 0)
		}
		pb.headerPkts = append(pb.headerPkts, pkt)
		pb.mu.Unlock()
		pb.tsMu.Unlock()
		return
	}

	sourceChanged := pb.initialized && sourceID != pb.lastSourceID
	if sourceChanged {
		// Source changed - clear old headers
		pb.mu.Lock()
		pb.headerPkts = make([]av.Packet, 0)
		pb.mu.Unlock()
		log.Debug().Str("baby_uid", pb.babyUID).Int("source_id", sourceID).Msg("Cleared headers for source switch")
	}
	pb.lastSourceID = sourceID
	pb.tsMu.Unlock()

	// Store header packet
	pb.mu.Lock()
	pb.headerPkts = append(pb.headerPkts, pkt)
	pb.mu.Unlock()
}

func (pb *PersistentBroadcaster) remapTimestamp(pkt av.Packet, sourceID int) av.Packet {
	pb.tsMu.Lock()
	defer pb.tsMu.Unlock()

	if !pb.initialized {
		// First packet ever
		pb.initialized = true
		pb.lastSourceID = sourceID
		pb.lastTimestamp = pkt.Time
		pb.timestampOffset = 0
		return pkt
	}

	if sourceID != pb.lastSourceID {
		// Source changed - calculate offset to maintain continuity
		// New timestamp = old timestamp + offset
		// We want: pkt.Time + newOffset = lastTimestamp + frameInterval
		// So: newOffset = lastTimestamp + frameInterval - pkt.Time
		frameInterval := 33 * time.Millisecond // ~30fps
		pb.timestampOffset = pb.lastTimestamp + frameInterval - pkt.Time
		pb.lastSourceID = sourceID
	}

	// Apply offset
	remappedPkt := pkt
	remappedPkt.Time = pkt.Time + pb.timestampOffset

	// Ensure monotonic (safety check)
	if remappedPkt.Time <= pb.lastTimestamp {
		remappedPkt.Time = pb.lastTimestamp + time.Millisecond
	}

	pb.lastTimestamp = remappedPkt.Time
	return remappedPkt
}

// Close shuts down the broadcaster and closes all subscriber channels
func (pb *PersistentBroadcaster) Close() {
	// Stop stall checker first (serialize with SetStallConfig)
	pb.stallMu.Lock()
	if pb.stallCheckStop != nil {
		close(pb.stallCheckStop)
		pb.stallCheckStop = nil
		pb.stallCheckWg.Wait()
	}
	pb.stallMu.Unlock()

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.closed {
		return
	}
	pb.closed = true

	for sub := range pb.subscribers {
		sub.closeMu.Lock()
		if !sub.closed {
			sub.closed = true
			close(sub.pktChan)
		}
		sub.closeMu.Unlock()
	}
	pb.subscribers = make(map[*Subscription]struct{})
}

// SubscriberCount returns the current number of subscribers
func (pb *PersistentBroadcaster) SubscriberCount() int {
	pb.mu.RLock()
	defer pb.mu.RUnlock()
	return len(pb.subscribers)
}

// GetBroadcastFunc returns a function that can be used to broadcast packets
// This is useful for adapting to the existing BroadcasterFunc interface
func (pb *PersistentBroadcaster) GetBroadcastFunc(sourceID int) func(pkt av.Packet) {
	return func(pkt av.Packet) {
		pb.Broadcast(pkt, sourceID)
	}
}

// ActiveSourceID returns the current active source ID, or 0 if not set
func (pb *PersistentBroadcaster) ActiveSourceID() int {
	pb.tsMu.Lock()
	defer pb.tsMu.Unlock()
	if !pb.activeSourceSet {
		return 0
	}
	return pb.activeSourceID
}

// IsPendingSwitch returns whether a source switch is pending (waiting for keyframe)
func (pb *PersistentBroadcaster) IsPendingSwitch() bool {
	pb.tsMu.Lock()
	defer pb.tsMu.Unlock()
	return pb.pendingSourceSwitch
}
