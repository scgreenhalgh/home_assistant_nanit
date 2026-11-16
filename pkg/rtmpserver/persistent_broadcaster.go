package rtmpserver

import (
	"bytes"
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

// sessionRestartThreshold is how far a track's timestamp may jump before it
// is treated as a new session (reconnect) and rebased. Without a new video
// config only a backward jump counts; after one, a forward jump counts too.
const sessionRestartThreshold = time.Second

// keyframeGateTimeout is the longest the video gate waits for a keyframe
// before it opens anyway. A variable so tests can shorten it.
var keyframeGateTimeout = 10 * time.Second

// sourceState is what the broadcaster keeps per source ID (guarded by tsMu).
type sourceState struct {
	// headers holds one header per headerKey, in order of first arrival.
	// Stored for every source, also one that is not forwarded (hot standby).
	headers []av.Packet

	// Previous incoming timestamp per track (0 = video, 1 = audio) of the
	// forwarded packets of this source
	lastIn [2]time.Duration
	haveIn [2]bool

	// maybeNewSession is set when a video config arrives after media; the
	// next media packet decides whether a new session started
	maybeNewSession bool
}

// headerKey is the storage key of a header: its type. H264SPSPPSNALU carries a
// single NALU, so it is keyed by NALU type to keep an SPS and a PPS apart.
func headerKey(pkt av.Packet) int {
	if pkt.Type == av.H264SPSPPSNALU && len(pkt.Data) > 0 {
		return pkt.Type<<8 | int(pkt.Data[0]&0x1f)
	}
	return pkt.Type
}

func (st *sourceState) header(key int) (av.Packet, bool) {
	for _, h := range st.headers {
		if headerKey(h) == key {
			return h, true
		}
	}
	return av.Packet{}, false
}

// storeHeader replaces the stored header with the same key in place, or appends.
func (st *sourceState) storeHeader(pkt av.Packet) {
	key := headerKey(pkt)
	for i := range st.headers {
		if headerKey(st.headers[i]) == key {
			st.headers[i] = pkt
			return
		}
	}
	st.headers = append(st.headers, pkt)
}

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
// - Header packets are stored per source and sent to new subscribers
// - A source's session restart (reconnect) is detected and rebased
// - Stall detection with callback for hot standby switching
// - Active source filtering - only forwards packets from active source
type PersistentBroadcaster struct {
	babyUID string

	mu           sync.RWMutex
	subscribers  map[*Subscription]struct{}
	closed       bool

	// Timestamp remapping state
	tsMu            sync.Mutex
	lastSourceID    int
	lastTimestamp   time.Duration
	timestampOffset time.Duration
	initialized     bool

	// Per-source headers and session detection (see detectSessionLocked)
	sources           map[int]*sourceState
	inputSeen         bool      // a media packet passed session detection
	lastInputSourceID int       // source of the last packet that passed session detection
	lastPacketSource  int       // filtering off: the source that last sent a packet
	videoGate         bool      // hold video until a keyframe (new session or changed video config)
	videoGateSince    time.Time // when videoGate was armed

	// Active source filtering (for hot standby)
	activeSourceID    int  // Only forward packets from this source (0 = accept all)
	activeSourceSet   bool // Whether active source filtering is enabled

	// Keyframe-aligned switching
	pendingSourceID      int  // Source we want to switch to (waiting for keyframe)
	pendingSourceSwitch  bool // Whether we're waiting for a keyframe from pendingSourceID
	keyframeReceived     bool // Whether we've received a keyframe from current active source

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
		sources:     make(map[int]*sourceState),
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
		pb.pendingSourceID = 0
		log.Debug().Str("baby_uid", pb.babyUID).Msg("Active source filtering disabled")
	} else {
		changed := pb.forwardedSourceLocked() != sourceID
		pb.activeSourceID = sourceID
		pb.activeSourceSet = true
		pb.keyframeReceived = false // Reset - need keyframe from new source
		pb.pendingSourceSwitch = false
		pb.pendingSourceID = 0
		if changed {
			// Re-initialize connected decoders with the new source's headers
			pb.sendHeadersLocked(pb.sourceLocked(sourceID).headers)
		}
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
		pb.pendingSourceID = 0
		log.Debug().Str("baby_uid", pb.babyUID).Msg("Active source filtering disabled")
		return
	}

	// If already on this source, nothing to do (cancels a pending switch)
	if pb.activeSourceSet && pb.activeSourceID == sourceID {
		pb.pendingSourceSwitch = false
		pb.pendingSourceID = 0
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
	// tsMu first (lock order tsMu -> mu): the stored headers and the forwarded
	// source must not change while the subscriber is added
	pb.tsMu.Lock()
	defer pb.tsMu.Unlock()
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

	// Send the forwarded source's stored headers to the new subscriber
	var headers []av.Packet
	if st := pb.sources[pb.forwardedSourceLocked()]; st != nil {
		headers = st.headers
	}
	for _, hdr := range headers {
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
	// Handle header packets (decoder configs and metadata); H264, AAC and OPUS are media
	if pkt.Type > av.OPUS {
		pb.handleHeaderPacket(pkt, sourceID)
		return
	}

	// One tsMu hold covers the source check, session detection, the gate,
	// the remap and the distribution of this packet: a packet is remapped with
	// the offset its own detection saw, and subscribers get packets in remap
	// order (a switch's headers can never overtake an old-source packet).
	// Distribution never blocks (full buffers drop), so the hold is short.
	pb.tsMu.Lock()
	defer pb.tsMu.Unlock()
	st := pb.sourceLocked(sourceID)

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

		// Send the new source's stored headers to all existing subscribers
		// before its keyframe, for decoder re-initialization
		pb.sendHeadersLocked(st.headers)
	}

	// Check if this source is active (for hot standby filtering)
	if pb.activeSourceSet && sourceID != pb.activeSourceID {
		// Not the active source - don't forward but don't update wall time either
		// (stall detection should only track the active source)
		return
	}
	if !pb.activeSourceSet {
		pb.lastPacketSource = sourceID
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
		return
	}

	// Detect a session restart of this source and rebase if needed
	pb.detectSessionLocked(pkt, sourceID, st)

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

	// Video gate: after a new session or a changed video config, hold video
	// (not audio) until a keyframe. Held packets still count as activity above.
	if pb.videoGate && pkt.Type == av.H264 {
		if pkt.IsKeyFrame {
			pb.videoGate = false
		} else if time.Since(pb.videoGateSince) >= keyframeGateTimeout {
			pb.videoGate = false
			log.Warn().
				Str("baby_uid", pb.babyUID).
				Int("source_id", sourceID).
				Dur("timeout", keyframeGateTimeout).
				Msg("No keyframe after new video config, forwarding video anyway")
		} else {
			return
		}
	}

	// Remap timestamp for continuity
	pkt = pb.remapTimestampLocked(pkt, sourceID)

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

// handleHeaderPacket stores a header for its source and, if that source is
// forwarded, sends it to connected subscribers
func (pb *PersistentBroadcaster) handleHeaderPacket(pkt av.Packet, sourceID int) {
	pb.tsMu.Lock()
	defer pb.tsMu.Unlock()

	st := pb.sourceLocked(sourceID)
	forwarded := !pb.activeSourceSet || sourceID == pb.activeSourceID
	if !pb.activeSourceSet {
		pb.lastPacketSource = sourceID
	}

	old, stored := st.header(headerKey(pkt))
	identical := stored && bytes.Equal(old.Data, pkt.Data)

	// A video or audio config after media: maybe a new session (decided at
	// the next media packet). A changed video config needs a keyframe after it.
	isVideoConfig := pkt.Type == av.H264DecoderConfig || pkt.Type == av.H264SPSPPSNALU
	isConfig := isVideoConfig || pkt.Type == av.AACDecoderConfig
	if isConfig && forwarded && (st.haveIn[0] || st.haveIn[1]) {
		st.maybeNewSession = true
		if isVideoConfig && stored && !identical {
			pb.armVideoGateLocked()
			log.Info().Str("baby_uid", pb.babyUID).Int("source_id", sourceID).Msg("Video config changed, holding video until a keyframe")
		}
	}

	// A header identical to the stored one changes nothing for a decoder:
	// not forwarded (encoders that resend their config before every keyframe)
	if identical {
		return
	}
	st.storeHeader(pkt)
	if forwarded {
		pb.sendHeadersLocked([]av.Packet{pkt})
	}
}

// sourceLocked returns the state of a source, creating it. Caller holds tsMu.
func (pb *PersistentBroadcaster) sourceLocked(sourceID int) *sourceState {
	st := pb.sources[sourceID]
	if st == nil {
		st = &sourceState{}
		pb.sources[sourceID] = st
	}
	return st
}

// forwardedSourceLocked returns the source whose packets subscribers get: the
// active source, or with filtering off the source that last sent a packet.
// Caller holds tsMu.
func (pb *PersistentBroadcaster) forwardedSourceLocked() int {
	if pb.activeSourceSet {
		return pb.activeSourceID
	}
	return pb.lastPacketSource
}

func (pb *PersistentBroadcaster) armVideoGateLocked() {
	if !pb.videoGate {
		pb.videoGate = true
		pb.videoGateSince = time.Now()
	}
}

// sendHeadersLocked sends headers to all connected subscribers, stamped with
// the current output timestamp so the output never steps back. Caller holds
// tsMu (lock order tsMu -> mu -> closeMu).
func (pb *PersistentBroadcaster) sendHeadersLocked(headers []av.Packet) {
	if len(headers) == 0 {
		return
	}
	pb.mu.RLock()
	defer pb.mu.RUnlock()
	dropped := false
	for sub := range pb.subscribers {
		sub.closeMu.Lock()
		if !sub.closed {
			for _, hdr := range headers {
				hdr.Time = pb.lastTimestamp
				select {
				case sub.pktChan <- hdr:
				default:
					dropped = true
				}
			}
		}
		sub.closeMu.Unlock()
	}
	if dropped {
		log.Warn().
			Str("baby_uid", pb.babyUID).
			Msg("Header dropped because a subscriber buffer is full; that subscriber cannot decode the new session")
	}
}

// detectSessionLocked rebases the timeline when the forwarded source changes
// or restarts its session, so output continues at the last output timestamp
// plus one frame. A restart is detected per track (video and audio apart):
//   - after a new video or audio config: the next timestamp is below the
//     previous one, or more than sessionRestartThreshold above it
//     (continuous = config resent)
//   - otherwise: the timestamp is more than sessionRestartThreshold below the
//     previous one (smaller steps are interleave and only hit the clamp)
//
// Caller holds tsMu.
func (pb *PersistentBroadcaster) detectSessionLocked(pkt av.Packet, sourceID int, st *sourceState) {
	track := 1
	if pkt.Type == av.H264 {
		track = 0
	}

	newSession, backward := false, false
	if prev := st.lastIn[track]; st.haveIn[track] {
		d := pkt.Time - prev
		if st.maybeNewSession {
			newSession = d < 0 || d > sessionRestartThreshold
		} else {
			newSession = d < -sessionRestartThreshold
		}
		backward = d < 0
		st.maybeNewSession = false
	}

	if pb.inputSeen && (sourceID != pb.lastInputSourceID || newSession) {
		// New timestamp = incoming timestamp + offset
		// We want: pkt.Time + newOffset = lastTimestamp + frameInterval
		frameInterval := 33 * time.Millisecond // ~30fps
		pb.timestampOffset = pb.lastTimestamp + frameInterval - pkt.Time
		pb.lastSourceID = sourceID
		st.haveIn = [2]bool{} // one restart must not fire twice
		st.maybeNewSession = false
		if newSession {
			log.Info().
				Str("baby_uid", pb.babyUID).
				Int("source_id", sourceID).
				Dur("new_ts", pkt.Time).
				Msg("Source started a new session, rebasing timestamps")
			// A backward step is a certain restart: its first frames may not be
			// decodable. A forward jump after a config may be a camera stall
			// after a resent config: rebase for continuity, but do not gate.
			if backward && !(pkt.Type == av.H264 && pkt.IsKeyFrame) {
				pb.armVideoGateLocked()
			}
		}
	}
	pb.inputSeen = true
	pb.lastInputSourceID = sourceID
	st.lastIn[track] = pkt.Time
	st.haveIn[track] = true
}

// remapTimestampLocked applies the offset set by detectSessionLocked. Caller
// holds tsMu.
func (pb *PersistentBroadcaster) remapTimestampLocked(pkt av.Packet, sourceID int) av.Packet {
	if !pb.initialized {
		// First packet ever
		pb.initialized = true
		pb.lastSourceID = sourceID
		pb.lastTimestamp = pkt.Time
		pb.timestampOffset = 0
		return pkt
	}

	// Apply offset (set by detectSessionLocked)
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
