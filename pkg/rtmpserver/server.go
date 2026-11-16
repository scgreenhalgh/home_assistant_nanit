package rtmpserver

import (
	"net"
	"regexp"
	"sync"
	"time"

	"github.com/notedit/rtmp/av"
	"github.com/notedit/rtmp/format/rtmp"
	"github.com/rs/zerolog/log"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/baby"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/security/ipaccess"
)

// Source IDs for timestamp remapping
const (
	SourceIDLocal  = 1 // Camera publishing directly to RTMP server
	SourceIDRemote = 2 // Remote relay from Nanit cloud
)

// ServerConfig holds configuration for the RTMP server
type ServerConfig struct {
	// ListenAddr is the address to listen on (e.g., ":1935")
	ListenAddr string

	// Security options for IP-based access control
	AllowedIPs     string // Comma-separated IPs/CIDRs
	AllowedPresets string // Comma-separated preset names
	LogDenied      bool   // Log denied connections
	LogAllowed     bool   // Log allowed connections
}

// RTMPServer wraps the RTMP server and provides access to broadcasters
type RTMPServer struct {
	handler  *rtmpHandler
	ipAccess *ipaccess.Controller
	listener net.Listener
	closed   bool
	mu       sync.Mutex
}

type rtmpHandler struct {
	babyStateManager  *baby.StateManager
	broadcastersMu    sync.RWMutex
	broadcastersByUID map[string]*PersistentBroadcaster

	// Publisher wait channels for timeout detection
	publisherWaitMu sync.Mutex
	publisherWaitCh map[string]chan struct{}

	// Track active local publishers count to detect reconnections vs drops
	localPublishersMu sync.RWMutex
	localPublishers   map[string]int       // count of active publishers per baby
	lastLocalConnect  map[string]time.Time // when a local publisher last connected, per baby
	streamStateClaims map[string]int       // claims on StreamState per baby; the handler writes it only at 0
}

// StartRTMPServer - Blocking server (legacy, starts in background and returns RTMPServer for access)
// Deprecated: Use NewServer with ServerConfig for new code
func StartRTMPServer(addr string, babyStateManager *baby.StateManager) *RTMPServer {
	server := NewRTMPServer(babyStateManager)
	go server.RunWithAddr(addr)
	return server
}

// NewRTMPServer creates a new RTMP server without security config (backward compatible)
// Deprecated: Use NewServer with ServerConfig for new code
func NewRTMPServer(babyStateManager *baby.StateManager) *RTMPServer {
	return &RTMPServer{
		handler: newRtmpHandler(babyStateManager),
	}
}

// NewServer creates a new RTMP server with the given configuration
func NewServer(cfg ServerConfig, babyStateManager *baby.StateManager) (*RTMPServer, error) {
	server := &RTMPServer{
		handler: newRtmpHandler(babyStateManager),
	}

	// Initialize IP access controller if any security options are set
	if cfg.AllowedIPs != "" || cfg.AllowedPresets != "" {
		ipAccessCfg := ipaccess.Config{
			AllowedIPs:     cfg.AllowedIPs,
			AllowedPresets: cfg.AllowedPresets,
			LogDenied:      cfg.LogDenied,
			LogAllowed:     cfg.LogAllowed,
			Logger:         log.With().Str("component", "rtmp-ipaccess").Logger(),
		}

		ipAccess, err := ipaccess.New(ipAccessCfg)
		if err != nil {
			return nil, err
		}

		server.ipAccess = ipAccess
		log.Info().Str("config", ipAccess.String()).Msg("RTMP IP access control configured")
	} else {
		log.Warn().Msg("RTMP server running without IP access control - all connections will be denied")
	}

	return server, nil
}

// Run starts the RTMP server (blocking) using config from NewServer
func (srv *RTMPServer) Run(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error().Str("addr", addr).Err(err).Msg("Unable to start RTMP server")
		return err
	}

	srv.mu.Lock()
	srv.listener = lis
	srv.mu.Unlock()

	log.Info().Str("addr", addr).Msg("RTMP server started")

	s := rtmp.NewServer()
	s.HandleConn = srv.handleConnectionWithSecurity

	for {
		nc, err := lis.Accept()
		if err != nil {
			srv.mu.Lock()
			closed := srv.closed
			srv.mu.Unlock()
			if closed {
				return nil // Normal shutdown
			}
			log.Warn().Err(err).Msg("Error accepting connection")
			time.Sleep(time.Second)
			continue
		}
		go s.HandleNetConn(nc)
	}
}

// Close gracefully shuts down the RTMP server
func (srv *RTMPServer) Close() error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	srv.closed = true
	if srv.listener != nil {
		return srv.listener.Close()
	}
	return nil
}

// handleConnectionWithSecurity wraps handleConnection with IP access control
func (srv *RTMPServer) handleConnectionWithSecurity(c *rtmp.Conn, nc net.Conn) {
	// Check IP access control if configured
	if srv.ipAccess != nil {
		remoteAddr := nc.RemoteAddr().String()
		if !srv.ipAccess.IsAllowed(remoteAddr) {
			nc.Close()
			return
		}
	}

	// Proceed with normal connection handling
	srv.handler.handleConnection(c, nc)
}

// RunWithAddr starts the RTMP server on the given address (blocking)
func (srv *RTMPServer) RunWithAddr(addr string) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal().Str("addr", addr).Err(err).Msg("Unable to start RTMP server")
		panic(err)
	}

	log.Info().Str("addr", addr).Msg("RTMP server started")

	s := rtmp.NewServer()
	s.HandleConn = srv.handler.handleConnection

	for {
		nc, err := lis.Accept()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		go s.HandleNetConn(nc)
	}
}

// GetOrCreateBroadcaster returns an existing broadcaster or creates one for remote streaming
// Returns the broadcast function that can be used to send packets (with remote source ID)
func (srv *RTMPServer) GetOrCreateBroadcaster(babyUID string) func(pkt av.Packet) {
	pb := srv.getOrCreatePersistentBroadcaster(babyUID)
	return pb.GetBroadcastFunc(SourceIDRemote)
}

// getOrCreatePersistentBroadcaster gets or creates a PersistentBroadcaster for the baby
func (srv *RTMPServer) getOrCreatePersistentBroadcaster(babyUID string) *PersistentBroadcaster {
	srv.handler.broadcastersMu.Lock()
	defer srv.handler.broadcastersMu.Unlock()

	existing, exists := srv.handler.broadcastersByUID[babyUID]
	if exists {
		return existing
	}

	// Create a new persistent broadcaster
	pb := NewPersistentBroadcaster(babyUID)
	srv.handler.broadcastersByUID[babyUID] = pb
	log.Info().Str("baby_uid", babyUID).Msg("Created persistent broadcaster")
	return pb
}

// GetBroadcaster returns the PersistentBroadcaster for a baby, or nil if none exists
func (srv *RTMPServer) GetBroadcaster(babyUID string) *PersistentBroadcaster {
	srv.handler.broadcastersMu.RLock()
	defer srv.handler.broadcastersMu.RUnlock()
	return srv.handler.broadcastersByUID[babyUID]
}

// RemoveBroadcaster removes the broadcaster for a baby (used when stream stops permanently)
// Note: With PersistentBroadcaster, this should rarely be called since broadcasters persist
func (srv *RTMPServer) RemoveBroadcaster(babyUID string) {
	srv.handler.broadcastersMu.Lock()
	defer srv.handler.broadcastersMu.Unlock()

	if pb, exists := srv.handler.broadcastersByUID[babyUID]; exists {
		pb.Close()
		delete(srv.handler.broadcastersByUID, babyUID)
		log.Info().Str("baby_uid", babyUID).Msg("Removed persistent broadcaster")
	}
}

// WaitForPublisher waits for a publisher to connect for the given babyUID.
// Returns nil if publisher connects within timeout, or an error if timeout occurs.
func (srv *RTMPServer) WaitForPublisher(babyUID string, timeout time.Duration) error {
	// Check if publisher is already active (fixes race condition where camera
	// connects before we start waiting)
	if srv.handler.IsLocalPublisherActive(babyUID) {
		log.Debug().Str("baby_uid", babyUID).Msg("Publisher already active, no need to wait")
		return nil
	}

	// Create a channel to wait for publisher
	ch := make(chan struct{})

	srv.handler.publisherWaitMu.Lock()
	// Double-check after acquiring lock (publisher may have connected while we were waiting for lock)
	if srv.handler.IsLocalPublisherActive(babyUID) {
		srv.handler.publisherWaitMu.Unlock()
		log.Debug().Str("baby_uid", babyUID).Msg("Publisher connected while acquiring lock")
		return nil
	}
	srv.handler.publisherWaitCh[babyUID] = ch
	srv.handler.publisherWaitMu.Unlock()

	// Cleanup on exit
	defer func() {
		srv.handler.publisherWaitMu.Lock()
		delete(srv.handler.publisherWaitCh, babyUID)
		srv.handler.publisherWaitMu.Unlock()
	}()

	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
		return ErrPublisherTimeout
	}
}

// LocalPublisherConnectedAt returns when a local publisher last connected for
// the baby, or the zero time if none has.
func (srv *RTMPServer) LocalPublisherConnectedAt(babyUID string) time.Time {
	srv.handler.localPublishersMu.RLock()
	defer srv.handler.localPublishersMu.RUnlock()
	return srv.handler.lastLocalConnect[babyUID]
}

// ClaimStreamState makes the caller the writer of the baby's StreamState: the
// publish handler stops writing it until the last claim is released. Claims
// are counted. When it returns, a handler write that had begun is complete,
// so the caller's next write lands after it.
func (srv *RTMPServer) ClaimStreamState(babyUID string) {
	srv.handler.localPublishersMu.Lock()
	defer srv.handler.localPublishersMu.Unlock()
	srv.handler.streamStateClaims[babyUID]++
}

// ReleaseStreamState drops one claim. If it was the last, final runs with
// whether a local publisher is connected, before the handler can write again
// and while no publisher can connect or leave: every later handler write lands
// after final's. While other claims remain, final does not run. final must not
// call back into the RTMPServer.
func (srv *RTMPServer) ReleaseStreamState(babyUID string, final func(publisherConnected bool)) {
	h := srv.handler
	h.localPublishersMu.Lock()
	defer h.localPublishersMu.Unlock()
	h.streamStateClaims[babyUID]--
	if h.streamStateClaims[babyUID] > 0 {
		return
	}
	delete(h.streamStateClaims, babyUID)
	final(h.localPublishers[babyUID] > 0)
}

// ErrPublisherTimeout is returned when waiting for publisher times out
var ErrPublisherTimeout = net.UnknownNetworkError("publisher connection timeout")

// WaitForPublisherWithURL waits for a publisher at a specific URL to connect.
// The URL should be the full RTMP URL (e.g., rtmp://host:port/local/babyUID_0)
// This extracts the stream key from the URL and waits for that specific publisher.
func (srv *RTMPServer) WaitForPublisherWithURL(babyUID, url string, timeout time.Duration) error {
	// Extract stream key from URL (e.g., "babyuid_0" from "rtmp://host:port/local/babyuid_0")
	// The URL path should be /local/{streamKey}
	streamKey := babyUID // Default to babyUID if we can't parse
	if idx := len(url) - 1; idx > 0 {
		for i := idx; i >= 0; i-- {
			if url[i] == '/' {
				streamKey = url[i+1:]
				break
			}
		}
	}

	// Check if publisher is already active
	srv.handler.localPublishersMu.RLock()
	if srv.handler.localPublishers[streamKey] > 0 {
		srv.handler.localPublishersMu.RUnlock()
		log.Debug().Str("stream_key", streamKey).Msg("Publisher already active for URL")
		return nil
	}
	srv.handler.localPublishersMu.RUnlock()

	// Create a channel to wait for this specific publisher
	ch := make(chan struct{})

	srv.handler.publisherWaitMu.Lock()
	// Double-check after lock
	srv.handler.localPublishersMu.RLock()
	if srv.handler.localPublishers[streamKey] > 0 {
		srv.handler.localPublishersMu.RUnlock()
		srv.handler.publisherWaitMu.Unlock()
		log.Debug().Str("stream_key", streamKey).Msg("Publisher connected while acquiring lock")
		return nil
	}
	srv.handler.localPublishersMu.RUnlock()
	srv.handler.publisherWaitCh[streamKey] = ch
	srv.handler.publisherWaitMu.Unlock()

	// Cleanup on exit
	defer func() {
		srv.handler.publisherWaitMu.Lock()
		delete(srv.handler.publisherWaitCh, streamKey)
		srv.handler.publisherWaitMu.Unlock()
	}()

	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
		return ErrPublisherTimeout
	}
}

func newRtmpHandler(babyStateManager *baby.StateManager) *rtmpHandler {
	return &rtmpHandler{
		broadcastersByUID: make(map[string]*PersistentBroadcaster),
		babyStateManager:  babyStateManager,
		publisherWaitCh:   make(map[string]chan struct{}),
		localPublishers:   make(map[string]int),
		lastLocalConnect:  make(map[string]time.Time),
		streamStateClaims: make(map[string]int),
	}
}

// rtmpURLRX matches:
// - /local/{babyUID} or /local/{babyUID}_{slot} for publishers
// - /live/{babyUID} for subscribers
// Group 1: babyUID, Group 2 (optional): slot suffix including underscore
var rtmpURLRX = regexp.MustCompile(`^/(local|live)/([a-z0-9-]+)(_[0-9]+)?$`)

func (s *rtmpHandler) handleConnection(c *rtmp.Conn, nc net.Conn) {
	sublog := log.With().Stringer("client_addr", nc.RemoteAddr()).Logger()

	submatch := rtmpURLRX.FindStringSubmatch(c.URL.Path)
	if len(submatch) < 3 {
		sublog.Warn().Str("path", c.URL.Path).Msg("Invalid RTMP stream requested")
		nc.Close()
		return
	}

	// Group 1: "local" or "live", Group 2: babyUID, Group 3 (optional): slot suffix
	babyUID := submatch[2]
	slotSuffix := ""
	if len(submatch) >= 4 {
		slotSuffix = submatch[3] // e.g., "_0" or "_1"
	}
	streamKey := babyUID + slotSuffix // Full stream key for tracking

	sublog = sublog.With().Str("baby_uid", babyUID).Str("stream_key", streamKey).Logger()

	if c.Publishing {
		sublog.Info().Msg("New local stream publisher connected")
		broadcastFunc := s.registerLocalPublisher(babyUID, slotSuffix, streamKey)

		// Signal any waiters that publisher connected
		// Check both streamKey-specific and babyUID-general waiters
		s.publisherWaitMu.Lock()
		if ch, exists := s.publisherWaitCh[streamKey]; exists {
			close(ch)
			delete(s.publisherWaitCh, streamKey)
		}
		if ch, exists := s.publisherWaitCh[babyUID]; exists {
			close(ch)
			delete(s.publisherWaitCh, babyUID)
		}
		s.publisherWaitMu.Unlock()

		s.updateStreamState(babyUID, baby.StreamState_Alive)

		for {
			pkt, err := c.ReadPacket()
			if err != nil {
				// Unregister and check if other publishers remain
				remaining := s.unregisterLocalPublisher(babyUID, streamKey)

				if remaining > 0 {
					// Normal reconnection - another publisher already took over
					sublog.Info().Err(err).Msg("Local publisher stream closed (new connection active)")
				} else {
					// No other publisher - this is an actual stream drop
					sublog.Warn().Err(err).Msg("Local publisher stream closed (no replacement)")
					s.updateStreamState(babyUID, baby.StreamState_Unhealthy)
				}
				return
			}

			broadcastFunc(pkt)
		}

	} else {
		sublog.Debug().Msg("New stream subscriber connected")
		subscription := s.getNewSubscriber(babyUID)

		if subscription == nil {
			sublog.Warn().Msg("No broadcaster registered yet, closing subscriber stream")
			nc.Close()
			return
		}

		closeC := c.CloseNotify()
		for {
			select {
			case pkt, open := <-subscription.Packets():
				if !open {
					sublog.Debug().Msg("Subscription closed")
					nc.Close()
					return
				}

				c.WritePacket(pkt)

			case <-closeC:
				sublog.Debug().Msg("Stream subscriber disconnected")
				subscription.Unsubscribe()
				return
			}
		}
	}
}

// registerLocalPublisher gets/creates a PersistentBroadcaster and returns the local broadcast function
// slotSuffix is the optional slot identifier (e.g., "_0", "_1"), empty for legacy single-stream mode
// streamKey is the full key (babyUID + slotSuffix) for publisher tracking
func (s *rtmpHandler) registerLocalPublisher(babyUID, slotSuffix, streamKey string) func(pkt av.Packet) {
	s.broadcastersMu.Lock()
	pb, exists := s.broadcastersByUID[babyUID]
	if !exists {
		pb = NewPersistentBroadcaster(babyUID)
		s.broadcastersByUID[babyUID] = pb
		log.Info().Str("baby_uid", babyUID).Msg("Created persistent broadcaster for local publisher")
	}
	s.broadcastersMu.Unlock()

	// Increment local publisher count (by babyUID for backward compatibility)
	s.localPublishersMu.Lock()
	s.localPublishers[babyUID]++
	count := s.localPublishers[babyUID]
	s.lastLocalConnect[babyUID] = time.Now()
	// Also track by streamKey for pool-aware lookups
	if streamKey != babyUID {
		s.localPublishers[streamKey]++
	}
	s.localPublishersMu.Unlock()

	log.Info().Str("baby_uid", babyUID).Str("stream_key", streamKey).Int("active_count", count).Msg("Local publisher registered")

	// Determine source ID based on slot suffix
	sourceID := SourceIDLocal // Default for legacy single-stream
	if slotSuffix == "_0" {
		sourceID = 10 // Pool slot 0
	} else if slotSuffix == "_1" {
		sourceID = 11 // Pool slot 1
	}

	return pb.GetBroadcastFunc(sourceID)
}

// unregisterLocalPublisher decrements the publisher count and returns the remaining count
func (s *rtmpHandler) unregisterLocalPublisher(babyUID, streamKey string) int {
	s.localPublishersMu.Lock()
	s.localPublishers[babyUID]--
	remaining := s.localPublishers[babyUID]
	if remaining <= 0 {
		delete(s.localPublishers, babyUID)
		remaining = 0
	}
	// Also decrement streamKey count
	if streamKey != babyUID {
		s.localPublishers[streamKey]--
		if s.localPublishers[streamKey] <= 0 {
			delete(s.localPublishers, streamKey)
		}
	}
	s.localPublishersMu.Unlock()

	log.Info().Str("baby_uid", babyUID).Str("stream_key", streamKey).Int("remaining_count", remaining).Msg("Local publisher unregistered")
	return remaining
}

// getNewSubscriber creates a subscription to the PersistentBroadcaster
func (s *rtmpHandler) getNewSubscriber(babyUID string) *Subscription {
	s.broadcastersMu.RLock()
	pb, hasBroadcaster := s.broadcastersByUID[babyUID]
	s.broadcastersMu.RUnlock()

	if !hasBroadcaster {
		return nil
	}

	return pb.Subscribe()
}

// updateStreamState writes StreamState unless a stream pool has claimed it.
// The check and the write are one step against ClaimStreamState and
// ReleaseStreamState. The caller must not hold localPublishersMu.
func (s *rtmpHandler) updateStreamState(babyUID string, state baby.StreamState) {
	s.localPublishersMu.RLock()
	defer s.localPublishersMu.RUnlock()
	if s.streamStateClaims[babyUID] > 0 {
		return
	}
	s.babyStateManager.Update(babyUID, *baby.NewState().SetStreamState(state))
}

// IsLocalPublisherActive returns whether a local publisher is currently active for the baby
func (s *rtmpHandler) IsLocalPublisherActive(babyUID string) bool {
	s.localPublishersMu.RLock()
	defer s.localPublishersMu.RUnlock()
	return s.localPublishers[babyUID] > 0
}
