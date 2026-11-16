package stream

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/notedit/rtmp/av"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/baby"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/client"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/rtmpserver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testBabyUID  = "baby1"
	testLocalURL = "rtmp://10.0.0.1:1935/local/baby1"
	waitTimeout  = 3 * time.Second
)

var errForbidden = errors.New("Forbidden: Number of Mobile App connections above limit, declining connection")

// --- fake requester ---

type recordedRequest struct {
	Type   client.RequestType
	Status client.Streaming_Status
	URL    string
	At     time.Time
}

type fakeRequester struct {
	mu       sync.Mutex
	requests []recordedRequest
	// reply decides the answer for a request; nil means "200 OK at once".
	reply func(r recordedRequest) (time.Duration, error)
	// block, if set, makes SendRequest itself block until it is closed.
	block chan struct{}
}

func (f *fakeRequester) SendRequest(t client.RequestType, req *client.Request) func(time.Duration) (*client.Response, error) {
	r := recordedRequest{Type: t, Status: req.GetStreaming().GetStatus(), URL: req.GetStreaming().GetRtmpUrl(), At: time.Now()}
	f.mu.Lock()
	f.requests = append(f.requests, r)
	reply := f.reply
	block := f.block
	f.mu.Unlock()
	if block != nil {
		<-block
	}

	return func(timeout time.Duration) (*client.Response, error) {
		var delay time.Duration
		var err error
		if reply != nil {
			delay, err = reply(r)
		}
		if delay > timeout {
			time.Sleep(timeout)
			return nil, errors.New("Request timeout")
		}
		time.Sleep(delay)
		return &client.Response{}, err
	}
}

func (f *fakeRequester) count(status client.Streaming_Status) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.requests {
		if r.Type == client.RequestType_PUT_STREAMING && r.Status == status {
			n++
		}
	}
	return n
}

func (f *fakeRequester) all() []recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedRequest(nil), f.requests...)
}

// --- fake relay ---

type fakeRelay struct {
	cfg     RemoteRelayConfig
	started atomic.Int32
	stopped atomic.Int32
}

func (r *fakeRelay) Start(ctx context.Context) error { r.started.Add(1); return nil }
func (r *fakeRelay) Stop() error                     { r.stopped.Add(1); return nil }

// --- packet feeder ---

// feeder broadcasts one media packet every 10 ms while running. The first
// packet after each start is a keyframe, then every 5th packet if keyframes is true.
type feeder struct {
	send      func(av.Packet)
	marker    byte
	keyframes bool

	mu   sync.Mutex
	stop chan struct{}
	wg   sync.WaitGroup
}

func (f *feeder) start() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stop != nil {
		return
	}
	stop := make(chan struct{})
	f.stop = stop
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for i := 0; ; i++ {
			kf := i == 0 || (f.keyframes && i%5 == 0)
			f.send(av.Packet{Type: av.H264, IsKeyFrame: kf, Time: time.Duration(i) * 33 * time.Millisecond, Data: []byte{f.marker}})
			select {
			case <-stop:
				return
			case <-tick.C:
			}
		}
	}()
}

func (f *feeder) halt() {
	f.mu.Lock()
	stop := f.stop
	f.stop = nil
	f.mu.Unlock()
	if stop != nil {
		close(stop)
		f.wg.Wait()
	}
}

// --- subscriber ---

type packetCounter struct {
	local, remote atomic.Int64
	// first remote media packet seen, and whether it was a keyframe
	firstRemoteSeen, firstRemoteKF atomic.Bool
}

func subscribe(t *testing.T, b *rtmpserver.PersistentBroadcaster) *packetCounter {
	c := &packetCounter{}
	sub := b.Subscribe()
	require.NotNil(t, sub)
	go func() {
		for pkt := range sub.Packets() {
			if pkt.Type > 2 || len(pkt.Data) == 0 {
				continue
			}
			switch pkt.Data[0] {
			case 'L':
				c.local.Add(1)
			case 'R':
				if !c.firstRemoteSeen.Load() {
					c.firstRemoteKF.Store(pkt.IsKeyFrame)
					c.firstRemoteSeen.Store(true)
				}
				c.remote.Add(1)
			}
		}
	}()
	t.Cleanup(sub.Unsubscribe)
	return c
}

// --- harness ---

type harness struct {
	t      *testing.T
	sm     *baby.StateManager
	srv    *rtmpserver.RTMPServer
	b      *rtmpserver.PersistentBroadcaster
	req    *fakeRequester
	relay  *fakeRelay
	pool   *HybridStreamPool
	local  *feeder
	remote *feeder
}

func testConfig() HybridStreamPoolConfig {
	return HybridStreamPoolConfig{
		BabyUID:          testBabyUID,
		LocalURL:         testLocalURL,
		StallThreshold:   time.Hour, // the 1 s stall checker is not used by these tests
		WatchdogInterval: 20 * time.Millisecond,
		HealthWindow:     150 * time.Millisecond,
		MinRetryInterval: 50 * time.Millisecond,
		InitialBackoff:   50 * time.Millisecond,
		MaxBackoff:       200 * time.Millisecond,
		RequestTimeout:   200 * time.Millisecond,
		PublisherWait:    150 * time.Millisecond,
	}
}

func newHarness(t *testing.T, mutate func(*HybridStreamPoolConfig)) *harness {
	h := &harness{t: t, sm: baby.NewStateManager(), req: &fakeRequester{}, relay: &fakeRelay{}}
	h.srv = rtmpserver.NewRTMPServer(h.sm)
	h.srv.GetOrCreateBroadcaster(testBabyUID)
	h.b = h.srv.GetBroadcaster(testBabyUID)
	h.local = &feeder{send: h.b.GetBroadcastFunc(HybridSourceLocal), marker: 'L', keyframes: true}
	h.remote = &feeder{marker: 'R', keyframes: true}

	h.pool = h.newPool(mutate)
	t.Cleanup(func() {
		h.local.halt()
		h.remote.halt()
	})
	return h
}

// newPool makes another pool on the same server, requester and relay fake.
func (h *harness) newPool(mutate func(*HybridStreamPoolConfig)) *HybridStreamPool {
	cfg := testConfig()
	cfg.Conn = h.req
	cfg.StateManager = h.sm
	cfg.RTMPServer = h.srv
	cfg.NewRelay = func(c RemoteRelayConfig) RemoteRelay {
		h.relay.cfg = c
		h.remote.send = c.PacketReceiver.ReceivePacket
		return h.relay
	}
	if mutate != nil {
		mutate(&cfg)
	}
	pool := NewHybridStreamPool(cfg)
	h.t.Cleanup(pool.Close)
	return pool
}

func (h *harness) start() {
	done := make(chan struct{})
	go func() {
		_ = h.pool.RequestLocalStream(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(waitTimeout):
		h.t.Fatal("RequestLocalStream did not return")
	}
}

func (h *harness) state() (baby.StreamState, baby.StreamType) {
	s := h.sm.GetBabyState(testBabyUID)
	if s == nil {
		return baby.StreamState_Unknown, baby.StreamType_None
	}
	return s.GetStreamState(), s.GetStreamType()
}

func (h *harness) waitState(want baby.StreamState, msg string) {
	require.Eventually(h.t, func() bool { s, _ := h.state(); return s == want }, waitTimeout, 5*time.Millisecond, msg)
}

func (h *harness) waitType(want baby.StreamType, msg string) {
	require.Eventually(h.t, func() bool { _, ty := h.state(); return ty == want }, waitTimeout, 5*time.Millisecond, msg)
}

func (h *harness) waitStarted(n int, msg string) {
	require.Eventually(h.t, func() bool { return h.req.count(client.Streaming_STARTED) >= n }, waitTimeout, 5*time.Millisecond, msg)
}

// waitLocalHealthy lets enough local packets through for the window to see them.
func (h *harness) waitPackets(c *packetCounter, local bool, msg string) {
	var get func() int64
	if local {
		get = c.local.Load
	} else {
		get = c.remote.Load
	}
	base := get()
	require.Eventually(h.t, func() bool { return get() >= base+5 }, waitTimeout, 5*time.Millisecond, msg)
}

// T1: local packets stop with no remote -> Unhealthy and a new STARTED;
// local resumes -> Alive and no further requests.
func TestHybridPool_LocalDropWithoutRemote_RequestsAndRecovers(t *testing.T) {
	h := newHarness(t, nil)
	h.local.start()
	time.Sleep(30 * time.Millisecond)
	h.start()
	time.Sleep(100 * time.Millisecond)

	h.local.halt()
	h.waitState(baby.StreamState_Unhealthy, "Unhealthy must be published when local stops and remote is absent")
	h.waitStarted(1, "a new STARTED must be sent after local stops")
	for _, r := range h.req.all() {
		assert.Equal(t, testLocalURL, r.URL)
	}

	h.local.start()
	h.waitState(baby.StreamState_Alive, "state must return to Alive when local resumes")
	settled := h.req.count(client.Streaming_STARTED)
	time.Sleep(300 * time.Millisecond) // many watchdog ticks
	assert.Equal(t, settled, h.req.count(client.Streaming_STARTED), "no further STARTED while local is healthy")
}

// T2: a new pool (websocket reconnect) while local packets are flowing sends
// no PUT_STREAMING request and does not disturb the filter.
func TestHybridPool_StartWhileLocalFlowing_SendsNothing(t *testing.T) {
	h := newHarness(t, nil)
	// Previous pool left the filter on local; the stream already had its keyframe.
	h.b.SetActiveSource(HybridSourceLocal)
	h.local.keyframes = false // only the first packet is a keyframe
	c := subscribe(t, h.b)
	h.local.start()
	h.waitPackets(c, true, "precondition: local packets reach the subscriber")

	h.start()
	assert.Equal(t, 0, len(h.req.all()), "no PUT_STREAMING request at start while local is healthy")
	h.waitState(baby.StreamState_Alive, "state should be Alive")
	h.waitPackets(c, true, "subscriber must keep receiving local packets after the new pool starts")
	time.Sleep(200 * time.Millisecond)
	h.waitPackets(c, true, "subscriber must keep receiving local packets")

	assert.Equal(t, 0, len(h.req.all()), "no PUT_STREAMING request while local is healthy")
	assert.Equal(t, HybridSourceLocal, h.b.ActiveSourceID())
	assert.False(t, h.b.IsPendingSwitch())
	_, ty := h.state()
	assert.Equal(t, baby.StreamType_Local, ty)
}

// T3: local dead, request refused with Forbidden, then local packets arrive
// anyway: they reach the subscriber, state becomes Alive, retries stop.
func TestHybridPool_ForbiddenThenLocalArrives_LocalIsForwarded(t *testing.T) {
	h := newHarness(t, nil)
	h.req.reply = func(r recordedRequest) (time.Duration, error) {
		if r.Status == client.Streaming_STARTED {
			return 0, errForbidden
		}
		return 0, nil
	}
	c := subscribe(t, h.b)
	h.start()
	h.waitStarted(1, "a STARTED should be sent when local is dead")
	time.Sleep(50 * time.Millisecond) // let the refusal be processed

	h.local.start()
	h.waitPackets(c, true, "local packets must reach the subscriber after a refused request")
	h.waitState(baby.StreamState_Alive, "state must become Alive")
	settled := h.req.count(client.Streaming_STARTED)
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, settled, h.req.count(client.Streaming_STARTED), "retries must stop once local flows")
	assert.Equal(t, int64(0), c.remote.Load())
}

// T4: STARTED acknowledged but no packets arrive -> retried after the backoff,
// with the request rate bounded by the configured intervals.
func TestHybridPool_AckWithoutPackets_RetriesWithBoundedRate(t *testing.T) {
	// Short packet wait so the backoff, not the wait, sets the gaps.
	mutate := func(c *HybridStreamPoolConfig) {
		c.PublisherWait = 30 * time.Millisecond
		c.MinRetryInterval = 20 * time.Millisecond
		c.InitialBackoff = 80 * time.Millisecond
		c.MaxBackoff = 240 * time.Millisecond
	}
	cfg := testConfig()
	mutate(&cfg)
	h := newHarness(t, mutate)
	h.start()
	h.waitStarted(2, "a second STARTED must follow when the acknowledged request brings no packets")

	time.Sleep(time.Second)
	reqs := h.req.all()
	require.GreaterOrEqual(t, len(reqs), 4)

	// Gap i follows failure i: InitialBackoff, then x1.5 up to MaxBackoff.
	const slack = 10 * time.Millisecond
	backoff := cfg.InitialBackoff
	for i := 1; i < len(reqs); i++ {
		assert.Equal(t, client.Streaming_STARTED, reqs[i].Status)
		gap := reqs[i].At.Sub(reqs[i-1].At)
		assert.GreaterOrEqual(t, int64(gap), int64(backoff-slack), "gap %d is %v, want at least %v", i, gap, backoff)
		backoff = time.Duration(float64(backoff) * 1.5)
		if backoff > cfg.MaxBackoff {
			backoff = cfg.MaxBackoff
		}
	}
	// Upper bound on the count: one first request, then at most one per InitialBackoff.
	elapsed := reqs[len(reqs)-1].At.Sub(reqs[0].At)
	assert.LessOrEqual(t, len(reqs), int(elapsed/cfg.InitialBackoff)+1, "request rate exceeds the configured bound")
}

// T5: local dies while remote flows -> filter to remote, StreamType_Remote,
// state Alive, local requested; local returns -> filter back to local.
func TestHybridPool_FailoverToRemoteAndBack(t *testing.T) {
	h := newHarness(t, nil)
	c := subscribe(t, h.b)
	h.local.start()
	time.Sleep(30 * time.Millisecond)
	h.start()
	require.NotNil(t, h.remote.send, "relay must be created with a packet receiver")
	h.remote.start()
	h.waitType(baby.StreamType_Local, "starts on local")
	h.waitPackets(c, true, "local forwarded")

	h.local.halt()
	h.waitType(baby.StreamType_Remote, "StreamType_Remote must be published when local dies and remote flows")
	h.waitPackets(c, false, "remote packets must reach the subscriber")
	assert.Equal(t, HybridSourceRemote, h.b.ActiveSourceID())
	s, _ := h.state()
	assert.Equal(t, baby.StreamState_Alive, s, "state stays Alive on remote")
	h.waitStarted(1, "a local request must be sent while on remote")

	h.local.start()
	h.waitType(baby.StreamType_Local, "StreamType_Local must be published when local returns")
	h.waitPackets(c, true, "local forwarded again")
	require.Eventually(t, func() bool { return h.b.ActiveSourceID() == HybridSourceLocal && !h.b.IsPendingSwitch() }, waitTimeout, 5*time.Millisecond)
}

func TestHybridPool_Close(t *testing.T) {
	h := newHarness(t, nil)
	h.local.start()
	time.Sleep(30 * time.Millisecond)
	h.start()
	require.Eventually(t, func() bool { return h.relay.started.Load() == 1 }, waitTimeout, 5*time.Millisecond, "relay started")
	time.Sleep(50 * time.Millisecond)

	stopWithin(t, func() { h.pool.Close() })
	stopWithin(t, func() { h.pool.Close() })
	assert.Equal(t, 0, len(h.req.all()), "Close must send nothing")
	assertStopped(t, h)
}

func stopWithin(t *testing.T, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { f(); close(done) }()
	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatal("stop call did not return")
	}
}

func assertStopped(t *testing.T, h *harness) {
	t.Helper()
	select {
	case <-h.pool.watchdogDone:
	case <-time.After(waitTimeout):
		t.Fatal("watchdog goroutine did not exit")
	}
	assert.GreaterOrEqual(t, h.relay.stopped.Load(), int32(1), "relay must be stopped")

	// Local dies after the stop: no tick may send anything.
	before := len(h.req.all())
	h.local.halt()
	time.Sleep(400 * time.Millisecond)
	assert.Equal(t, before, len(h.req.all()), "nothing may be sent after stop")
}

// A StreamState/StreamType written by someone else (the RTMP handler) is
// corrected on the next evaluation.
func TestHybridPool_ReconcilesOutsideStateWrites(t *testing.T) {
	h := newHarness(t, nil)
	h.local.start()
	time.Sleep(30 * time.Millisecond)
	h.start()
	h.waitState(baby.StreamState_Alive, "Alive")

	h.sm.Update(testBabyUID, *baby.NewState().SetStreamState(baby.StreamState_Unhealthy).SetStreamType(baby.StreamType_Remote))
	h.waitState(baby.StreamState_Alive, "outside Unhealthy write must be corrected")
	h.waitType(baby.StreamType_Local, "outside StreamType write must be corrected")
}

// --- review fixes ---

func longWatchdog(c *HybridStreamPoolConfig) { c.WatchdogInterval = time.Hour }

// goToRemote starts the pool with local flowing, then lets local die while
// remote flows, and waits until the filter is on remote.
func (h *harness) goToRemote() {
	h.local.start()
	time.Sleep(30 * time.Millisecond)
	h.start()
	h.remote.start()
	h.local.halt()
	h.waitType(baby.StreamType_Remote, "on remote")
	require.Eventually(h.t, func() bool { return h.b.ActiveSourceID() == HybridSourceRemote }, waitTimeout, 5*time.Millisecond)
}

// F1: Close while on remote puts the filter back on local, so local packets
// pass while no pool exists, and publishes Local/Unhealthy.
func TestHybridPool_CloseOnRemote_FilterBackToLocal(t *testing.T) {
	h := newHarness(t, nil)
	c := subscribe(t, h.b)
	h.goToRemote()

	h.remote.halt() // the relay stops with the pool
	stopWithin(t, h.pool.Close)

	assert.Equal(t, HybridSourceLocal, h.b.ActiveSourceID(), "filter must be on local after Close")
	assert.False(t, h.b.IsPendingSwitch())
	st, ty := h.state()
	assert.Equal(t, baby.StreamType_Local, ty)
	assert.Equal(t, baby.StreamState_Unhealthy, st, "local has no recent packets and the relay is stopped")

	h.local.start()
	h.waitPackets(c, true, "local packets must reach the subscriber after Close")
}

// F2: a flapping local stream does not reset the backoff; only a stable local
// stream (StableWindow) does. The exact growth is covered by the pacer tests
// with a fake clock; this test only checks the pool feeds the pacer correctly.
func TestHybridPool_FlappingLocal_BackoffGrows(t *testing.T) {
	mutate := func(c *HybridStreamPoolConfig) {
		c.HealthWindow = 30 * time.Millisecond
		c.WatchdogInterval = 5 * time.Millisecond
		c.MinRetryInterval = 10 * time.Millisecond
		c.InitialBackoff = 20 * time.Millisecond
		c.MaxBackoff = 80 * time.Millisecond
		c.StableWindow = 600 * time.Millisecond
		c.PublisherWait = 15 * time.Millisecond
		c.RequestTimeout = 50 * time.Millisecond
	}
	cfg := testConfig()
	mutate(&cfg)
	h := newHarness(t, mutate)
	h.local.start()
	time.Sleep(30 * time.Millisecond)
	h.start()

	// 4 s on / 6 s off, scaled to 80 ms / 120 ms, ten cycles.
	for i := 0; i < 10; i++ {
		h.local.halt()
		time.Sleep(120 * time.Millisecond)
		h.local.start()
		time.Sleep(80 * time.Millisecond)
	}
	assert.GreaterOrEqual(t, len(h.req.all()), 5)
	assert.Equal(t, cfg.MaxBackoff, h.pool.pacer.Backoff(), "flapping must grow the backoff to MaxBackoff, not reset it")

	// Local stable for longer than StableWindow: the backoff resets.
	require.Eventually(t, func() bool { return h.pool.pacer.Backoff() == 0 }, waitTimeout, 5*time.Millisecond,
		"backoff must reset after StableWindow of healthy local")
}

// F4: a blocking SendRequest does not stall evaluate or Close, and the
// in-flight request does nothing once the pool is closed.
func TestHybridPool_BlockingSend_DoesNotStallEvaluateOrClose(t *testing.T) {
	h := newHarness(t, longWatchdog)
	block := make(chan struct{})
	h.req.block = block
	released := false
	release := func() {
		if !released {
			released = true
			close(block)
		}
	}
	t.Cleanup(release)

	go h.pool.RequestLocalStream(context.Background())
	h.waitStarted(1, "request is being sent")

	evalDone := make(chan struct{})
	go func() { h.pool.evaluate(); close(evalDone) }()
	select {
	case <-evalDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("evaluate blocked by an in-flight SendRequest")
	}

	closeDone := make(chan struct{})
	go func() { h.pool.Close(); close(closeDone) }()
	select {
	case <-closeDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close blocked by an in-flight SendRequest")
	}

	active := h.b.ActiveSourceID()
	st, ty := h.state()
	h.local.start() // packets arrive as the request wakes up
	release()
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, active, h.b.ActiveSourceID(), "closed pool must not touch the filter")
	st2, ty2 := h.state()
	assert.Equal(t, st, st2, "closed pool must not publish state")
	assert.Equal(t, ty, ty2)
	assert.Equal(t, 1, len(h.req.all()))
}

// F6: an outside StreamState write is repaired at once, not at the next tick.
func TestHybridPool_OutsideWriteRepairedImmediately(t *testing.T) {
	h := newHarness(t, longWatchdog)
	h.start()
	h.remote.start()
	time.Sleep(50 * time.Millisecond)
	h.pool.evaluate()
	h.waitType(baby.StreamType_Remote, "on remote")
	h.waitState(baby.StreamState_Alive, "Alive on remote")

	h.sm.Update(testBabyUID, *baby.NewState().SetStreamState(baby.StreamState_Unhealthy))
	require.Eventually(t, func() bool { s, _ := h.state(); return s == baby.StreamState_Alive }, 500*time.Millisecond, 5*time.Millisecond,
		"outside Unhealthy must be repaired well inside one watchdog interval")
}

// F7: a failed request must not move the filter.
func TestHybridPool_FailedRequestKeepsFilterOnLocal(t *testing.T) {
	h := newHarness(t, longWatchdog)
	h.req.reply = func(r recordedRequest) (time.Duration, error) { return 0, errForbidden }
	h.start()
	h.waitStarted(1, "request sent")
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, HybridSourceLocal, h.b.ActiveSourceID())
}

// F7: pointing the filter back at the active source cancels a pending switch.
func TestHybridPool_EvaluateCancelsPendingSwitch(t *testing.T) {
	h := newHarness(t, longWatchdog)
	h.local.start()
	time.Sleep(30 * time.Millisecond)
	h.start()
	require.Equal(t, HybridSourceLocal, h.b.ActiveSourceID())

	// A subscription-triggered evaluate may run at any moment; either it or the
	// direct call below must cancel the pending switch.
	h.b.RequestSourceSwitch(HybridSourceRemote)
	h.pool.evaluate()
	assert.False(t, h.b.IsPendingSwitch(), "evaluate must cancel the pending switch while local is healthy")
	h.b.GetBroadcastFunc(HybridSourceRemote)(av.Packet{Type: av.H264, IsKeyFrame: true, Data: []byte{'R'}})
	assert.Equal(t, HybridSourceLocal, h.b.ActiveSourceID())
}

// F7: the switch to remote is keyframe-aligned.
func TestHybridPool_SwitchToRemoteWaitsForKeyframe(t *testing.T) {
	h := newHarness(t, longWatchdog)
	c := subscribe(t, h.b)
	h.local.start()
	time.Sleep(30 * time.Millisecond)
	h.start()
	h.local.halt()

	remote := func(kf bool) { h.remote.send(av.Packet{Type: av.H264, IsKeyFrame: kf, Data: []byte{'R'}}) }
	for end := time.Now().Add(200 * time.Millisecond); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		remote(false)
	}
	h.pool.evaluate()
	assert.True(t, h.b.IsPendingSwitch(), "switch to remote must wait for a keyframe")
	assert.Equal(t, HybridSourceRemote, h.b.PendingSourceID())
	assert.Equal(t, HybridSourceLocal, h.b.ActiveSourceID())

	remote(false)
	remote(true)
	remote(false)
	require.Eventually(t, func() bool { return c.remote.Load() >= 2 }, waitTimeout, 5*time.Millisecond)
	assert.True(t, c.firstRemoteKF.Load(), "first remote packet a subscriber gets must be a keyframe")
}

// --- second review ---

// G2: a STARTED whose send is reached after the pool closed is not sent.
func TestHybridPool_NoStartedAfterClose(t *testing.T) {
	h := newHarness(t, longWatchdog)
	h.pool.sendMu.Lock() // the request goroutine waits here, past its in-flight check
	h.start()
	time.Sleep(50 * time.Millisecond)
	stopWithin(t, h.pool.Close)
	h.pool.sendMu.Unlock()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, h.req.count(client.Streaming_STARTED), "no STARTED may be sent once the pool is closed")
}

// G6: a closed pool does not evaluate.
func TestHybridPool_ClosedPoolDoesNotEvaluate(t *testing.T) {
	h := newHarness(t, longWatchdog)
	h.start()
	stopWithin(t, h.pool.Close)
	st, ty := h.state()

	h.remote.start()
	time.Sleep(50 * time.Millisecond)
	h.pool.evaluate()
	assert.Equal(t, HybridSourceLocal, h.b.ActiveSourceID())
	assert.False(t, h.b.IsPendingSwitch(), "closed pool must not move the filter")
	st2, ty2 := h.state()
	assert.Equal(t, st, st2)
	assert.Equal(t, ty, ty2, "closed pool must not publish")
}

// G6: after a stall callback that comes before the health window expires, the
// pool evaluates again when it does.
func TestHybridPool_StallCallbackFollowUp(t *testing.T) {
	h := newHarness(t, func(c *HybridStreamPoolConfig) {
		c.WatchdogInterval = time.Hour
		c.HealthWindow = 300 * time.Millisecond
	})
	h.local.start()
	time.Sleep(30 * time.Millisecond)
	h.start()
	h.waitState(baby.StreamState_Alive, "Alive")
	h.local.halt()
	time.Sleep(100 * time.Millisecond)
	h.pool.onStallDetected(HybridSourceLocal) // local is still inside the window
	s, _ := h.state()
	require.Equal(t, baby.StreamState_Alive, s)
	require.Eventually(t, func() bool { s, _ := h.state(); return s == baby.StreamState_Unhealthy }, time.Second, 5*time.Millisecond,
		"follow-up evaluation must notice local left the window")
}
