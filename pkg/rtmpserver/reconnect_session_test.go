package rtmpserver

import (
	"bytes"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/notedit/rtmp/av"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// Session detection, keyframe gate and per-source header tests.
// ============================================================

func shift(pkts []av.Packet, d time.Duration) []av.Packet {
	out := make([]av.Packet, len(pkts))
	for i, p := range pkts {
		p.Time += d
		out[i] = p
	}
	return out
}

func videoOnly(pkts []av.Packet) []av.Packet {
	var out []av.Packet
	for _, p := range pkts {
		if p.Type == av.H264 {
			out = append(out, p)
		}
	}
	return out
}

func assertMonotonic(t *testing.T, got []av.Packet) {
	t.Helper()
	for i := 1; i < len(got); i++ {
		if got[i].Time < got[i-1].Time {
			t.Fatalf("timestamp went backwards at %s: %v -> %v", pktName(got[i]), got[i-1].Time, got[i].Time)
		}
	}
}

// --- P1: per-track restart detection ---

// Audio 1.5 s behind video on every packet must not count as a restart.
func TestSession_TrackOffsetOverOneSecondDoesNotRebase(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	var pkts []av.Packet
	for i := 0; time.Duration(i)*simVideoDelta < 10*time.Second; i++ {
		ts := time.Duration(i) * simVideoDelta
		pkts = append(pkts,
			av.Packet{Type: av.H264, Time: ts + 2*time.Second, IsKeyFrame: i%simGOP == 0, Data: []byte("v")},
			av.Packet{Type: av.AAC, Time: ts + 500*time.Millisecond, Data: []byte("a")})
	}
	feed(t, pb, c, pkts, SourceIDLocal, deadline)

	got := c.finish(t, deadline)
	require.NotEmpty(t, got)
	span := got[len(got)-1].Time - got[0].Time
	assert.True(t, span > 9*time.Second && span < 12*time.Second, "10 s of input must give about 10 s of output, got %v", span)
}

// --- P2: header-armed session detection ---

func testRestartWithHeaders(t *testing.T, s1dur, s2start time.Duration) {
	deadline := time.Now().Add(10 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	feed(t, pb, c, simHeaders("s1"), SourceIDLocal, deadline)
	feed(t, pb, c, simSession("s1", s1dur), SourceIDLocal, deadline)
	feed(t, pb, c, simHeaders("s2"), SourceIDLocal, deadline)
	feed(t, pb, c, shift(simSession("s2", 10*time.Second), s2start), SourceIDLocal, deadline)

	assertReconnectTimeline(t, c.finish(t, deadline))
}

func TestSession_ShortFirstSessionThenRestart(t *testing.T) {
	testRestartWithHeaders(t, 500*time.Millisecond, 0)
}

func TestSession_RestartAboveThePreviousTimestamps(t *testing.T) {
	testRestartWithHeaders(t, 5*time.Second, 5*time.Second+3*time.Hour)
}

// --- P3: keyframe gate ---

// A video config resent mid-stream with the same bytes and continuous
// timestamps changes nothing: no gate, no drops, no rebase.
func TestSession_IdenticalConfigResentMidStreamChangesNothing(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	full := simSession("s1", 4*time.Second)
	i := indexOf(full, "s1v31") // an inter frame
	feed(t, pb, c, simHeaders("s1"), SourceIDLocal, deadline)
	feed(t, pb, c, full[:i], SourceIDLocal, deadline)
	feed(t, pb, c, []av.Packet{{Type: av.H264DecoderConfig, Data: []byte("s1vcfg")}}, SourceIDLocal, deadline)
	feed(t, pb, c, full[i:], SourceIDLocal, deadline)

	got := c.finish(t, deadline)
	var media []av.Packet
	for _, p := range got {
		if p.Type <= 2 {
			media = append(media, p)
		}
	}
	assert.Equal(t, len(full), len(media), "no media packet may be dropped")
	v := videoOnly(media)
	for k := 1; k < len(v); k++ {
		assert.Equal(t, simVideoDelta, v[k].Time-v[k-1].Time, "video delta at %s", pktName(v[k]))
	}
}

// A changed video config with continuous timestamps holds video until the
// next keyframe; audio passes; there is no rebase.
func TestSession_ChangedConfigMidStreamHoldsVideoOnly(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	full := simSession("s1", 4*time.Second)
	i := indexOf(full, "s1v31")
	feed(t, pb, c, simHeaders("s1"), SourceIDLocal, deadline)
	feed(t, pb, c, full[:i], SourceIDLocal, deadline)
	feed(t, pb, c, []av.Packet{{Type: av.H264DecoderConfig, Data: []byte("s1vcfgB")}}, SourceIDLocal, deadline)
	feed(t, pb, c, full[i:], SourceIDLocal, deadline)

	got := c.finish(t, deadline)
	names := names(got)
	for k := 31; k < 45; k++ {
		assert.NotContains(t, names, "s1v"+strconv.Itoa(k), "inter frame before the next keyframe must be held")
	}
	cfg, key := indexOf(got, "s1vcfgB"), indexOf(got, "s1v45")
	require.True(t, cfg >= 0 && key > cfg)
	audioBetween := 0
	for _, p := range got[cfg:key] {
		if p.Type == av.AAC {
			audioBetween++
		}
	}
	assert.Greater(t, audioBetween, 30, "audio must pass while video is held")
	assert.Equal(t, 15*simVideoDelta, got[key].Time-got[indexOf(got, "s1v30")].Time, "no rebase for a continuous stream")
}

// After a new session the gate holds video only; audio of the new session passes.
func TestSession_NewSessionGateHoldsVideoOnly(t *testing.T) {
	deadline := time.Now().Add(5 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	feed(t, pb, c, simHeaders("s1"), SourceIDLocal, deadline)
	feed(t, pb, c, simSession("s1", 2*time.Second), SourceIDLocal, deadline)
	feed(t, pb, c, simHeaders("s2"), SourceIDLocal, deadline)
	feed(t, pb, c, []av.Packet{
		{Type: av.H264, Time: 0, Data: []byte("lead")},
		{Type: av.AAC, Time: 0, Data: []byte("s2a0")},
		{Type: av.AAC, Time: 23 * time.Millisecond, Data: []byte("s2a1")},
		{Type: av.H264, Time: 66 * time.Millisecond, IsKeyFrame: true, Data: []byte("s2key")},
	}, SourceIDLocal, deadline)

	got := c.finish(t, deadline)
	assertMonotonic(t, got)
	assert.Equal(t, -1, indexOf(got, "lead"))
	a0, a1, key := indexOf(got, "s2a0"), indexOf(got, "s2a1"), indexOf(got, "s2key")
	assert.True(t, a0 >= 0 && a1 > a0 && key > a1, "audio must pass before the keyframe: %v", names(got[len(got)-6:]))
}

// Packets held by the gate still count as activity for stall detection.
func TestSession_GatedPacketsCountForStallDetection(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	var stalls atomic.Int32
	pb.SetStallConfig(&StallConfig{Threshold: 1500 * time.Millisecond, Callback: func(int) { stalls.Add(1) }})
	c := collect(pb.Subscribe())

	feed(t, pb, c, simHeaders("s1"), SourceIDLocal, deadline)
	feed(t, pb, c, simSession("s1", time.Second), SourceIDLocal, deadline)
	feed(t, pb, c, simHeaders("s2"), SourceIDLocal, deadline)
	// The new session sends only inter frames for 3 s of wall time.
	end := time.Now().Add(3 * time.Second)
	for i := 0; time.Now().Before(end); i++ {
		pb.Broadcast(av.Packet{Type: av.H264, Time: time.Duration(i) * 50 * time.Millisecond, Data: []byte("inter")}, SourceIDLocal)
		time.Sleep(50 * time.Millisecond)
	}
	c.finish(t, deadline)
	assert.Zero(t, stalls.Load(), "stall callback fired while packets were arriving")
}

// The gate opens by itself if no keyframe arrives within the limit.
func TestSession_GateOpensAfterTimeout(t *testing.T) {
	old := keyframeGateTimeout
	keyframeGateTimeout = 300 * time.Millisecond
	defer func() { keyframeGateTimeout = old }()

	deadline := time.Now().Add(5 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	feed(t, pb, c, simHeaders("s1"), SourceIDLocal, deadline)
	feed(t, pb, c, simSession("s1", time.Second), SourceIDLocal, deadline)
	feed(t, pb, c, simHeaders("s2"), SourceIDLocal, deadline)
	end := time.Now().Add(700 * time.Millisecond)
	for i := 0; time.Now().Before(end); i++ {
		pb.Broadcast(av.Packet{Type: av.H264, Time: time.Duration(i) * 20 * time.Millisecond, Data: []byte("inter")}, SourceIDLocal)
		time.Sleep(20 * time.Millisecond)
	}
	got := names(c.finish(t, deadline))
	assert.Contains(t, got, "inter", "video must flow again after the gate timeout")
}

// --- P4: per-source headers ---

const (
	srcL = 1
	srcR = 21
)

func (c *collector) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return names(c.pkts)
}

// A hot standby's headers arrive long before the switch; they are stored but
// not forwarded, and are delivered at the switch before the keyframe.
func TestSession_StandbyHeadersDeliveredAtSwitch(t *testing.T) {
	deadline := time.Now().Add(5 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	pb.SetActiveSource(srcL)
	c := collect(pb.Subscribe())

	feed(t, pb, c, simHeaders("L"), srcL, deadline)
	feed(t, pb, c, simSession("L", time.Second), srcL, deadline)
	feed(t, pb, c, simHeaders("R"), srcR, deadline)
	feed(t, pb, c, simSession("R", time.Second), srcR, deadline)
	assert.NotContains(t, c.snapshot(), "Rvcfg", "standby headers must not be forwarded")

	pb.RequestSourceSwitch(srcR)
	pb.Broadcast(av.Packet{Type: av.H264, Time: 1100 * time.Millisecond, Data: []byte("Rinter")}, srcR)
	pb.Broadcast(av.Packet{Type: av.H264, Time: 1166 * time.Millisecond, IsKeyFrame: true, Data: []byte("Rkey")}, srcR)

	got := names(c.finish(t, deadline))
	k := indexOf(c.pkts, "Rkey")
	require.GreaterOrEqual(t, k, 3)
	assert.Equal(t, []string{"Rmeta", "Rvcfg", "Racfg", "Rkey"}, got[k-3:k+1])
}

// A subscriber that joins during a pending switch gets the active source's headers.
func TestSession_SubscriberDuringPendingSwitchGetsActiveHeaders(t *testing.T) {
	deadline := time.Now().Add(5 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	pb.SetActiveSource(srcL)

	feed(t, pb, nil, simHeaders("L"), srcL, deadline)
	feed(t, pb, nil, simSession("L", time.Second), srcL, deadline)
	pb.RequestSourceSwitch(srcR)
	feed(t, pb, nil, simHeaders("R"), srcR, deadline)

	fresh := collect(pb.Subscribe())
	assert.Equal(t, []string{"Lmeta", "Lvcfg", "Lacfg"}, names(fresh.finish(t, deadline)))
}

// A cancelled pending switch leaves no state behind.
func TestSession_CancelledSwitchLeavesNoState(t *testing.T) {
	cancels := map[string]func(pb *PersistentBroadcaster){
		"RequestSourceSwitch(active)": func(pb *PersistentBroadcaster) { pb.RequestSourceSwitch(srcL) },
		"SetActiveSource(active)":     func(pb *PersistentBroadcaster) { pb.SetActiveSource(srcL) },
	}
	for name, cancel := range cancels {
		t.Run(name, func(t *testing.T) {
			deadline := time.Now().Add(5 * time.Second)
			pb := NewPersistentBroadcaster("baby123")
			defer pb.Close()
			pb.SetActiveSource(srcL)
			c := collect(pb.Subscribe())

			feed(t, pb, c, simHeaders("L"), srcL, deadline)
			feed(t, pb, c, simSession("L", time.Second), srcL, deadline)
			pb.RequestSourceSwitch(srcR)
			feed(t, pb, c, simHeaders("R"), srcR, deadline)
			cancel(pb)
			assert.False(t, pb.IsPendingSwitch())
			pb.Broadcast(av.Packet{Type: av.H264, Time: 5 * time.Second, IsKeyFrame: true, Data: []byte("Rkey")}, srcR)
			pb.Broadcast(av.Packet{Type: av.H264, Time: 1033 * time.Millisecond, IsKeyFrame: true, Data: []byte("Lkey")}, srcL)
			assert.Equal(t, srcL, pb.ActiveSourceID())

			fresh := collect(pb.Subscribe())
			assert.Equal(t, []string{"Lmeta", "Lvcfg", "Lacfg"}, names(fresh.finish(t, deadline)))
			got := names(c.finish(t, deadline))
			for _, n := range []string{"Rmeta", "Rvcfg", "Racfg", "Rkey"} {
				assert.NotContains(t, got, n)
			}
			assert.Contains(t, got, "Lkey")
		})
	}
}

// SetActiveSource (immediate) sends the new source's stored headers to
// connected subscribers, then waits for that source's keyframe.
func TestSession_ImmediateSwitchSendsStoredHeaders(t *testing.T) {
	deadline := time.Now().Add(5 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	pb.SetActiveSource(srcL)
	c := collect(pb.Subscribe())

	feed(t, pb, c, simHeaders("L"), srcL, deadline)
	feed(t, pb, c, simSession("L", time.Second), srcL, deadline)
	feed(t, pb, c, simHeaders("R"), srcR, deadline)
	pb.SetActiveSource(srcR)
	pb.Broadcast(av.Packet{Type: av.H264, Time: 3 * time.Second, Data: []byte("Rinter")}, srcR)
	pb.Broadcast(av.Packet{Type: av.H264, Time: 3066 * time.Millisecond, IsKeyFrame: true, Data: []byte("Rkey")}, srcR)

	got := names(c.finish(t, deadline))
	k := indexOf(c.pkts, "Rkey")
	require.GreaterOrEqual(t, k, 3)
	assert.Equal(t, []string{"Rmeta", "Rvcfg", "Racfg", "Rkey"}, got[k-3:k+1])
	assert.NotContains(t, got, "Rinter")
}

// A source that flaps before any media does not grow the stored list.
func TestSession_FlappingBeforeMediaDoesNotGrowHeaders(t *testing.T) {
	deadline := time.Now().Add(5 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	for _, tag := range []string{"a", "b", "c", "d", "e"} {
		feed(t, pb, nil, simHeaders(tag), SourceIDLocal, deadline)
	}
	fresh := collect(pb.Subscribe())
	assert.Equal(t, []string{"emeta", "evcfg", "eacfg"}, names(fresh.finish(t, deadline)))
}

// --- P5: forwarded headers ---

// A forwarded header carries the current output timestamp; stored copies keep
// their own time.
func TestSession_ForwardedHeaderUsesOutputTimestamp(t *testing.T) {
	deadline := time.Now().Add(5 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	feed(t, pb, c, simHeaders("s1"), SourceIDLocal, deadline)
	feed(t, pb, c, simSession("s1", 2*time.Second), SourceIDLocal, deadline)
	feed(t, pb, c, simHeaders("s2"), SourceIDLocal, deadline)
	feed(t, pb, c, simSession("s2", time.Second), SourceIDLocal, deadline)

	fresh := collect(pb.Subscribe())
	for _, p := range fresh.finish(t, deadline) {
		assert.Zero(t, p.Time, "stored header %s keeps its own time", pktName(p))
	}
	assertMonotonic(t, c.finish(t, deadline))
}

// A header that cannot be delivered to a full subscriber is logged.
func TestSession_HeaderDroppedForFullSubscriberIsLogged(t *testing.T) {
	oldLogger := log.Logger
	var buf syncBuffer
	log.Logger = zerolog.New(&buf)
	defer func() { log.Logger = oldLogger }()

	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	stuck := pb.Subscribe() // never read
	require.NotNil(t, stuck)
	for i := 0; i < 600; i++ {
		pb.Broadcast(av.Packet{Type: av.H264, Time: time.Duration(i) * simVideoDelta, IsKeyFrame: i == 0}, SourceIDLocal)
	}
	pb.Broadcast(av.Packet{Type: av.H264DecoderConfig, Data: []byte("vcfg")}, SourceIDLocal)
	assert.Contains(t, buf.String(), "cannot decode")
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// --- Lock order stress ---

// Media into a full subscriber from one goroutine and headers from another
// must not deadlock.
func TestSession_NoDeadlockMediaAndHeaders(t *testing.T) {
	oldLogger := log.Logger
	log.Logger = zerolog.Nop()
	defer func() { log.Logger = oldLogger }()

	pb := NewPersistentBroadcaster("baby123") // no deferred Close: it would hide a deadlock
	require.NotNil(t, pb.Subscribe())         // never read

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 3000; i++ {
			pb.Broadcast(av.Packet{Type: av.H264, Time: time.Duration(i) * time.Millisecond, IsKeyFrame: i%15 == 0}, SourceIDLocal)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 3000; i++ {
			pb.Broadcast(av.Packet{Type: av.H264DecoderConfig, Data: []byte{byte(i % 2)}}, SourceIDLocal)
			pb.Broadcast(av.Packet{Type: av.AACDecoderConfig, Data: []byte{byte(i)}}, srcR)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 3000; i++ {
			if i%2 == 0 {
				pb.SetActiveSource(srcR)
			} else {
				pb.RequestSourceSwitch(SourceIDLocal)
			}
			if sub := pb.Subscribe(); sub != nil {
				sub.Unsubscribe()
			}
		}
	}()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		pb.Close()
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: workers did not finish within 10s")
	}
}

// --- P6: per-packet drop for an evicted publisher ---

// --- B1: one tsMu hold per packet ---

// Two live sources and a switch: a packet of the old source must never be
// remapped with the new source's offset. Bounded stress through the public API.
func TestSession_NoTimelineJumpDuringSwitchStress(t *testing.T) {
	oldLogger := log.Logger
	log.Logger = zerolog.Nop()
	defer func() { log.Logger = oldLogger }()

	switches := map[string]func(pb *PersistentBroadcaster){
		"RequestSourceSwitch": func(pb *PersistentBroadcaster) { pb.RequestSourceSwitch(2) },
		"SetActiveSource":     func(pb *PersistentBroadcaster) { pb.SetActiveSource(2) },
	}
	for name, doSwitch := range switches {
		t.Run(name, func(t *testing.T) {
			deadline := time.Now().Add(20 * time.Second)
			for trial := 0; trial < 1000 && time.Now().Before(deadline); trial++ {
				pb := NewPersistentBroadcaster("b")
				pb.SetActiveSource(10)
				c := collect(pb.Subscribe())
				// 240 packets per source: never more than the 500-packet buffer,
				// so no drop can fake a gap
				var wg sync.WaitGroup
				var sentOld atomic.Int32
				for _, src := range []int{10, 2} {
					base := time.Duration(0)
					if src == 10 {
						base = 3 * time.Hour
					}
					wg.Add(1)
					go func(src int, base time.Duration) {
						defer wg.Done()
						for i := 0; i < 240; i++ {
							pb.Broadcast(av.Packet{Type: av.H264, Time: base + time.Duration(i)*simVideoDelta, IsKeyFrame: src == 2 || i%15 == 0}, src)
							if src == 10 {
								sentOld.Add(1)
							}
						}
					}(src, base)
				}
				for sentOld.Load() < 60 && time.Now().Before(deadline) {
					runtime.Gosched()
				}
				doSwitch(pb) // while both sources are still sending
				wg.Wait()
				got := c.finish(t, deadline)
				pb.Close()

				run := 0
				for i := 1; i < len(got); i++ {
					d := got[i].Time - got[i-1].Time
					if d > 5*time.Second || d < 0 {
						t.Fatalf("trial %d: output step %v at packet %d", trial, d, i)
					}
					if d == time.Millisecond {
						run++
						if run > 20 {
							t.Fatalf("trial %d: long run of +1 ms clamped packets at %d", trial, i)
						}
					} else {
						run = 0
					}
				}
			}
		})
	}
}

// --- R1: identical resent headers are not forwarded ---

func TestSession_IdenticalConfigBeforeEveryKeyframeIsNotForwarded(t *testing.T) {
	deadline := time.Now().Add(20 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	pkts := simHeaders("s1")
	for _, p := range simSession("s1", 60*time.Second) {
		if p.Type == av.H264 && p.IsKeyFrame && p.Time > 0 {
			pkts = append(pkts, av.Packet{Type: av.H264DecoderConfig, Data: []byte("s1vcfg")})
		}
		pkts = append(pkts, p)
	}
	feed(t, pb, c, pkts, SourceIDLocal, deadline)

	got := c.finish(t, deadline)
	headers := 0
	for _, p := range got {
		if p.Type > av.OPUS {
			headers++
		}
	}
	assert.Equal(t, 3, headers, "only the first header set may be forwarded")
	assert.Equal(t, len(simSession("s1", 60*time.Second)), len(got)-headers, "no media may be dropped")
}

// --- R2: gate on an ambiguous restart ---

// Identical config, then a camera stall (forward jump > 1 s) decided by an
// inter frame: rebase, but no gate.
func TestSession_IdenticalConfigThenStallRebasesWithoutGate(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	full := simSession("s1", 8*time.Second)
	i := indexOf(full, "s1v31") // inter frame
	feed(t, pb, c, simHeaders("s1"), SourceIDLocal, deadline)
	feed(t, pb, c, full[:i], SourceIDLocal, deadline)
	feed(t, pb, c, []av.Packet{{Type: av.H264DecoderConfig, Data: []byte("s1vcfg")}}, SourceIDLocal, deadline)
	feed(t, pb, c, shift(full[i:], 3*time.Second), SourceIDLocal, deadline)

	got := c.finish(t, deadline)
	assertMonotonic(t, got)
	v30, v31 := indexOf(got, "s1v30"), indexOf(got, "s1v31")
	require.True(t, v30 >= 0 && v31 > v30, "the inter frame after the stall must not be held")
	assert.Less(t, int64(got[v31].Time-got[v30].Time), int64(200*time.Millisecond), "forward jump after a config must be rebased")
}

// Identical config bytes, then a real reconnect whose first video packet is
// an inter frame: rebase and hold video until the keyframe.
func TestSession_IdenticalConfigReconnectHoldsInterFrames(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	feed(t, pb, c, simHeaders("s1"), SourceIDLocal, deadline)
	feed(t, pb, c, simSession("s1", 5*time.Second), SourceIDLocal, deadline)
	feed(t, pb, c, simHeaders("s1"), SourceIDLocal, deadline) // same bytes
	lead := []av.Packet{{Type: av.H264, Time: 0, Data: []byte("lead")}}
	feed(t, pb, c, append(lead, simSession("s2", 10*time.Second)...), SourceIDLocal, deadline)

	got := c.finish(t, deadline)
	assert.Equal(t, -1, indexOf(got, "lead"), "inter frame before the new session's keyframe must be held")
	assertReconnectTimeline(t, got)
}

// --- R3: audio config marks a possible new session ---

func TestSession_AudioFirstRestartAboveThePreviousTimestamps(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	feed(t, pb, c, simHeaders("s1"), SourceIDLocal, deadline)
	feed(t, pb, c, simSession("s1", 5*time.Second), SourceIDLocal, deadline)

	start := 5*time.Second + 3*time.Hour
	s2 := shift(simSession("s2", 5*time.Second), start)
	var early, rest []av.Packet
	for _, p := range s2 {
		if p.Type == av.AAC && p.Time < start+300*time.Millisecond {
			early = append(early, p)
		} else {
			rest = append(rest, p)
		}
	}
	feed(t, pb, c, []av.Packet{{Type: av.AACDecoderConfig, Data: []byte("s2acfg")}}, SourceIDLocal, deadline)
	feed(t, pb, c, early, SourceIDLocal, deadline)
	feed(t, pb, c, []av.Packet{{Type: av.H264DecoderConfig, Data: []byte("s2vcfg")}}, SourceIDLocal, deadline)
	feed(t, pb, c, rest, SourceIDLocal, deadline)

	got := c.finish(t, deadline)
	assertMonotonic(t, got)
	for i := 1; i < len(got); i++ {
		if d := got[i].Time - got[i-1].Time; d > time.Second {
			t.Fatalf("output gap %v at %s", d, pktName(got[i]))
		}
	}
	// Once caught up, video and audio keep their real relation (same offset).
	v, a := got[indexOf(got, "s2v30")], got[indexOf(got, "s2a100")]
	in := map[string]time.Duration{}
	for _, p := range s2 {
		in[pktName(p)] = p.Time
	}
	// The monotonic clamp moves late audio by up to simAudioLag+1ms
	assert.InDelta(t, float64(v.Time-in["s2v30"]), float64(a.Time-in["s2a100"]), float64(simAudioLag+time.Millisecond),
		"video and audio must share one offset")
}

// --- OPUS is media ---

func TestSession_OpusIsMediaNotHeader(t *testing.T) {
	deadline := time.Now().Add(5 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	var pkts []av.Packet
	for i := 0; i < 50; i++ {
		pkts = append(pkts, av.Packet{Type: av.OPUS, Time: time.Duration(i) * 20 * time.Millisecond, Data: []byte("opus" + strconv.Itoa(i))})
	}
	feed(t, pb, c, pkts, SourceIDLocal, deadline)

	fresh := collect(pb.Subscribe())
	assert.Empty(t, names(fresh.finish(t, deadline)), "OPUS packets must not be stored as headers")
	got := c.finish(t, deadline)
	require.Len(t, got, 50)
	for i := 1; i < len(got); i++ {
		assert.Equal(t, 20*time.Millisecond, got[i].Time-got[i-1].Time, "OPUS keeps its timing")
	}
}
