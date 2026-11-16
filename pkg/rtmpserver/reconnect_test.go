package rtmpserver

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/notedit/rtmp/av"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// Publisher reconnect tests: a source that restarts its session
// (timestamps restart, new headers) and two publishers on one key.
// ============================================================

const (
	simVideoDelta = 66 * time.Millisecond // 15 fps
	simAudioDelta = 23 * time.Millisecond // AAC 1024 samples @ 44.1 kHz
	simAudioLag   = 10 * time.Millisecond // audio is muxed slightly late
	simGOP        = 15
)

// simSession returns the media packets of one publisher session that starts at
// timestamp 0: video every 66 ms (keyframe every 15 frames) and audio every
// 23 ms, interleaved the way a muxer emits them, so that small backward steps
// between audio and video timestamps occur. Data is "<tag>v<n>" or "<tag>a<n>".
func simSession(tag string, dur time.Duration) []av.Packet {
	type ev struct {
		emit time.Duration
		pkt  av.Packet
	}
	var evs []ev
	for i := 0; time.Duration(i)*simVideoDelta < dur; i++ {
		ts := time.Duration(i) * simVideoDelta
		evs = append(evs, ev{ts, av.Packet{Type: av.H264, Time: ts, IsKeyFrame: i%simGOP == 0, Data: []byte(fmt.Sprintf("%sv%d", tag, i))}})
	}
	for i := 0; time.Duration(i)*simAudioDelta < dur; i++ {
		ts := time.Duration(i) * simAudioDelta
		evs = append(evs, ev{ts + simAudioLag, av.Packet{Type: av.AAC, Time: ts, Data: []byte(fmt.Sprintf("%sa%d", tag, i))}})
	}
	sort.SliceStable(evs, func(i, j int) bool { return evs[i].emit < evs[j].emit })
	out := make([]av.Packet, len(evs))
	for i, e := range evs {
		out[i] = e.pkt
	}
	return out
}

func simHeaders(tag string) []av.Packet {
	return []av.Packet{
		{Type: av.Metadata, Data: []byte(tag + "meta")},
		{Type: av.H264DecoderConfig, Data: []byte(tag + "vcfg")},
		{Type: av.AACDecoderConfig, Data: []byte(tag + "acfg")},
	}
}

// collector drains a subscription in the background so that the 500-packet
// buffer never overflows.
type collector struct {
	sub  *Subscription
	mu   sync.Mutex
	pkts []av.Packet
	done chan struct{}
}

func collect(sub *Subscription) *collector {
	c := &collector{sub: sub, done: make(chan struct{})}
	go func() {
		defer close(c.done)
		for pkt := range sub.Packets() {
			c.mu.Lock()
			c.pkts = append(c.pkts, pkt)
			c.mu.Unlock()
		}
	}()
	return c
}

// waitDrained waits until the collector has emptied the subscription buffer.
func (c *collector) waitDrained(t *testing.T, deadline time.Time) {
	t.Helper()
	for len(c.sub.pktChan) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("deadline exceeded waiting for subscriber to drain")
		}
		time.Sleep(time.Millisecond)
	}
}

// finish unsubscribes and returns everything that was received.
func (c *collector) finish(t *testing.T, deadline time.Time) []av.Packet {
	t.Helper()
	c.sub.Unsubscribe()
	select {
	case <-c.done:
	case <-time.After(time.Until(deadline)):
		t.Fatal("deadline exceeded waiting for collector")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]av.Packet(nil), c.pkts...)
}

func feed(t *testing.T, pb *PersistentBroadcaster, c *collector, pkts []av.Packet, sourceID int, deadline time.Time) {
	t.Helper()
	for i, p := range pkts {
		pb.Broadcast(p, sourceID)
		if c != nil && i%100 == 99 {
			c.waitDrained(t, deadline)
		}
	}
	if c != nil {
		c.waitDrained(t, deadline)
	}
}

// pktName is the packet's marker: its data without trailing padding.
func pktName(p av.Packet) string {
	return string(bytes.TrimRight(p.Data, "\x00"))
}

func names(pkts []av.Packet) []string {
	out := make([]string, len(pkts))
	for i, p := range pkts {
		out[i] = pktName(p)
	}
	return out
}

func indexOf(pkts []av.Packet, data string) int {
	for i, p := range pkts {
		if pktName(p) == data {
			return i
		}
	}
	return -1
}

// assertReconnectTimeline checks the received media after a restart: global
// non-decreasing timestamps, continuation from the last output timestamp,
// and video advancing at the source rate after the restart.
func assertReconnectTimeline(t *testing.T, got []av.Packet) {
	t.Helper()
	var media []av.Packet
	for _, p := range got {
		if p.Type <= 2 {
			media = append(media, p)
		}
	}
	for i := 1; i < len(media); i++ {
		if media[i].Time < media[i-1].Time {
			t.Fatalf("timestamp went backwards at %s: %v -> %v", media[i].Data, media[i-1].Time, media[i].Time)
		}
	}

	first2 := indexOf(media, "s2v0")
	require.Greater(t, first2, 0, "session 2 first video packet must be received")
	jump := media[first2].Time - media[first2-1].Time
	assert.True(t, jump >= 0 && jump < 200*time.Millisecond,
		"session 2 must continue from the last output timestamp, got jump %v", jump)

	var s2video []av.Packet
	for _, p := range media[first2:] {
		if strings.HasPrefix(string(p.Data), "s2v") {
			s2video = append(s2video, p)
		}
	}
	require.Greater(t, len(s2video), 100)
	bad := 0
	for i := 1; i < len(s2video); i++ {
		if d := s2video[i].Time - s2video[i-1].Time; d != simVideoDelta {
			if bad < 3 {
				t.Errorf("video delta after restart at %s = %v, want %v", s2video[i].Data, d, simVideoDelta)
			}
			bad++
		}
	}
	assert.Zero(t, bad, "video deltas after restart that differ from the source delta")
}

func testReconnectTimeline(t *testing.T, sourceID int) {
	deadline := time.Now().Add(10 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	feed(t, pb, c, simSession("s1", 30*time.Second), sourceID, deadline)
	feed(t, pb, c, simSession("s2", 10*time.Second), sourceID, deadline)

	assertReconnectTimeline(t, c.finish(t, deadline))
}

// T1
func TestReconnect_LocalSourceRestartKeepsTimeline(t *testing.T) {
	testReconnectTimeline(t, SourceIDLocal)
}

// T2: the cloud relay reconnects through the same broadcast func, with no
// explicit session signal.
func TestReconnect_RelaySourceRestartKeepsTimeline(t *testing.T) {
	testReconnectTimeline(t, 21)
}

// T1/T2 with new-session headers in front of the second session, as a real
// RTMP publisher sends them.
func TestReconnect_RestartWithHeadersKeepsTimeline(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	feed(t, pb, c, simHeaders("s1"), SourceIDLocal, deadline)
	feed(t, pb, c, simSession("s1", 30*time.Second), SourceIDLocal, deadline)
	feed(t, pb, c, simHeaders("s2"), SourceIDLocal, deadline)
	feed(t, pb, c, simSession("s2", 10*time.Second), SourceIDLocal, deadline)

	assertReconnectTimeline(t, c.finish(t, deadline))
}

// T3a: interleave jitter and a 500 ms backward step must not rebase.
func TestReconnect_SmallBackwardStepsDoNotRebase(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	pkts := simSession("s1", 10*time.Second)
	// One late audio packet 500 ms behind the stream.
	late := av.Packet{Type: av.AAC, Time: 4500 * time.Millisecond, Data: []byte("late")}
	i := indexOf(pkts, "s1v76") // ts 5016 ms
	pkts = append(pkts[:i+1], append([]av.Packet{late}, pkts[i+1:]...)...)
	feed(t, pb, c, pkts, SourceIDLocal, deadline)

	got := c.finish(t, deadline)
	var video []av.Packet
	for _, p := range got {
		if p.Type == av.H264 {
			video = append(video, p)
		}
	}
	require.Len(t, video, 152)
	for i := 1; i < len(video); i++ {
		assert.Equal(t, simVideoDelta, video[i].Time-video[i-1].Time, "video delta at %s", video[i].Data)
	}
	for i := 1; i < len(got); i++ {
		assert.True(t, got[i].Time >= got[i-1].Time, "monotonic at %s", got[i].Data)
	}
}

// T3b: a large forward jump on the same source (camera stalled, then resumed)
// is preserved: the gap is real elapsed time, and audio and video keep their
// relative timing. Output stays monotonic.
func TestReconnect_LargeForwardJumpIsPreserved(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	s1 := simSession("s1", 5*time.Second)
	s2 := simSession("s2", 5*time.Second)
	for i := range s2 {
		s2[i].Time += 15 * time.Second
	}
	feed(t, pb, c, s1, SourceIDLocal, deadline)
	feed(t, pb, c, s2, SourceIDLocal, deadline)

	got := c.finish(t, deadline)
	for i := 1; i < len(got); i++ {
		require.True(t, got[i].Time >= got[i-1].Time, "monotonic at %s", got[i].Data)
	}
	last1 := indexOf(got, "s1v75")
	first2 := indexOf(got, "s2v0")
	require.True(t, last1 >= 0 && first2 > last1)
	assert.Equal(t, 15*time.Second-75*simVideoDelta, got[first2].Time-got[last1].Time,
		"forward jump must be passed through")
}

// T4: new-session headers replace the stored ones, reach connected
// subscribers before the session's media, and media waits for a keyframe.
func TestReconnect_NewSessionHeadersReplaceAndForward(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	for k := 0; k <= 5; k++ {
		tag := fmt.Sprintf("k%d", k)
		feed(t, pb, c, simHeaders(tag), SourceIDLocal, deadline)
		media := simSession(tag, 2*time.Second)
		if k > 0 {
			// The new session starts with inter frames before its first keyframe.
			lead := []av.Packet{
				{Type: av.H264, Time: 0, Data: []byte(tag + "lead0")},
				{Type: av.H264, Time: 0, Data: []byte(tag + "lead1")},
			}
			media = append(lead, media...)
		}
		feed(t, pb, c, media, SourceIDLocal, deadline)
	}

	fresh := collect(pb.Subscribe())
	freshGot := fresh.finish(t, deadline)
	assert.Equal(t, []string{"k5meta", "k5vcfg", "k5acfg"}, names(freshGot))

	got := names(c.finish(t, deadline))
	for k := 1; k <= 5; k++ {
		tag := fmt.Sprintf("k%d", k)
		meta, vcfg, acfg := indexOf(c.pkts, tag+"meta"), indexOf(c.pkts, tag+"vcfg"), indexOf(c.pkts, tag+"acfg")
		key := indexOf(c.pkts, tag+"v0")
		if !assert.True(t, meta >= 0 && vcfg > meta && acfg > vcfg, "session %d headers must reach the connected subscriber in order", k) {
			continue
		}
		assert.Greater(t, key, acfg, "session %d keyframe must come after its headers", k)
		// Nothing from this session may come before its first keyframe.
		for i := acfg + 1; i < key; i++ {
			if strings.HasPrefix(got[i], tag) {
				t.Errorf("session %d: %s forwarded before the first keyframe", k, got[i])
			}
		}
		assert.Equal(t, -1, indexOf(c.pkts, tag+"lead0"), "inter frames before the keyframe must be dropped")
	}
}

// A lone metadata packet mid-stream replaces only the stored metadata; it
// must not discard the decoder configuration or stall the media.
func TestReconnect_MidStreamMetadataKeepsDecoderConfig(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	feed(t, pb, c, simHeaders("s1"), SourceIDLocal, deadline)
	feed(t, pb, c, simSession("s1", 2*time.Second), SourceIDLocal, deadline)
	feed(t, pb, c, []av.Packet{
		{Type: av.Metadata, Data: []byte("meta2")},
		{Type: av.H264, Time: 2100 * time.Millisecond, Data: []byte("after")},
	}, SourceIDLocal, deadline)

	fresh := collect(pb.Subscribe())
	assert.ElementsMatch(t, []string{"meta2", "s1vcfg", "s1acfg"}, names(fresh.finish(t, deadline)))
	got := c.finish(t, deadline)
	assert.Greater(t, indexOf(got, "after"), indexOf(got, "meta2"), "media after mid-stream metadata must be forwarded")
}

// A config that arrives after media (here the audio config after the first
// video frame) is added; it must not discard the stored video config or
// stall a stream that never reconnects.
func TestReconnect_LateAudioConfigKeepsVideoConfig(t *testing.T) {
	deadline := time.Now().Add(5 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	feed(t, pb, c, []av.Packet{
		{Type: av.Metadata, Data: []byte("meta")},
		{Type: av.H264DecoderConfig, Data: []byte("vcfg")},
		{Type: av.H264, Time: 0, IsKeyFrame: true, Data: []byte("v0")},
		{Type: av.AACDecoderConfig, Data: []byte("acfg")},
		{Type: av.H264, Time: 66 * time.Millisecond, Data: []byte("v1")},
	}, SourceIDLocal, deadline)

	fresh := collect(pb.Subscribe())
	assert.ElementsMatch(t, []string{"meta", "vcfg", "acfg"}, names(fresh.finish(t, deadline)))
	assert.Equal(t, []string{"meta", "vcfg", "v0", "acfg", "v1"}, names(c.finish(t, deadline)))
}

// An encoder that resends its video config before keyframes replaces only the
// stored video config; the audio config stays.
func TestReconnect_ResentVideoConfigKeepsAudioConfig(t *testing.T) {
	deadline := time.Now().Add(5 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	feed(t, pb, c, simHeaders("s1"), SourceIDLocal, deadline)
	feed(t, pb, c, []av.Packet{
		{Type: av.H264, Time: 0, IsKeyFrame: true, Data: []byte("v0")},
		{Type: av.H264DecoderConfig, Data: []byte("vcfg2")},
		{Type: av.H264, Time: 66 * time.Millisecond, IsKeyFrame: true, Data: []byte("v1")},
	}, SourceIDLocal, deadline)

	fresh := collect(pb.Subscribe())
	assert.ElementsMatch(t, []string{"s1meta", "s1acfg", "vcfg2"}, names(fresh.finish(t, deadline)))
	assert.Equal(t, []string{"s1meta", "s1vcfg", "s1acfg", "v0", "vcfg2", "v1"}, names(c.finish(t, deadline)))
}

// T5: a subscriber that connected before any header existed gets the headers
// when they arrive, then media from the keyframe.
func TestReconnect_EarlySubscriberGetsFirstHeaders(t *testing.T) {
	deadline := time.Now().Add(5 * time.Second)
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	c := collect(pb.Subscribe())

	feed(t, pb, c, simHeaders("s1"), SourceIDLocal, deadline)
	feed(t, pb, c, simSession("s1", time.Second), SourceIDLocal, deadline)

	got := names(c.finish(t, deadline))
	require.GreaterOrEqual(t, len(got), 4)
	assert.Equal(t, []string{"s1meta", "s1vcfg", "s1acfg", "s1v0"}, got[:4])
}

// T6: headers from a non-active source are stored but not forwarded, and the
// keyframe-aligned switch delivers the new source's headers before its keyframe.
func TestReconnect_InactiveSourceHeadersIgnoredAndSwitchWorks(t *testing.T) {
	deadline := time.Now().Add(5 * time.Second)
	const local, remote = 1, 21
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()
	pb.SetActiveSource(local)
	c := collect(pb.Subscribe())

	feed(t, pb, c, simHeaders("L"), local, deadline)
	feed(t, pb, c, simSession("L", time.Second), local, deadline)
	feed(t, pb, c, simHeaders("R"), remote, deadline) // not active, not pending
	feed(t, pb, c, simSession("R", time.Second), remote, deadline)

	fresh := collect(pb.Subscribe())
	assert.Equal(t, []string{"Lmeta", "Lvcfg", "Lacfg"}, names(fresh.finish(t, deadline)))

	pb.RequestSourceSwitch(remote)
	feed(t, pb, c, simHeaders("R2"), remote, deadline) // pending: stored, not forwarded yet
	pb.Broadcast(av.Packet{Type: av.H264, Time: 5000 * time.Millisecond, Data: []byte("R2inter")}, remote)
	// The active source reconnects while the switch is pending: its new headers
	// reach the subscriber, but must not replace the held pending headers.
	feed(t, pb, c, simHeaders("L2"), local, deadline)
	pb.Broadcast(av.Packet{Type: av.H264, Time: 0, IsKeyFrame: true, Data: []byte("L2key")}, local)
	pb.Broadcast(av.Packet{Type: av.H264, Time: 1100 * time.Millisecond, Data: []byte("Lmore")}, local)
	pb.Broadcast(av.Packet{Type: av.H264, Time: 5066 * time.Millisecond, IsKeyFrame: true, Data: []byte("R2key")}, remote)
	pb.Broadcast(av.Packet{Type: av.H264, Time: 5132 * time.Millisecond, Data: []byte("R2next")}, remote)
	assert.Equal(t, remote, pb.ActiveSourceID())

	got := c.finish(t, deadline)
	gotNames := names(got)
	for _, n := range []string{"Rmeta", "Rvcfg", "Racfg", "R2inter"} {
		assert.NotContains(t, gotNames, n)
	}
	for _, n := range gotNames {
		assert.False(t, strings.HasPrefix(n, "Rv") || strings.HasPrefix(n, "Ra"), "inactive source media forwarded: %s", n)
	}
	l2meta, l2key := indexOf(got, "L2meta"), indexOf(got, "L2key")
	assert.True(t, l2meta >= 0 && l2key > l2meta, "active source reconnect headers must be forwarded before its keyframe")
	lmore, meta, key := indexOf(got, "Lmore"), indexOf(got, "R2meta"), indexOf(got, "R2key")
	require.True(t, lmore >= 0 && meta > lmore && key > meta, "order: %v", gotNames[len(gotNames)-6:])
	assert.Equal(t, []string{"R2meta", "R2vcfg", "R2acfg", "R2key", "R2next"}, gotNames[meta:])
	assert.True(t, got[key].Time > got[lmore].Time && got[key].Time-got[lmore].Time < time.Second,
		"switch must keep the timeline continuous: %v -> %v", got[lmore].Time, got[key].Time)
}
