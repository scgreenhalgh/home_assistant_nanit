package rtmpserver

import (
	"net"
	"testing"
	"time"

	"github.com/notedit/rtmp/av"
	"github.com/notedit/rtmp/format/rtmp"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/baby"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnregisterLocalPublisher_PlainKeyCountsOnce(t *testing.T) {
	handler := newRtmpHandler(baby.NewStateManager())

	handler.registerLocalPublisher("baby123", "", "baby123")
	handler.registerLocalPublisher("baby123", "", "baby123")
	assert.Equal(t, 1, handler.unregisterLocalPublisher("baby123", "baby123"), "one publisher is still registered")
	assert.Equal(t, 0, handler.unregisterLocalPublisher("baby123", "baby123"), "the last publisher left")
	assert.False(t, handler.IsLocalPublisherActive("baby123"))

	// Slot keys are still counted under both the baby UID and the slot key.
	handler.registerLocalPublisher("baby123", "_0", "baby123_0")
	handler.registerLocalPublisher("baby123", "_1", "baby123_1")
	assert.Equal(t, 1, handler.unregisterLocalPublisher("baby123", "baby123_0"))
	assert.Equal(t, 0, handler.unregisterLocalPublisher("baby123", "baby123_1"))
}

// startTestRTMPServer serves the handler on a loopback port and returns its address.
func startTestRTMPServer(t *testing.T, handler *rtmpHandler) string {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := rtmp.NewServer()
	s.HandleConn = handler.handleConnection
	go func() {
		for {
			nc, err := lis.Accept()
			if err != nil {
				return
			}
			go s.HandleNetConn(nc)
		}
	}()
	t.Cleanup(func() { lis.Close() })
	return lis.Addr().String()
}

func publish(t *testing.T, addr, uid string) (*rtmp.Conn, net.Conn) {
	conn, nc, err := rtmp.NewClient().Dial("rtmp://"+addr+"/local/"+uid, rtmp.PrepareWriting)
	require.NoError(t, err)
	require.NoError(t, conn.WritePacket(av.Packet{Type: av.H264, IsKeyFrame: true, Data: []byte{1, 2, 3}}))
	return conn, nc
}

func streamState(sm *baby.StateManager, uid string) baby.StreamState {
	s := sm.GetBabyState(uid)
	if s == nil {
		return baby.StreamState_Unknown
	}
	return s.GetStreamState()
}

func TestLocalPublisherLeaving_PublishesUnhealthyOnlyWhenLast(t *testing.T) {
	sm := baby.NewStateManager()
	handler := newRtmpHandler(sm)
	addr := startTestRTMPServer(t, handler)
	const uid = "baby123"

	waitRegistered := func(n int) {
		require.Eventually(t, func() bool {
			handler.localPublishersMu.RLock()
			defer handler.localPublishersMu.RUnlock()
			return handler.localPublishers[uid] == n
		}, 3*time.Second, 5*time.Millisecond)
	}

	_, nc1 := publish(t, addr, uid)
	waitRegistered(1)
	_, nc2 := publish(t, addr, uid)
	waitRegistered(2)
	require.Eventually(t, func() bool { return streamState(sm, uid) == baby.StreamState_Alive }, 3*time.Second, 5*time.Millisecond)

	// One of two publishers leaves: still Alive.
	nc1.Close()
	waitRegistered(1)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, baby.StreamState_Alive, streamState(sm, uid), "a publisher leaving while another remains must not publish Unhealthy")

	// The last publisher leaves: Unhealthy.
	nc2.Close()
	require.Eventually(t, func() bool { return streamState(sm, uid) == baby.StreamState_Unhealthy }, 3*time.Second, 5*time.Millisecond,
		"the last publisher leaving must publish Unhealthy")
}

func TestSourceAge(t *testing.T) {
	pb := NewPersistentBroadcaster("baby123")
	pb.SetActiveSource(1)

	_, seen := pb.SourceAge(21)
	assert.False(t, seen, "no packet yet")

	// Header packets are not media and do not count.
	pb.Broadcast(av.Packet{Type: av.H264DecoderConfig}, 21)
	_, seen = pb.SourceAge(21)
	assert.False(t, seen, "header packets do not count")

	// A media packet from a filtered (inactive) source still counts.
	pb.Broadcast(av.Packet{Type: av.H264, IsKeyFrame: true}, 21)
	age, seen := pb.SourceAge(21)
	assert.True(t, seen)
	assert.Less(t, int64(age), int64(time.Second))

	time.Sleep(30 * time.Millisecond)
	age, _ = pb.SourceAge(21)
	assert.GreaterOrEqual(t, int64(age), int64(30*time.Millisecond))
	_, seen = pb.SourceAge(1)
	assert.False(t, seen, "sources are tracked separately")
}

// Requesting the current active source cancels a pending switch to another source.
func TestRequestSourceSwitch_ToActiveCancelsPending(t *testing.T) {
	pb := NewPersistentBroadcaster("baby123")
	pb.SetActiveSource(1)
	pb.RequestSourceSwitch(21)
	require.True(t, pb.IsPendingSwitch())
	assert.Equal(t, 21, pb.PendingSourceID())

	pb.RequestSourceSwitch(1)
	assert.False(t, pb.IsPendingSwitch())
	assert.Equal(t, 0, pb.PendingSourceID())

	// A later keyframe from 21 must not switch.
	pb.Broadcast(av.Packet{Type: av.H264, IsKeyFrame: true}, 21)
	assert.Equal(t, 1, pb.ActiveSourceID())
}

// A stale owner clearing its stall config must not remove a newer owner's.
func TestClearStallConfig_OnlyClearsOwnConfig(t *testing.T) {
	pb := NewPersistentBroadcaster("baby123")
	old := &StallConfig{Threshold: time.Hour, Callback: func(int) {}}
	pb.SetStallConfig(old)
	newer := &StallConfig{Threshold: time.Hour, Callback: func(int) {}}
	pb.SetStallConfig(newer)

	pb.ClearStallConfig(old)
	pb.tsMu.Lock()
	assert.Same(t, newer, pb.stallConfig)
	pb.tsMu.Unlock()

	pb.ClearStallConfig(newer)
	pb.tsMu.Lock()
	assert.Nil(t, pb.stallConfig)
	pb.tsMu.Unlock()
	pb.Close()
}

// Health means video: audio-only packets do not make a source seen.
func TestSourceAge_IgnoresAudio(t *testing.T) {
	pb := NewPersistentBroadcaster("baby123")
	pb.Broadcast(av.Packet{Type: av.AAC}, 21)
	pb.Broadcast(av.Packet{Type: av.OPUS}, 21)
	_, seen := pb.SourceAge(21)
	assert.False(t, seen, "audio packets must not count as a healthy source")
}

// Only the release of the last claim runs final; until then the handler stays
// silent, and after it the handler writes again.
func TestStreamStateClaims_OnlyLastReleaseRunsFinal(t *testing.T) {
	sm := baby.NewStateManager()
	srv := &RTMPServer{handler: newRtmpHandler(sm)}
	addr := startTestRTMPServer(t, srv.handler)
	const uid = "baby123"
	claimed := func() bool {
		srv.handler.localPublishersMu.RLock()
		defer srv.handler.localPublishersMu.RUnlock()
		return srv.handler.streamStateClaims[uid] > 0
	}

	srv.ClaimStreamState(uid)
	srv.ClaimStreamState(uid)
	calls := 0
	srv.ReleaseStreamState(uid, func(bool) { calls++ })
	assert.Equal(t, 0, calls, "final must not run while another claim remains")
	assert.True(t, claimed(), "the handler must stay silent while another claim remains")

	_, nc := publish(t, addr, uid)
	require.Eventually(t, func() bool { return srv.handler.IsLocalPublisherActive(uid) }, 3*time.Second, 5*time.Millisecond)
	connected := false
	srv.ReleaseStreamState(uid, func(c bool) { calls++; connected = c })
	assert.Equal(t, 1, calls, "the last release runs final once")
	assert.True(t, connected, "final sees the connected publisher")
	assert.False(t, claimed())

	nc.Close()
	require.Eventually(t, func() bool { return streamState(sm, uid) == baby.StreamState_Unhealthy }, 3*time.Second, 5*time.Millisecond,
		"the handler writes again after the last release")
}
