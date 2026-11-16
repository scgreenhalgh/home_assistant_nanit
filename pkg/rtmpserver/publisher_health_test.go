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

