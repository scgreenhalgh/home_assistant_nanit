package rtmpserver

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/notedit/rtmp/av"
	"github.com/notedit/rtmp/format/rtmp"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/baby"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for "newest publisher wins" on one stream key.

// T7 (release): a publisher that disconnects gives up its claim on the key.
func TestServer_PublisherReleasesClaimOnExit(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	h := newRtmpHandler(baby.NewStateManager())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()
	srv := rtmp.NewServer()
	srv.HandleConn = h.handleConnection
	go func() {
		for {
			nc, err := lis.Accept()
			if err != nil {
				return
			}
			go srv.HandleNetConn(nc)
		}
	}()

	_, nc, err := rtmp.NewClient().Dial("rtmp://"+lis.Addr().String()+"/local/baby123", rtmp.PrepareWriting)
	require.NoError(t, err)
	claims := func() int {
		h.currentPublishersMu.Lock()
		defer h.currentPublishersMu.Unlock()
		return len(h.currentPublishers)
	}
	for claims() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("deadline exceeded waiting for the claim")
		}
		time.Sleep(5 * time.Millisecond)
	}
	nc.Close()
	for claims() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("claim was not released after the publisher disconnected")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// T7 (bookkeeping): the newest publisher on a stream key is the current one;
// an older one is no longer current, and its release does not evict the newer.
func TestPublisherBookkeeping_NewestWins(t *testing.T) {
	h := newRtmpHandler(baby.NewStateManager())
	a1, a2 := net.Pipe()
	b1, b2 := net.Pipe()
	defer a2.Close()
	defer b2.Close()

	assert.Nil(t, h.claimPublisher("baby123", a1))
	assert.True(t, h.isCurrentPublisher("baby123", a1))

	assert.Same(t, a1, h.claimPublisher("baby123", b1))
	assert.False(t, h.isCurrentPublisher("baby123", a1))
	assert.True(t, h.isCurrentPublisher("baby123", b1))

	// Another slot is independent.
	assert.Nil(t, h.claimPublisher("baby123_1", a1))

	h.releasePublisher("baby123", a1) // stale release must not evict b1
	assert.True(t, h.isCurrentPublisher("baby123", b1))
	h.releasePublisher("baby123", b1)
	assert.False(t, h.isCurrentPublisher("baby123", b1))
}

// T7 (server level): with real RTMP connections on 127.0.0.1, a second
// publisher on the same key closes the first, and packets the first one
// sends afterwards are not broadcast.
func TestServer_SecondPublisherReplacesFirst(t *testing.T) {
	deadline := time.Now().Add(10 * time.Second)
	h := newRtmpHandler(baby.NewStateManager())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()
	srv := rtmp.NewServer()
	srv.HandleConn = h.handleConnection
	go func() {
		for {
			nc, err := lis.Accept()
			if err != nil {
				return
			}
			go srv.HandleNetConn(nc)
		}
	}()
	url := "rtmp://" + lis.Addr().String() + "/local/baby123"

	// The client buffers writes in a 4 KiB bufio.Writer with no public Flush,
	// so each packet is padded to 8 KiB and followed by an 8 KiB pad packet.
	send := func(conn *rtmp.Conn, name string, ts time.Duration) error {
		data := make([]byte, 8192)
		copy(data, name)
		if err := conn.WritePacket(av.Packet{Type: av.H264, IsKeyFrame: true, Time: ts, Data: data}); err != nil {
			return err
		}
		pad := make([]byte, 8192)
		copy(pad, name+"pad")
		return conn.WritePacket(av.Packet{Type: av.H264, Time: ts, Data: pad})
	}
	waitFor := func(c *collector, name string) {
		for {
			c.mu.Lock()
			found := indexOf(c.pkts, name) >= 0
			c.mu.Unlock()
			if found {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("deadline exceeded waiting for %s", name)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	connA, ncA, err := rtmp.NewClient().Dial(url, rtmp.PrepareWriting)
	require.NoError(t, err)
	defer ncA.Close()
	require.NoError(t, send(connA, "A0", 0))

	// Wait for the broadcaster, then subscribe.
	var sub *Subscription
	for sub == nil {
		if time.Now().After(deadline) {
			t.Fatal("deadline exceeded waiting for broadcaster")
		}
		sub = h.getNewSubscriber("baby123")
		time.Sleep(5 * time.Millisecond)
	}
	c := collect(sub)
	require.NoError(t, send(connA, "A1", 66*time.Millisecond))
	waitFor(c, "A1")

	connB, ncB, err := rtmp.NewClient().Dial(url, rtmp.PrepareWriting)
	require.NoError(t, err)
	defer ncB.Close()

	// The server must close A as soon as B is registered.
	select {
	case <-connA.CloseNotify():
	case <-time.After(time.Until(deadline)):
		t.Fatal("first publisher was not closed")
	}

	require.NoError(t, send(connB, "B0", 0))
	waitFor(c, "B0")
	for i := 0; i < 5; i++ {
		_ = send(connA, "Alate", time.Duration(200+i)*time.Millisecond) // may fail: A is closed
	}
	require.NoError(t, send(connB, "B1", 66*time.Millisecond))
	waitFor(c, "B1")

	got := names(c.finish(t, deadline))
	b0 := indexOf(c.pkts, "B0")
	for _, n := range got[b0:] {
		assert.False(t, strings.HasPrefix(n, "A"), "packet from the replaced publisher broadcast after the new one: %s (all: %v)", n, got)
	}
}

func TestForwardIfCurrent_DropsPacketsFromReplacedPublisher(t *testing.T) {
	h := newRtmpHandler(baby.NewStateManager())
	a, a2 := net.Pipe()
	b, b2 := net.Pipe()
	defer a2.Close()
	defer b2.Close()

	var got []string
	send := func(p av.Packet) { got = append(got, string(p.Data)) }

	h.claimPublisher("baby123", a)
	assert.True(t, h.forwardIfCurrent("baby123", a, av.Packet{Data: []byte("a1")}, send))
	h.claimPublisher("baby123", b)
	assert.False(t, h.forwardIfCurrent("baby123", a, av.Packet{Data: []byte("a2")}, send))
	assert.True(t, h.forwardIfCurrent("baby123", b, av.Packet{Data: []byte("b1")}, send))
	assert.Equal(t, []string{"a1", "b1"}, got)
}
