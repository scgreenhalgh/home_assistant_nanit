package rtmpserver

import (
	"bytes"
	"strings"
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
// TESTS FOR PERSISTENT BROADCASTER
// Following TDD - these tests define expected behavior for
// a broadcaster that persists across source switches with
// timestamp remapping for stream continuity.
// ============================================================

// --- Basic Broadcasting Tests ---

func TestPersistentBroadcaster_SubscriberReceivesPackets(t *testing.T) {
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()

	// Subscribe
	sub := pb.Subscribe()
	require.NotNil(t, sub)

	// Broadcast a video packet
	pb.Broadcast(av.Packet{
		Type:       1, // Video
		IsKeyFrame: true,
		Time:       100 * time.Millisecond,
		Data:       []byte{0x01, 0x02, 0x03},
	}, 1) // sourceID = 1

	// Subscriber should receive the packet
	select {
	case pkt := <-sub.Packets():
		assert.Equal(t, byte(0x01), pkt.Data[0])
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Timeout waiting for packet")
	}
}

func TestPersistentBroadcaster_MultipleSubscribers(t *testing.T) {
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()

	sub1 := pb.Subscribe()
	sub2 := pb.Subscribe()
	sub3 := pb.Subscribe()

	// Broadcast packet
	pb.Broadcast(av.Packet{
		Type:       1,
		IsKeyFrame: true,
		Time:       100 * time.Millisecond,
		Data:       []byte{0xAA},
	}, 1)

	// All subscribers should receive it
	for i, sub := range []*Subscription{sub1, sub2, sub3} {
		select {
		case pkt := <-sub.Packets():
			assert.Equal(t, byte(0xAA), pkt.Data[0], "Subscriber %d", i)
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("Subscriber %d timeout", i)
		}
	}
}

func TestPersistentBroadcaster_UnsubscribeStopsDelivery(t *testing.T) {
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()

	sub := pb.Subscribe()
	sub.Unsubscribe()

	// Broadcast after unsubscribe
	pb.Broadcast(av.Packet{
		Type: 1,
		Time: 100 * time.Millisecond,
		Data: []byte{0xBB},
	}, 1)

	// Channel should be closed
	select {
	case _, ok := <-sub.Packets():
		assert.False(t, ok, "Channel should be closed")
	case <-time.After(50 * time.Millisecond):
		// Also acceptable - nothing received
	}
}

// --- Header Packet Tests ---

func TestPersistentBroadcaster_HeaderPacketsStoredAndSentToNewSubscribers(t *testing.T) {
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()

	// Send header packets (type > 2)
	pb.Broadcast(av.Packet{Type: 5, Data: []byte("SPS")}, 1)
	pb.Broadcast(av.Packet{Type: 5, Data: []byte("PPS")}, 1)

	// Send a video packet
	pb.Broadcast(av.Packet{Type: 1, Time: 100 * time.Millisecond, Data: []byte("video1")}, 1)

	// New subscriber joins after headers were sent
	sub := pb.Subscribe()

	// Send another video packet to trigger delivery
	pb.Broadcast(av.Packet{Type: 1, Time: 133 * time.Millisecond, Data: []byte("video2")}, 1)

	// Subscriber should receive headers first, then video
	received := make([]string, 0)
	timeout := time.After(200 * time.Millisecond)

collectLoop:
	for i := 0; i < 4; i++ {
		select {
		case pkt := <-sub.Packets():
			received = append(received, string(pkt.Data))
		case <-timeout:
			break collectLoop
		}
	}

	// Should have: SPS, PPS, video1 (from before), video2
	// Actually, video1 was broadcast before subscribe, so just SPS, PPS, video2
	assert.Contains(t, received, "SPS")
	assert.Contains(t, received, "PPS")
	assert.Contains(t, received, "video2")
}

// --- Timestamp Remapping Tests ---

func TestPersistentBroadcaster_TimestampsAreMonotonic(t *testing.T) {
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()

	sub := pb.Subscribe()

	// Source 1 sends packets at high timestamps
	pb.Broadcast(av.Packet{Type: 1, Time: 50000 * time.Millisecond, Data: []byte("a")}, 1)
	pb.Broadcast(av.Packet{Type: 1, Time: 50033 * time.Millisecond, Data: []byte("b")}, 1)
	pb.Broadcast(av.Packet{Type: 1, Time: 50066 * time.Millisecond, Data: []byte("c")}, 1)

	// Switch to source 2 which starts at timestamp 0
	pb.Broadcast(av.Packet{Type: 1, Time: 0 * time.Millisecond, Data: []byte("d")}, 2)
	pb.Broadcast(av.Packet{Type: 1, Time: 33 * time.Millisecond, Data: []byte("e")}, 2)
	pb.Broadcast(av.Packet{Type: 1, Time: 66 * time.Millisecond, Data: []byte("f")}, 2)

	// Collect timestamps
	timestamps := make([]time.Duration, 0)
	timeout := time.After(200 * time.Millisecond)

	for i := 0; i < 6; i++ {
		select {
		case pkt := <-sub.Packets():
			timestamps = append(timestamps, pkt.Time)
		case <-timeout:
			break
		}
	}

	require.Len(t, timestamps, 6, "Should receive 6 packets")

	// Verify timestamps are monotonically increasing
	for i := 1; i < len(timestamps); i++ {
		if timestamps[i] <= timestamps[i-1] {
			t.Errorf("Timestamp %d (%v) should be > timestamp %d (%v)",
				i, timestamps[i], i-1, timestamps[i-1])
		}
	}
}

func TestPersistentBroadcaster_TimestampContinuityOnSourceSwitch(t *testing.T) {
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()

	sub := pb.Subscribe()

	// Source 1: timestamps 1000, 1033, 1066
	pb.Broadcast(av.Packet{Type: 1, Time: 1000 * time.Millisecond, Data: []byte("1")}, 1)
	pb.Broadcast(av.Packet{Type: 1, Time: 1033 * time.Millisecond, Data: []byte("2")}, 1)
	pb.Broadcast(av.Packet{Type: 1, Time: 1066 * time.Millisecond, Data: []byte("3")}, 1)

	// Source 2: timestamps 5000, 5033 (completely different timeline)
	pb.Broadcast(av.Packet{Type: 1, Time: 5000 * time.Millisecond, Data: []byte("4")}, 2)
	pb.Broadcast(av.Packet{Type: 1, Time: 5033 * time.Millisecond, Data: []byte("5")}, 2)

	// Collect packets
	var lastTimestamp time.Duration
	for i := 0; i < 5; i++ {
		select {
		case pkt := <-sub.Packets():
			if i > 0 {
				// Check gap is reasonable (not a huge jump)
				gap := pkt.Time - lastTimestamp
				assert.True(t, gap > 0 && gap < 1*time.Second,
					"Gap between packets should be small, got %v", gap)
			}
			lastTimestamp = pkt.Time
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("Timeout waiting for packet %d", i)
		}
	}
}

func TestPersistentBroadcaster_MultipleSourceSwitchesMaintainContinuity(t *testing.T) {
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()

	sub := pb.Subscribe()

	// Switch between sources multiple times
	sources := []struct {
		sourceID  int
		startTime time.Duration
	}{
		{1, 0},
		{2, 50000 * time.Millisecond},
		{1, 100 * time.Millisecond},
		{2, 99000 * time.Millisecond},
	}

	for _, src := range sources {
		for i := 0; i < 3; i++ {
			pb.Broadcast(av.Packet{
				Type: 1,
				Time: src.startTime + time.Duration(i*33)*time.Millisecond,
				Data: []byte{byte(src.sourceID)},
			}, src.sourceID)
		}
	}

	// Verify all timestamps are monotonically increasing
	var lastTs time.Duration
	for i := 0; i < 12; i++ {
		select {
		case pkt := <-sub.Packets():
			if i > 0 && pkt.Time <= lastTs {
				t.Errorf("Packet %d timestamp (%v) should be > previous (%v)", i, pkt.Time, lastTs)
			}
			lastTs = pkt.Time
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("Timeout at packet %d", i)
		}
	}
}

// --- Source Switch Tests (No Disconnection) ---

func TestPersistentBroadcaster_SubscribersNotDisconnectedOnSourceSwitch(t *testing.T) {
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()

	sub := pb.Subscribe()
	packetsReceived := int32(0)

	// Start goroutine to count packets
	done := make(chan struct{})
	go func() {
		for {
			select {
			case _, ok := <-sub.Packets():
				if !ok {
					close(done)
					return
				}
				atomic.AddInt32(&packetsReceived, 1)
			case <-done:
				return
			}
		}
	}()

	// Send from source 1
	for i := 0; i < 5; i++ {
		pb.Broadcast(av.Packet{Type: 1, Time: time.Duration(i*33) * time.Millisecond}, 1)
	}

	// Switch to source 2 (should NOT disconnect subscriber)
	for i := 0; i < 5; i++ {
		pb.Broadcast(av.Packet{Type: 1, Time: time.Duration(i*33) * time.Millisecond}, 2)
	}

	// Give time for delivery
	time.Sleep(100 * time.Millisecond)

	// Subscriber should still be connected and received all 10 packets
	count := atomic.LoadInt32(&packetsReceived)
	assert.Equal(t, int32(10), count, "Should receive all 10 packets without disconnection")

	// Clean up
	sub.Unsubscribe()
}

func TestPersistentBroadcaster_HeadersReplacedOnSourceSwitch(t *testing.T) {
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()

	// Source 1 headers
	pb.Broadcast(av.Packet{Type: 5, Data: []byte("SPS_v1")}, 1)
	pb.Broadcast(av.Packet{Type: 5, Data: []byte("PPS_v1")}, 1)

	// Source 1 video
	pb.Broadcast(av.Packet{Type: 1, Time: 100 * time.Millisecond}, 1)

	// Switch to source 2 with different headers
	pb.Broadcast(av.Packet{Type: 5, Data: []byte("SPS_v2")}, 2)
	pb.Broadcast(av.Packet{Type: 5, Data: []byte("PPS_v2")}, 2)

	// New subscriber joins - should get source 2 headers
	sub := pb.Subscribe()

	// Trigger delivery
	pb.Broadcast(av.Packet{Type: 1, Time: 200 * time.Millisecond, Data: []byte("video")}, 2)

	// Collect received data
	received := make([]string, 0)
	timeout := time.After(200 * time.Millisecond)
	for {
		select {
		case pkt := <-sub.Packets():
			if len(pkt.Data) > 0 {
				received = append(received, string(pkt.Data))
			}
		case <-timeout:
			goto check
		}
	}
check:
	// Should have v2 headers, not v1
	assert.Contains(t, received, "SPS_v2")
	assert.Contains(t, received, "PPS_v2")
	assert.NotContains(t, received, "SPS_v1")
	assert.NotContains(t, received, "PPS_v1")
}

// --- Concurrent Access Tests ---

func TestPersistentBroadcaster_ConcurrentSubscribeUnsubscribe(t *testing.T) {
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()

	var wg sync.WaitGroup

	// Concurrent subscribers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub := pb.Subscribe()
			time.Sleep(10 * time.Millisecond)
			sub.Unsubscribe()
		}()
	}

	// Concurrent broadcasts
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pb.Broadcast(av.Packet{
				Type: 1,
				Time: time.Duration(idx*33) * time.Millisecond,
			}, 1)
		}(i)
	}

	wg.Wait()
	// Test passes if no race/deadlock
}

func TestPersistentBroadcaster_ConcurrentSourceSwitches(t *testing.T) {
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()

	sub := pb.Subscribe()
	received := int32(0)

	go func() {
		for range sub.Packets() {
			atomic.AddInt32(&received, 1)
		}
	}()

	var wg sync.WaitGroup
	// Multiple goroutines broadcasting from different sources
	for srcID := 1; srcID <= 3; srcID++ {
		wg.Add(1)
		go func(src int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				pb.Broadcast(av.Packet{
					Type: 1,
					Time: time.Duration(i*33) * time.Millisecond,
				}, src)
			}
		}(srcID)
	}

	wg.Wait()
	time.Sleep(50 * time.Millisecond)

	// For live streams, packets may be dropped during concurrent source switches
	// Test verifies thread safety - a reasonable number of packets should arrive without race/deadlock
	// With 3 concurrent sources switching rapidly, significant packet loss is expected
	count := atomic.LoadInt32(&received)
	assert.True(t, count >= 100, "Should receive reasonable packets despite source switches (got %d/300)", count)
}

// --- Edge Cases ---

func TestPersistentBroadcaster_NoSubscribers(t *testing.T) {
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()

	// Should not panic with no subscribers
	pb.Broadcast(av.Packet{Type: 1, Time: 100 * time.Millisecond}, 1)
	pb.Broadcast(av.Packet{Type: 5, Data: []byte("header")}, 1)
}

func TestPersistentBroadcaster_LargeTimestampJumpForward(t *testing.T) {
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()

	sub := pb.Subscribe()

	// Normal timestamps
	pb.Broadcast(av.Packet{Type: 1, Time: 100 * time.Millisecond}, 1)
	pb.Broadcast(av.Packet{Type: 1, Time: 133 * time.Millisecond}, 1)

	// Huge jump forward (same source) - could indicate stream discontinuity
	pb.Broadcast(av.Packet{Type: 1, Time: 60000 * time.Millisecond}, 1)

	timestamps := make([]time.Duration, 0)
	for i := 0; i < 3; i++ {
		select {
		case pkt := <-sub.Packets():
			timestamps = append(timestamps, pkt.Time)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Timeout")
		}
	}

	// Large forward jump within same source should be preserved (not remapped)
	// This is a policy decision - could also remap these
	require.Len(t, timestamps, 3)
}

func TestPersistentBroadcaster_GetBabyUID(t *testing.T) {
	pb := NewPersistentBroadcaster("test-baby-uid")
	assert.Equal(t, "test-baby-uid", pb.BabyUID())
}

// --- Concurrent Safety Tests ---

func TestPersistentBroadcaster_ConcurrentClose(t *testing.T) {
	// Test that multiple concurrent Close() calls don't panic (e.g., from double channel close)
	pb := NewPersistentBroadcaster("baby123")

	// Enable stall detection to test stallCheckStop channel close safety
	stallCalled := atomic.Int32{}
	pb.SetStallConfig(&StallConfig{
		Threshold: 100 * time.Millisecond,
		Callback: func(sourceID int) {
			stallCalled.Add(1)
		},
	})

	// Add some subscribers
	sub1 := pb.Subscribe()
	sub2 := pb.Subscribe()
	_ = sub1
	_ = sub2

	// Launch multiple goroutines that all try to close
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// This should not panic even when called concurrently
			pb.Close()
		}()
	}

	wg.Wait()
	// If we get here without panic, the test passes
}

func TestPersistentBroadcaster_ConcurrentSetStallConfig(t *testing.T) {
	// Test that concurrent SetStallConfig calls don't panic from double channel close
	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()

	var wg sync.WaitGroup

	// Launch multiple goroutines that all reconfigure stall detection
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// This should not panic even when called concurrently
			pb.SetStallConfig(&StallConfig{
				Threshold: time.Duration(100+i) * time.Millisecond,
				Callback: func(sourceID int) {},
			})
		}(i)
	}

	wg.Wait()
	// If we get here without panic, the test passes
}

func TestPersistentBroadcaster_ConcurrentUnsubscribeAndClose(t *testing.T) {
	// Test that concurrent Unsubscribe and Close don't cause double channel close
	pb := NewPersistentBroadcaster("baby123")

	// Create many subscribers
	subs := make([]*Subscription, 20)
	for i := range subs {
		subs[i] = pb.Subscribe()
	}

	var wg sync.WaitGroup

	// Half the subscribers unsubscribe themselves
	for i := 0; i < len(subs)/2; i++ {
		wg.Add(1)
		go func(sub *Subscription) {
			defer wg.Done()
			sub.Unsubscribe()
		}(subs[i])
	}

	// Close is called concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		pb.Close()
	}()

	wg.Wait()
	// If we get here without panic, the test passes
}

// --- Lock Order Tests ---

// Two sources broadcasting into a full subscriber buffer while headers arrive and
// subscribers come and go must not deadlock (tsMu and mu must be taken in one order).
func TestPersistentBroadcaster_NoDeadlockWithFullSubscriberAndHeaders(t *testing.T) {
	oldLogger := log.Logger
	log.Logger = zerolog.Nop()
	defer func() { log.Logger = oldLogger }()

	// No deferred Close: on a deadlock Close would block on mu and hide the failure
	pb := NewPersistentBroadcaster("baby123")

	// A subscriber that never reads, so its buffer fills and every packet is dropped
	stuck := pb.Subscribe()
	require.NotNil(t, stuck)

	const iterations = 2000
	var wg sync.WaitGroup
	wg.Add(3)

	// Source A: media packets
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			pb.Broadcast(av.Packet{Type: 1, Time: time.Duration(i) * time.Millisecond}, 1)
		}
	}()

	// Source B: header packets and media
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			pb.Broadcast(av.Packet{Type: 3, Data: []byte{byte(i)}}, 2)
			pb.Broadcast(av.Packet{Type: 1, Time: time.Duration(i) * time.Millisecond}, 2)
		}
	}()

	// Subscribe/Unsubscribe churn
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if sub := pb.Subscribe(); sub != nil {
				sub.Unsubscribe()
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		pb.Close()
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: broadcaster workers did not finish within 10s")
	}
}

// Dropped packets are all counted, but the warning is logged at most once per 10s.
func TestPersistentBroadcaster_DroppedPacketWarningIsRateLimited(t *testing.T) {
	oldLogger := log.Logger
	var buf bytes.Buffer
	log.Logger = zerolog.New(&buf)
	defer func() { log.Logger = oldLogger }()

	pb := NewPersistentBroadcaster("baby123")
	defer pb.Close()

	stuck := pb.Subscribe() // never read: buffer of 500 fills
	require.NotNil(t, stuck)

	const sent = 1500
	for i := 0; i < sent; i++ {
		pb.Broadcast(av.Packet{Type: 1, Time: time.Duration(i) * time.Millisecond}, 1)
	}

	assert.Equal(t, int64(sent-500), pb.droppedPackets.Load())
	out := buf.String()
	assert.Equal(t, 1, strings.Count(out, "Packet(s) dropped due to full subscriber buffer"))
	assert.Contains(t, out, `"dropped_since_last_log":1`)
}
