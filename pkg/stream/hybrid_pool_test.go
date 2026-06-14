package stream

import "testing"

// TestHybridStreamPool_DisableRemoteRelay_NeverStartsRelay verifies local-only
// mode: with DisableRemoteRelay set, startRemoteRelay (the single chokepoint for
// the camera->Nanit-cloud media relay) must early-return before touching the
// (here nil) conn/rtmpServer, leaving the relay unstarted. If the gate regressed,
// this would either start a relay or panic dereferencing the nil rtmpServer.
func TestHybridStreamPool_DisableRemoteRelay_NeverStartsRelay(t *testing.T) {
	pool := NewHybridStreamPool(HybridStreamPoolConfig{
		BabyUID:            "test-baby",
		LocalURL:           "rtmp://127.0.0.1/local/test",
		DisableRemoteRelay: true,
	})

	if !pool.disableRemoteRelay {
		t.Fatal("disableRemoteRelay not plumbed from config")
	}

	pool.startRemoteRelay()

	if pool.remoteRelay != nil {
		t.Error("remote relay was started despite DisableRemoteRelay=true")
	}
	if pool.IsRemoteActive() {
		t.Error("IsRemoteActive() = true; want false in local-only mode")
	}
}

// TestHybridStreamPool_DefaultStartsHotStandby is the negative control: with the
// default config the flag stays false, so the pool is in normal hybrid mode.
func TestHybridStreamPool_DefaultStartsHotStandby(t *testing.T) {
	pool := NewHybridStreamPool(HybridStreamPoolConfig{
		BabyUID:  "test-baby",
		LocalURL: "rtmp://127.0.0.1/local/test",
	})

	if pool.disableRemoteRelay {
		t.Error("disableRemoteRelay defaulted to true; want false (hybrid mode)")
	}
}
