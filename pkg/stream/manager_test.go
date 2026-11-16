package stream_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/scgreenhalgh/home_assistant_nanit/pkg/baby"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// TESTS FOR STREAM MANAGER
// These tests define the expected behavior for the StreamManager
// which handles local/remote stream failover with auto-restoration.
// Following TDD, these tests are written BEFORE implementation.
// ============================================================

// --- Mock Dependencies ---

// MockLocalStreamer simulates the local streaming request
type MockLocalStreamer struct {
	mu            sync.Mutex
	attempts      int
	shouldSucceed bool
	shouldError   error
	onAttempt     func(attempt int)
	waitForPubErr error
}

func (m *MockLocalStreamer) RequestLocalStream(ctx context.Context) error {
	m.mu.Lock()
	m.attempts++
	attempt := m.attempts
	shouldSucceed := m.shouldSucceed
	shouldError := m.shouldError
	onAttempt := m.onAttempt
	m.mu.Unlock()

	if onAttempt != nil {
		onAttempt(attempt)
	}

	if shouldError != nil {
		return shouldError
	}
	if !shouldSucceed {
		return errors.New("local stream failed")
	}
	return nil
}

func (m *MockLocalStreamer) StopLocalStream(ctx context.Context) error {
	return nil
}

func (m *MockLocalStreamer) GetAttempts() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.attempts
}

func (m *MockLocalStreamer) SetShouldSucceed(val bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldSucceed = val
	m.shouldError = nil
}

func (m *MockLocalStreamer) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shouldError = err
}

func (m *MockLocalStreamer) WaitForPublisher(ctx context.Context, timeout time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.waitForPubErr
}

func (m *MockLocalStreamer) SetWaitForPubErr(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.waitForPubErr = err
}

// MockRemoteRelay simulates the remote stream relay
type MockRemoteRelay struct {
	mu        sync.Mutex
	started   bool
	stopped   bool
	startErr  error
	onStart   func()
	onStop    func()
}

func (m *MockRemoteRelay) Start(ctx context.Context) error {
	m.mu.Lock()
	m.started = true
	startErr := m.startErr
	onStart := m.onStart
	m.mu.Unlock()

	if onStart != nil {
		onStart()
	}
	return startErr
}

func (m *MockRemoteRelay) Stop() error {
	m.mu.Lock()
	m.stopped = true
	onStop := m.onStop
	m.mu.Unlock()

	if onStop != nil {
		onStop()
	}
	return nil
}

func (m *MockRemoteRelay) IsStarted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.started
}

func (m *MockRemoteRelay) IsStopped() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopped
}

// --- Test Cases ---

func TestNewStreamManager(t *testing.T) {
	stateManager := baby.NewStateManager()
	localStreamer := &MockLocalStreamer{}
	remoteRelay := &MockRemoteRelay{}

	sm := stream.NewStreamManager(stream.StreamManagerConfig{
		BabyUID:       "baby123",
		StateManager:  stateManager,
		LocalStreamer: localStreamer,
		RemoteRelay:   remoteRelay,
	})

	assert.NotNil(t, sm)
	assert.Equal(t, baby.StreamType_None, sm.GetCurrentType())
}

func TestStreamManagerStartLocalSuccess(t *testing.T) {
	stateManager := baby.NewStateManager()
	localStreamer := &MockLocalStreamer{shouldSucceed: true}
	remoteRelay := &MockRemoteRelay{}

	sm := stream.NewStreamManager(stream.StreamManagerConfig{
		BabyUID:       "baby123",
		StateManager:  stateManager,
		LocalStreamer: localStreamer,
		RemoteRelay:   remoteRelay,
	})

	ctx := context.Background()
	err := sm.Start(ctx)

	require.NoError(t, err)
	assert.Equal(t, baby.StreamType_Local, sm.GetCurrentType())
	assert.Equal(t, 1, localStreamer.GetAttempts())
	assert.False(t, remoteRelay.IsStarted())
}

func TestStreamManagerFailoverToRemoteOnMaxConnections(t *testing.T) {
	stateManager := baby.NewStateManager()
	localStreamer := &MockLocalStreamer{
		shouldError: stream.ErrMaxConnections,
	}
	remoteRelay := &MockRemoteRelay{}

	sm := stream.NewStreamManager(stream.StreamManagerConfig{
		BabyUID:       "baby123",
		StateManager:  stateManager,
		LocalStreamer: localStreamer,
		RemoteRelay:   remoteRelay,
	})

	ctx := context.Background()
	err := sm.Start(ctx)

	require.NoError(t, err) // Should succeed via remote fallback
	assert.Equal(t, baby.StreamType_Remote, sm.GetCurrentType())
	assert.True(t, remoteRelay.IsStarted())
}

func TestStreamManagerFailoverToRemoteOnMaxConnectionsErrorString(t *testing.T) {
	stateManager := baby.NewStateManager()
	localStreamer := &MockLocalStreamer{
		shouldError: errors.New("Forbidden: Number of Mobile App connections above limit, declining connection"),
	}
	remoteRelay := &MockRemoteRelay{}

	sm := stream.NewStreamManager(stream.StreamManagerConfig{
		BabyUID:       "baby123",
		StateManager:  stateManager,
		LocalStreamer: localStreamer,
		RemoteRelay:   remoteRelay,
	})

	ctx := context.Background()
	err := sm.Start(ctx)

	require.NoError(t, err)
	assert.Equal(t, baby.StreamType_Remote, sm.GetCurrentType())
	assert.True(t, remoteRelay.IsStarted())
}

func TestStreamManagerUpdatesStateManager(t *testing.T) {
	stateManager := baby.NewStateManager()
	localStreamer := &MockLocalStreamer{shouldSucceed: true}
	remoteRelay := &MockRemoteRelay{}

	sm := stream.NewStreamManager(stream.StreamManagerConfig{
		BabyUID:       "baby123",
		StateManager:  stateManager,
		LocalStreamer: localStreamer,
		RemoteRelay:   remoteRelay,
	})

	ctx := context.Background()
	err := sm.Start(ctx)
	require.NoError(t, err)

	// StateManager should reflect the stream type
	state := stateManager.GetBabyState("baby123")
	assert.Equal(t, baby.StreamType_Local, state.GetStreamType())
}

func TestStreamManagerRestoreLocalWhileOnRemote(t *testing.T) {
	stateManager := baby.NewStateManager()
	attemptChan := make(chan int, 10)

	localStreamer := &MockLocalStreamer{
		shouldError: stream.ErrMaxConnections,
		onAttempt: func(attempt int) {
			attemptChan <- attempt
		},
	}
	remoteRelay := &MockRemoteRelay{}

	// Use short intervals for testing
	sm := stream.NewStreamManager(stream.StreamManagerConfig{
		BabyUID:             "baby123",
		StateManager:        stateManager,
		LocalStreamer:       localStreamer,
		RemoteRelay:         remoteRelay,
		RestoreInitialDelay: 50 * time.Millisecond,
		RestoreMaxDelay:     200 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := sm.Start(ctx)
	require.NoError(t, err)
	assert.Equal(t, baby.StreamType_Remote, sm.GetCurrentType())

	// Wait for first attempt (initial)
	select {
	case <-attemptChan:
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for first attempt")
	}

	// Now make local succeed
	localStreamer.SetShouldSucceed(true)

	// Wait for restore attempt
	select {
	case <-attemptChan:
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for restore attempt")
	}

	// Give time for state transition
	time.Sleep(50 * time.Millisecond)

	// Should have switched back to local
	assert.Equal(t, baby.StreamType_Local, sm.GetCurrentType())
	assert.True(t, remoteRelay.IsStopped())
}

func TestStreamManagerRestoreBackoff(t *testing.T) {
	stateManager := baby.NewStateManager()
	attemptTimes := make([]time.Time, 0)
	var mu sync.Mutex

	localStreamer := &MockLocalStreamer{
		shouldError: stream.ErrMaxConnections,
		onAttempt: func(attempt int) {
			mu.Lock()
			attemptTimes = append(attemptTimes, time.Now())
			mu.Unlock()
		},
	}
	remoteRelay := &MockRemoteRelay{}

	sm := stream.NewStreamManager(stream.StreamManagerConfig{
		BabyUID:             "baby123",
		StateManager:        stateManager,
		LocalStreamer:       localStreamer,
		RemoteRelay:         remoteRelay,
		RestoreInitialDelay: 20 * time.Millisecond,
		RestoreMaxDelay:     100 * time.Millisecond,
		RestoreMultiplier:   2.0,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := sm.Start(ctx)
	require.NoError(t, err)

	// Wait for a few restore attempts
	time.Sleep(250 * time.Millisecond)
	cancel()

	mu.Lock()
	times := make([]time.Time, len(attemptTimes))
	copy(times, attemptTimes)
	mu.Unlock()

	// Should have multiple attempts with increasing delays
	require.GreaterOrEqual(t, len(times), 3, "Expected at least 3 attempts")

	// Verify delays are increasing (with some tolerance)
	if len(times) >= 3 {
		delay1 := times[2].Sub(times[1])
		delay0 := times[1].Sub(times[0])
		// Second delay should be >= first delay (backoff)
		assert.GreaterOrEqual(t, delay1.Milliseconds(), delay0.Milliseconds()*80/100,
			"Backoff should increase delays")
	}
}

func TestStreamManagerStop(t *testing.T) {
	stateManager := baby.NewStateManager()
	localStreamer := &MockLocalStreamer{shouldSucceed: true}
	remoteRelay := &MockRemoteRelay{}

	sm := stream.NewStreamManager(stream.StreamManagerConfig{
		BabyUID:       "baby123",
		StateManager:  stateManager,
		LocalStreamer: localStreamer,
		RemoteRelay:   remoteRelay,
	})

	ctx := context.Background()
	err := sm.Start(ctx)
	require.NoError(t, err)

	sm.Stop()

	assert.Equal(t, baby.StreamType_None, sm.GetCurrentType())
}

func TestStreamManagerStopRemote(t *testing.T) {
	stateManager := baby.NewStateManager()
	localStreamer := &MockLocalStreamer{shouldError: stream.ErrMaxConnections}
	remoteRelay := &MockRemoteRelay{}

	sm := stream.NewStreamManager(stream.StreamManagerConfig{
		BabyUID:       "baby123",
		StateManager:  stateManager,
		LocalStreamer: localStreamer,
		RemoteRelay:   remoteRelay,
	})

	ctx := context.Background()
	err := sm.Start(ctx)
	require.NoError(t, err)
	assert.Equal(t, baby.StreamType_Remote, sm.GetCurrentType())

	sm.Stop()

	assert.Equal(t, baby.StreamType_None, sm.GetCurrentType())
	assert.True(t, remoteRelay.IsStopped())
}

func TestStreamManagerContextCancellation(t *testing.T) {
	stateManager := baby.NewStateManager()
	localStreamer := &MockLocalStreamer{shouldError: stream.ErrMaxConnections}
	remoteRelay := &MockRemoteRelay{}

	sm := stream.NewStreamManager(stream.StreamManagerConfig{
		BabyUID:             "baby123",
		StateManager:        stateManager,
		LocalStreamer:       localStreamer,
		RemoteRelay:         remoteRelay,
		RestoreInitialDelay: 50 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	err := sm.Start(ctx)
	require.NoError(t, err)

	// Cancel context
	cancel()

	// Give time for cleanup
	time.Sleep(100 * time.Millisecond)

	// Restore goroutine should have stopped
	initialAttempts := localStreamer.GetAttempts()
	time.Sleep(100 * time.Millisecond)
	finalAttempts := localStreamer.GetAttempts()

	assert.Equal(t, initialAttempts, finalAttempts, "No new attempts after context cancellation")
}

func TestStreamManagerRemoteFailure(t *testing.T) {
	stateManager := baby.NewStateManager()
	localStreamer := &MockLocalStreamer{shouldError: stream.ErrMaxConnections}
	remoteRelay := &MockRemoteRelay{startErr: errors.New("remote connection failed")}

	sm := stream.NewStreamManager(stream.StreamManagerConfig{
		BabyUID:       "baby123",
		StateManager:  stateManager,
		LocalStreamer: localStreamer,
		RemoteRelay:   remoteRelay,
	})

	ctx := context.Background()
	err := sm.Start(ctx)

	// Should return error if both local and remote fail
	require.Error(t, err)
	assert.Equal(t, baby.StreamType_None, sm.GetCurrentType())
}

func TestStreamManagerLocalErrorNotMaxConnections(t *testing.T) {
	stateManager := baby.NewStateManager()
	localStreamer := &MockLocalStreamer{shouldError: errors.New("some other error")}
	remoteRelay := &MockRemoteRelay{}

	sm := stream.NewStreamManager(stream.StreamManagerConfig{
		BabyUID:       "baby123",
		StateManager:  stateManager,
		LocalStreamer: localStreamer,
		RemoteRelay:   remoteRelay,
	})

	ctx := context.Background()
	err := sm.Start(ctx)

	// Non-max-connections errors should not trigger remote failover
	// They should return the error
	require.Error(t, err)
	assert.Equal(t, baby.StreamType_None, sm.GetCurrentType())
	assert.False(t, remoteRelay.IsStarted())
}

func TestStreamManagerConcurrentAccess(t *testing.T) {
	stateManager := baby.NewStateManager()
	localStreamer := &MockLocalStreamer{shouldSucceed: true}
	remoteRelay := &MockRemoteRelay{}

	sm := stream.NewStreamManager(stream.StreamManagerConfig{
		BabyUID:       "baby123",
		StateManager:  stateManager,
		LocalStreamer: localStreamer,
		RemoteRelay:   remoteRelay,
	})

	ctx := context.Background()
	err := sm.Start(ctx)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sm.GetCurrentType()
		}()
	}
	wg.Wait()
	// Should not panic
}

func TestStreamManagerRestoreResetsBackoffOnSuccess(t *testing.T) {
	stateManager := baby.NewStateManager()
	attemptCount := 0
	var mu sync.Mutex

	localStreamer := &MockLocalStreamer{
		shouldError: stream.ErrMaxConnections,
	}

	// Set up onAttempt after localStreamer is created
	localStreamer.onAttempt = func(attempt int) {
		mu.Lock()
		attemptCount++
		count := attemptCount
		mu.Unlock()

		// Succeed on 3rd attempt
		if count >= 3 {
			localStreamer.SetShouldSucceed(true)
		}
	}

	remoteRelay := &MockRemoteRelay{}

	sm := stream.NewStreamManager(stream.StreamManagerConfig{
		BabyUID:             "baby123",
		StateManager:        stateManager,
		LocalStreamer:       localStreamer,
		RemoteRelay:         remoteRelay,
		RestoreInitialDelay: 20 * time.Millisecond,
		RestoreMaxDelay:     100 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := sm.Start(ctx)
	require.NoError(t, err)

	// Wait for restore to succeed
	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, baby.StreamType_Local, sm.GetCurrentType())
}

// TestStreamManagerIsMaxConnectionsError tests the error detection
func TestStreamManagerIsMaxConnectionsError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "ErrMaxConnections sentinel",
			err:      stream.ErrMaxConnections,
			expected: true,
		},
		{
			name:     "exact error string",
			err:      errors.New("Forbidden: Number of Mobile App connections above limit, declining connection"),
			expected: true,
		},
		{
			name:     "partial match",
			err:      errors.New("connections above limit"),
			expected: true,
		},
		{
			name:     "unrelated error",
			err:      errors.New("network timeout"),
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stream.IsMaxConnectionsError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestStreamManagerMonitorRestartsAfterRestore tests that monitoring restarts
// after local stream is restored, so a second drop triggers failover again
func TestStreamManagerMonitorRestartsAfterRestore(t *testing.T) {
	stateManager := baby.NewStateManager()
	localStreamer := &MockLocalStreamer{
		shouldSucceed: true,
		shouldError:   stream.ErrMaxConnections, // First call fails
	}
	remoteRelay := &MockRemoteRelay{}

	// After first call, clear error so restore succeeds
	localStreamer.onAttempt = func(attempt int) {
		if attempt == 1 {
			// After first call completes, clear error for subsequent calls
			localStreamer.mu.Lock()
			localStreamer.shouldError = nil
			localStreamer.mu.Unlock()
		}
	}

	sm := stream.NewStreamManager(stream.StreamManagerConfig{
		BabyUID:             "baby123",
		StateManager:        stateManager,
		LocalStreamer:       localStreamer,
		RemoteRelay:         remoteRelay,
		RestoreInitialDelay: 50 * time.Millisecond,
		PublisherTimeout:    50 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := sm.Start(ctx)
	require.NoError(t, err)
	assert.Equal(t, baby.StreamType_Remote, sm.GetCurrentType())

	// Wait for restore
	require.Eventually(t, func() bool { return sm.GetCurrentType() == baby.StreamType_Local },
		2*time.Second, 5*time.Millisecond, "local stream must be restored")

	// Make local fail again, so the manager stays on remote after the failover.
	// If the restore succeeded, the manager would go remote -> local (restore
	// after 50 ms) -> remote (the new monitor gets the stored Unhealthy state)
	// and the test could read the short "local" state between the two.
	localStreamer.mu.Lock()
	localStreamer.shouldError = stream.ErrMaxConnections
	localStreamer.mu.Unlock()

	// Simulate second local stream drop by updating state to unhealthy
	stateManager.Update("baby123", *baby.NewState().SetStreamState(baby.StreamState_Unhealthy))

	// Should have failed over to remote again
	require.Eventually(t, func() bool { return sm.GetCurrentType() == baby.StreamType_Remote },
		2*time.Second, 5*time.Millisecond, "must fail over to remote again")
}
