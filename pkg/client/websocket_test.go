package client

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/scgreenhalgh/home_assistant_nanit/pkg/baby"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/session"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/utils"
)

func TestWebsocketBinaryMessageNilReadyState(t *testing.T) {
	// Test that receiving a binary message before connection is ready
	// does not cause a nil pointer panic

	store := session.NewSessionStore()
	babyStateManager := baby.NewStateManager()
	api := &NanitClient{SessionStore: store}

	manager := &WebsocketConnectionManager{
		BabyUID:          "test-baby",
		CameraUID:        "test-camera",
		Session:          store.Session,
		API:              api,
		BabyStateManager: babyStateManager,
		// readyState is nil (not connected yet)
	}

	// Simulate receiving a message - this should not panic
	// We'll call the internal logic that would be triggered by OnBinaryMessage

	// First verify readyState is nil
	manager.mu.RLock()
	readyState := manager.readyState
	manager.mu.RUnlock()

	if readyState != nil {
		t.Fatal("Expected readyState to be nil before connection")
	}

	// The handleBinaryMessage function should safely handle nil readyState
	// This tests the fix - if unfixed, this would panic
	done := make(chan bool, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("handleBinaryMessage panicked with nil readyState: %v", r)
			}
			done <- true
		}()
		manager.handleBinaryMessage([]byte{})
	}()

	select {
	case <-done:
		// Good - function completed without panic
	case <-time.After(time.Second):
		t.Fatal("handleBinaryMessage timed out")
	}
}

func TestWebsocketHandlerRegistration(t *testing.T) {
	// Test that handlers can be registered before connection is ready
	store := session.NewSessionStore()
	babyStateManager := baby.NewStateManager()
	api := &NanitClient{SessionStore: store}

	manager := &WebsocketConnectionManager{
		BabyUID:          "test-baby",
		CameraUID:        "test-camera",
		Session:          store.Session,
		API:              api,
		BabyStateManager: babyStateManager,
	}

	var handlerCalled bool
	var mu sync.Mutex

	manager.WithReadyConnection(func(conn *WebsocketConnection, ctx utils.GracefulContext) {
		mu.Lock()
		handlerCalled = true
		mu.Unlock()
	})

	// Handler should be registered
	manager.mu.RLock()
	numSubscribers := len(manager.readySubscribers)
	manager.mu.RUnlock()

	if numSubscribers != 1 {
		t.Errorf("Expected 1 subscriber, got %d", numSubscribers)
	}

	// Handler should not be called yet (no ready state)
	mu.Lock()
	called := handlerCalled
	mu.Unlock()

	if called {
		t.Error("Handler should not be called before connection is ready")
	}
}

// startAttempt runs closeAfterHandlers inside a real graceful context, with one ready
// handler whose cleanup blocks until releaseCleanup is closed. It returns the attempt
// context, the runner, and channels closed when the cleanup returned and when the
// socket was closed.
func startAttempt(t *testing.T, releaseCleanup <-chan struct{}) (utils.GracefulContext, utils.GracefulRunner, <-chan struct{}, <-chan struct{}) {
	t.Helper()
	attemptC := make(chan utils.GracefulContext, 1)
	cleanupDone := make(chan struct{})
	socketClosed := make(chan struct{})

	runner := utils.RunWithGracefulCancel(func(attempt utils.GracefulContext) {
		handlers := &handlerGroup{}
		notifyReadyHandler(func(_ *WebsocketConnection, ctx utils.GracefulContext) {
			<-ctx.Done()
			<-releaseCleanup
			close(cleanupDone)
		}, readyState{Context: attempt, Handlers: handlers})
		attemptC <- attempt
		closeAfterHandlers(attempt, handlers, func() { close(socketClosed) })
	})
	return <-attemptC, runner, cleanupDone, socketClosed
}

func TestCloseAfterHandlersWaitsForHandlerCleanup(t *testing.T) {
	release := make(chan struct{})
	_, runner, cleanupDone, socketClosed := startAttempt(t, release)

	go runner.Cancel()

	select {
	case <-socketClosed:
		t.Fatal("socket closed while the ready handler was still in its cleanup")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	select {
	case <-socketClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("socket not closed after the ready handler's cleanup returned")
	}
	select {
	case <-cleanupDone:
	default:
		t.Fatal("socket closed before the ready handler's cleanup returned")
	}
}

func TestCloseAfterHandlersBoundsTheWait(t *testing.T) {
	old := handlerCleanupTimeout
	handlerCleanupTimeout = 100 * time.Millisecond
	defer func() { handlerCleanupTimeout = old }()

	release := make(chan struct{})
	defer close(release)
	attempt, _, _, socketClosed := startAttempt(t, release)

	// Like a disconnect: fail the attempt without waiting for it
	attempt.Fail(errors.New("server closed the connection"))

	select {
	case <-socketClosed:
	case <-time.After(2 * time.Second):
		t.Fatal("socket not closed although the handler cleanup wait is bounded")
	}
}
