package main

import (
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

// fakeRunner is a controllable utils.GracefulRunner.
type fakeRunner struct {
	done        chan struct{} // closed when the runner has finished
	finish      sync.Once
	cancelled   chan struct{} // closed when Cancel is called
	cancelOnce  sync.Once
	cleanupWait chan struct{} // if non-nil, Cancel blocks until it is closed
	wasCanceled bool
	mu          sync.Mutex
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{done: make(chan struct{}), cancelled: make(chan struct{})}
}

func (r *fakeRunner) stop() { r.finish.Do(func() { close(r.done) }) }

func (r *fakeRunner) Wait() (bool, error) {
	<-r.done
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.wasCanceled, nil
}

func (r *fakeRunner) Cancel() {
	r.cancelOnce.Do(func() {
		r.mu.Lock()
		r.wasCanceled = true
		r.mu.Unlock()
		close(r.cancelled)
	})
	if r.cleanupWait != nil {
		<-r.cleanupWait
	}
	r.stop()
}

func runWait(t *testing.T, interrupt chan os.Signal, runner *fakeRunner) <-chan int {
	t.Helper()
	res := make(chan int, 1)
	go func() { res <- waitForShutdown(interrupt, runner) }()
	return res
}

func expectCode(t *testing.T, res <-chan int, zero bool) {
	t.Helper()
	select {
	case code := <-res:
		if zero && code != 0 {
			t.Fatalf("expected exit code 0, got %d", code)
		}
		if !zero && code == 0 {
			t.Fatal("expected non-zero exit code, got 0")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitForShutdown did not return in time")
	}
}

func TestSigtermCancelsAndExitsZero(t *testing.T) {
	interrupt := make(chan os.Signal, 2)
	runner := newFakeRunner()
	res := runWait(t, interrupt, runner)

	interrupt <- syscall.SIGTERM
	expectCode(t, res, true)

	select {
	case <-runner.cancelled:
	default:
		t.Fatal("Cancel was not called")
	}
}

func TestRunnerStoppingByItselfExitsNonZero(t *testing.T) {
	interrupt := make(chan os.Signal, 2)
	runner := newFakeRunner()
	res := runWait(t, interrupt, runner)

	runner.stop()
	expectCode(t, res, false)
}

func TestSecondSignalForcesExitDuringCleanup(t *testing.T) {
	interrupt := make(chan os.Signal, 2)
	runner := newFakeRunner()
	runner.cleanupWait = make(chan struct{})
	defer close(runner.cleanupWait)
	res := runWait(t, interrupt, runner)

	interrupt <- syscall.SIGTERM
	select {
	case <-runner.cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("Cancel was not called")
	}
	interrupt <- os.Interrupt
	expectCode(t, res, false)
}

func TestSignalThenRunnerFinishingExitsZero(t *testing.T) {
	interrupt := make(chan os.Signal, 2)
	runner := newFakeRunner()
	runner.cleanupWait = make(chan struct{})
	res := runWait(t, interrupt, runner)

	interrupt <- os.Interrupt
	<-runner.cancelled
	// The runner finishes because of the cancel, before clean-up returns.
	runner.stop()
	time.Sleep(50 * time.Millisecond)
	close(runner.cleanupWait)
	expectCode(t, res, true)
}
