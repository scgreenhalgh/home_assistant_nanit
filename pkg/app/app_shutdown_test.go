package app

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/scgreenhalgh/home_assistant_nanit/pkg/baby"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/client"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/stream"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/utils"
)

// fakePool records how runWebsocket ends the stream.
type fakePool struct {
	requested chan struct{}
	ended     chan struct{} // closed when StopLocalStream or Close returns
	stopBlock chan struct{} // if set, StopLocalStream blocks until it is closed

	mu          sync.Mutex
	stops       int
	closes      int
	stopTimeout time.Duration // time left on the ctx StopLocalStream got
}

func newFakePool() *fakePool {
	return &fakePool{requested: make(chan struct{}), ended: make(chan struct{})}
}

func (p *fakePool) RequestLocalStream(context.Context) error { close(p.requested); return nil }

func (p *fakePool) StopLocalStream(ctx context.Context) error {
	p.mu.Lock()
	p.stops++
	if deadline, ok := ctx.Deadline(); ok {
		p.stopTimeout = time.Until(deadline)
	}
	p.mu.Unlock()
	if p.stopBlock != nil {
		<-p.stopBlock
	}
	close(p.ended)
	return nil
}

func (p *fakePool) Close() {
	p.mu.Lock()
	p.closes++
	p.mu.Unlock()
	close(p.ended)
}

// fakeLoop runs one websocket attempt like client.WebsocketConnectionManager does:
// the ready handler runs as a child of the attempt, which is a child of the loop.
type fakeLoop struct {
	handler  client.WebsocketConnectionHandler
	attemptC chan utils.GracefulContext
}

func (l *fakeLoop) WithReadyConnection(h client.WebsocketConnectionHandler) { l.handler = h }

func (l *fakeLoop) RunWithinContext(ctx utils.GracefulContext) {
	ctx.RunAsChild(func(attempt utils.GracefulContext) {
		attempt.RunAsChild(func(handlerCtx utils.GracefulContext) {
			l.handler(client.NewWebsocketConnection(nil), handlerCtx)
		})
		l.attemptC <- attempt
		<-attempt.Done()
	}).Wait()
}

// useFakePool makes runWebsocket use pool for the rest of the test.
func useFakePool(t *testing.T) func(*fakePool) {
	var mu sync.Mutex
	var current *fakePool
	old := newStreamPool
	newStreamPool = func(*App, string, *client.WebsocketConnection, *stream.RequestPacer) streamPool {
		mu.Lock()
		defer mu.Unlock()
		return current
	}
	t.Cleanup(func() { newStreamPool = old })
	return func(p *fakePool) { mu.Lock(); current = p; mu.Unlock() }
}

// startBaby runs the production wiring under a tree like Run -> handleBaby and
// returns once the stream pool was requested.
func startBaby(t *testing.T, pool *fakePool) (utils.GracefulRunner, utils.GracefulContext) {
	t.Helper()
	app := &App{Opts: Opts{RTMP: &RTMPOpts{}}, BabyStateManager: baby.NewStateManager()}
	loop := &fakeLoop{attemptC: make(chan utils.GracefulContext, 1)}
	root := utils.RunWithGracefulCancel(func(rootCtx utils.GracefulContext) {
		rootCtx.RunAsChild(func(babyCtx utils.GracefulContext) {
			app.runBabyWebsocket("baby", loop, babyCtx)
		})
		<-rootCtx.Done()
	})
	attempt := <-loop.attemptC
	select {
	case <-pool.requested:
	case <-time.After(2 * time.Second):
		t.Fatal("stream pool not requested")
	}
	return root, attempt
}

func TestRunWebsocketCleanupFollowsShutdown(t *testing.T) {
	const runs = 300
	setPool := useFakePool(t)
	for _, tc := range []struct {
		name     string
		shutdown bool
	}{
		{"shutdown sends STOPPED", true},
		{"attempt end only closes", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deadline := time.After(20 * time.Second)
			right := 0
			for i := 0; i < runs; i++ {
				pool := newFakePool()
				setPool(pool)
				root, attempt := startBaby(t, pool)
				if tc.shutdown {
					go root.Cancel()
				} else {
					go attempt.Fail(errors.New("server closed the connection"))
				}
				select {
				case <-pool.ended:
				case <-deadline:
					t.Fatalf("deadline after %d runs", i)
				}
				root.Cancel()

				pool.mu.Lock()
				if tc.shutdown {
					if pool.stops == 1 && pool.closes == 0 && pool.stopTimeout > 0 && pool.stopTimeout <= streamStopTimeout {
						right++
					}
				} else if pool.closes == 1 && pool.stops == 0 {
					right++
				}
				pool.mu.Unlock()
			}
			if right != runs {
				t.Fatalf("right in %d/%d runs", right, runs)
			}
		})
	}
}

func TestShutdownWaitsForStreamStop(t *testing.T) {
	setPool := useFakePool(t)
	pool := newFakePool()
	pool.stopBlock = make(chan struct{})
	setPool(pool)
	root, _ := startBaby(t, pool)

	cancelled := make(chan struct{})
	go func() {
		root.Cancel()
		close(cancelled)
	}()

	select {
	case <-cancelled:
		t.Fatal("shutdown returned while StopLocalStream was still running")
	case <-time.After(200 * time.Millisecond):
	}

	close(pool.stopBlock)
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not return after StopLocalStream")
	}
}

func TestShutdownBoundsTheWebsocketWait(t *testing.T) {
	old := websocketShutdownTimeout
	websocketShutdownTimeout = 100 * time.Millisecond
	defer func() { websocketShutdownTimeout = old }()

	setPool := useFakePool(t)
	pool := newFakePool()
	pool.stopBlock = make(chan struct{})
	defer close(pool.stopBlock)
	setPool(pool)
	root, _ := startBaby(t, pool)

	cancelled := make(chan struct{})
	go func() {
		root.Cancel()
		close(cancelled)
	}()

	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown blocked beyond the websocket wait bound")
	}
}
