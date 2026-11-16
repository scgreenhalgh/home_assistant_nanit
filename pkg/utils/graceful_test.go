package utils_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/scgreenhalgh/home_assistant_nanit/pkg/utils"
)

func TestGracefulRunner(t *testing.T) {
	var mu sync.Mutex
	out := ""

	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			<-childCtx.Done()
			time.Sleep(500 * time.Millisecond)
			mu.Lock()
			out = out + " sub_finished"
			mu.Unlock()
		})

		<-ctx.Done()
		time.Sleep(200 * time.Millisecond)
		mu.Lock()
		out = out + " main_finished"
		mu.Unlock()
	})

	time.Sleep(100 * time.Millisecond)
	runner.Cancel()
	// Wait for runner to complete before appending
	runner.Wait()
	mu.Lock()
	out = out + " after_cancel"
	mu.Unlock()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, " main_finished sub_finished after_cancel", out)
}

func TestGracefulRunnerFinished(t *testing.T) {
	var mu sync.Mutex
	out := ""
	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			time.Sleep(300 * time.Millisecond)
			mu.Lock()
			out = out + " sub_finished"
			mu.Unlock()
		})

		time.Sleep(200 * time.Millisecond)
		mu.Lock()
		out = out + " main_finished"
		mu.Unlock()
	})

	_, err := runner.Wait()
	assert.NoError(t, err)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, " main_finished sub_finished", out)
}

func TestGracefulRunnerFail(t *testing.T) {
	var mu sync.Mutex
	out := ""
	runner := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
		ctx.RunAsChild(func(childCtx utils.GracefulContext) {
			<-childCtx.Done()
			time.Sleep(200 * time.Millisecond)
			mu.Lock()
			out = out + " sub_finished"
			mu.Unlock()
		})

		for {
			select {
			case <-time.After(200 * time.Millisecond):
				ctx.Fail(errors.New("simulated failure"))
			case <-ctx.Done():
				time.Sleep(100 * time.Millisecond)
				mu.Lock()
				out = out + " main_finished"
				mu.Unlock()
				return
			}
		}
	})

	_, err := runner.Wait()
	assert.EqualError(t, err, "simulated failure")
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, " main_finished sub_finished", out)
}

// Wait on a child that already finished must not race the parent cancelling it.
func TestGracefulChildWaitRacesParentCancel(t *testing.T) {
	for i := 0; i < 200; i++ {
		waited := make(chan struct{})
		parent := utils.RunWithGracefulCancel(func(ctx utils.GracefulContext) {
			child := ctx.RunAsChild(func(utils.GracefulContext) {})
			go func() { child.Wait(); close(waited) }()
			<-ctx.Done()
		})
		parent.Cancel()
		select {
		case <-waited:
		case <-time.After(2 * time.Second):
			t.Fatal("child Wait did not return")
		}
	}
}
