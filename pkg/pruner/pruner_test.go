package pruner

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
)

// testPrunerWithBlockedCycle returns a pruner whose cycle never finishes. Nothing in the
// purge path can be cancelled, so Stop has to cope with the wait never returning.
func testPrunerWithBlockedCycle() *Pruner {
	p := &Pruner{
		cfg:      &Config{Enabled: true},
		stopChan: make(chan struct{}),
	}
	p.isRunning = true
	p.wg.Add(1)
	return p
}

// The queue is saved after the wait, so a Stop that waits without a bound never reaches it
// and the queue is lost on every restart.
func TestStopReturnsWhenShutdownBudgetExpires(t *testing.T) {
	p := testPrunerWithBlockedCycle()
	defer p.wg.Done()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		p.Stop(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after its context expired")
	}
}

func TestStopSavesQueueWhenBudgetExpires(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prune_queue.gob")

	cfg := CollectionConfig{
		BlockCollection:      testBlockCollection,
		DependentCollections: []string{testLogCollection},
	}
	q := NewEventQueue(cfg)
	_, err := q.LoadFromFile(path) // sets the save path; the file does not exist yet
	require.NoError(t, err)
	q.Push(testBlockCollection, testDocID(1))
	q.Push(testLogCollection, testDocID(2))

	p := testPrunerWithBlockedCycle()
	defer p.wg.Done()
	p.queue = q

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	p.Stop(ctx)

	require.FileExists(t, path, "queue was not saved when the shutdown budget expired")

	restored := NewEventQueue(cfg)
	n, err := restored.LoadFromFile(path)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, 2, restored.Len())
}

// Close can be reached more than once, and the second call must not close stopChan again.
func TestStopIsIdempotent(t *testing.T) {
	p := &Pruner{cfg: &Config{Enabled: true}, stopChan: make(chan struct{})}
	p.isRunning = true

	ctx := context.Background()
	p.Stop(ctx)

	require.NotPanics(t, func() { p.Stop(ctx) })
}

// The pruner logs through the package-level sugared logger, which is nil until Init runs.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "pruner-logs")
	if err != nil {
		panic(err)
	}
	logger.Init(false, dir)

	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}
