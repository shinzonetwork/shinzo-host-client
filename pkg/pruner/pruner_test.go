package pruner

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb/client"

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

// testDocID returns a docID client.NewDocIDFromString accepts; anything else is skipped
// before it reaches the purge.
func testDocID(n int) string {
	return fmt.Sprintf("bae-%08x-0000-0000-0000-000000000000", n)
}

// testDocIDs returns n distinct docIDs.
func testDocIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = testDocID(i + 1)
	}
	return ids
}

// A cycle can outlast the shutdown budget, and a Stop that waits for it without a bound
// leaves the container to be killed instead of exiting.
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

// Close can be reached more than once, and the second call must not close stopChan again.
func TestStopIsIdempotent(t *testing.T) {
	p := &Pruner{cfg: &Config{Enabled: true}, stopChan: make(chan struct{})}
	p.isRunning = true

	ctx := context.Background()
	p.Stop(ctx)

	require.NotPanics(t, func() { p.Stop(ctx) })
}

func TestPurgeStopsBeforeFirstBatch(t *testing.T) {
	p := &Pruner{cfg: &Config{Enabled: true}, stopChan: make(chan struct{})}
	close(p.stopChan)

	// defraNode is nil, so reaching the collection lookup would panic. Returning at the
	// stop check before any submission is what keeps this safe.
	submitted, err := p.purgeByDocIDs(context.Background(), logCollection, []string{testDocID(1)})
	require.ErrorIs(t, err, errStopped)
	require.Zero(t, submitted)
}

// DefraDB does not check the context inside a call, so the batch boundary is the only place
// a stop can be noticed. Handing over the whole list in one call would leave shutdown
// unbounded however short the timeout.
func TestPurgeSubmitsInBatchesAndStopsAtABoundary(t *testing.T) {
	// Pinned by value: this is how often a stop can be noticed, so raising it lengthens
	// shutdown and should take a deliberate test update rather than passing silently.
	require.Equal(t, 1000, purgeBatchSize)
	const docs = 2010

	t.Run("batches the whole list", func(t *testing.T) {
		var sizes []int
		p := &Pruner{cfg: &Config{Enabled: true}, stopChan: make(chan struct{})}
		p.purgeDocs = func(_ context.Context, ids []client.DocID) error {
			sizes = append(sizes, len(ids))
			return nil
		}

		submitted, err := p.purgeByDocIDs(context.Background(), logCollection, testDocIDs(docs))
		require.NoError(t, err)
		require.Equal(t, int64(docs), submitted)
		require.Equal(t, []int{1000, 1000, 10}, sizes,
			"the list must be handed over in bounded batches, not in one call")
	})

	t.Run("stops at the next boundary", func(t *testing.T) {
		p := &Pruner{cfg: &Config{Enabled: true}, stopChan: make(chan struct{})}
		calls := 0
		p.purgeDocs = func(_ context.Context, _ []client.DocID) error {
			calls++
			close(p.stopChan) // the pruner is told to stop while this batch is in flight
			return nil
		}

		submitted, err := p.purgeByDocIDs(context.Background(), logCollection, testDocIDs(docs))
		require.ErrorIs(t, err, errStopped)
		require.Equal(t, 1, calls, "no batch may start after the stop")
		require.Equal(t, int64(1000), submitted, "the batch that did run is still reported")
	})
}

func TestPurgeStopsOnCancelledContext(t *testing.T) {
	p := &Pruner{cfg: &Config{Enabled: true}, stopChan: make(chan struct{})}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	submitted, err := p.purgeByDocIDs(ctx, logCollection, []string{testDocID(1)})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, submitted)
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

// Zero is a valid block number, so an unreadable value has to be distinguishable from it.
func TestParseBlockNumber(t *testing.T) {
	cases := []struct {
		name   string
		input  any
		want   int64
		parsed bool
	}{
		{"float64 as defradb returns it", float64(42), 42, true},
		{"int64", int64(42), 42, true},
		{"int", 42, 42, true},
		{"genuine block zero", float64(0), 0, true},
		{"absent field", nil, 0, false},
		{"string", "42", 0, false},
		{"bool", true, 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, parsed := parseBlockNumber(tc.input)
			require.Equal(t, tc.parsed, parsed)
			require.Equal(t, tc.want, got)
		})
	}
}
