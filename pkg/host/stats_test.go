package host

import (
	"context"
	"testing"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/pkg/pruner"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
)

func TestBlockAdvance(t *testing.T) {
	// No baseline yet: reporting the height as movement would show a jump of millions
	// on the first line.
	if got := blockAdvance(0, 25_700_000); got != 0 {
		t.Errorf("first sample: got %d, want 0", got)
	}

	if got := blockAdvance(25_700_000, 25_700_003); got != 3 {
		t.Errorf("steady state: got %d, want 3", got)
	}

	// A reorg or a restart onto a shorter chain moves the head backwards.
	if got := blockAdvance(25_700_003, 25_700_000); got != -3 {
		t.Errorf("head moved back: got %d, want -3", got)
	}
}

func TestPruneQueueLen(t *testing.T) {
	// -1 rather than 0, so a host running without a pruner is not read as one whose
	// queue is empty.
	h := &Host{}
	if got := h.pruneQueueLen(); got != -1 {
		t.Errorf("no pruner: got %d, want -1", got)
	}

	q := pruner.NewEventQueue(pruner.DefaultCollectionConfig())
	h.pruneQueue = q
	if got := h.pruneQueueLen(); got != 0 {
		t.Errorf("empty queue: got %d, want 0", got)
	}

	q.Push("Ethereum__Mainnet__Block", "bae-11111111-2222-3333-4444-555555555555")
	if got := h.pruneQueueLen(); got != 1 {
		t.Errorf("after one push: got %d, want 1", got)
	}
}

// The reporter runs for the life of the host, so a cancelled context has to end it
// rather than leave it ticking. The wait is shorter than statsInterval, so a reporter
// that only stops on its own tick fails here.
func TestReportStatsStopsWithContext(t *testing.T) {
	h := &Host{metrics: server.NewHostMetrics()}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		h.reportStats(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reportStats did not return after its context was cancelled")
	}
}
