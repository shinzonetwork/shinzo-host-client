package host

import (
	"context"
	"runtime/metrics"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/shinzonetwork/shinzo-host-client/pkg/pruner"
	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
)

func TestBlockAdvance(t *testing.T) {
	tests := []struct {
		name      string
		prevBlock int64
		block     int64
		want      int64
	}{
		{name: "no baseline", prevBlock: 0, block: 25_700_000, want: 0},
		{name: "steady state", prevBlock: 25_700_000, block: 25_700_003, want: 3},
		// A reorg or a restart onto a shorter chain moves the head backwards.
		{name: "head moved back", prevBlock: 25_700_003, block: 25_700_000, want: -3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, blockAdvance(tt.prevBlock, tt.block))
		})
	}
}

func TestPruneQueueLen(t *testing.T) {
	// -1 rather than 0, so a host running without a pruner is not read as one whose
	// queue is empty.
	h := &Host{}
	require.Equal(t, -1, h.pruneQueueLen(), "no pruner")

	q := pruner.NewEventQueue(pruner.DefaultCollectionConfig())
	h.pruneQueue = q
	require.Equal(t, 0, h.pruneQueueLen(), "empty queue")

	q.Push("Ethereum__Mainnet__Block", "bae-11111111-2222-3333-4444-555555555555")
	require.Equal(t, 1, h.pruneQueueLen(), "after one push")
}

// The reporter runs for the life of the host, so a cancelled context has to end it
// rather than leave it ticking. The wait is shorter than statsInterval, so a reporter
// that only stops on its own tick fails here.
func TestReportStatsStopsWithContext(t *testing.T) {
	h := &Host{metrics: server.NewHostMetrics()}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		h.reportStats(ctx)
		close(done)
	}()
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("reportStats did not return after its context was cancelled")
	}
}

// An unrecognised name would panic Value.Uint64, and reportStats runs on its own
// goroutine.
func TestMetricUint64(t *testing.T) {
	samples := []metrics.Sample{
		{Name: "/gc/cycles/total:gc-cycles"},
		{Name: "/gc/cycles/total:gc-cycles-not-a-metric"},
	}
	metrics.Read(samples)

	require.Equal(t, metrics.KindUint64, samples[0].Value.Kind())
	require.Equal(t, samples[0].Value.Uint64(), metricUint64(samples[0]))

	require.Equal(t, metrics.KindBad, samples[1].Value.Kind())
	require.NotPanics(t, func() { metricUint64(samples[1]) })
	require.Zero(t, metricUint64(samples[1]))
}
