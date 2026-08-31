package host

import (
	"context"
	"runtime"
	"runtime/metrics"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
)

const bytesPerMiB = 1 << 20

// statsInterval matches the interval DefraDB reports its ingest queue on, so the two
// lines line up when reading a log.
const statsInterval = 30 * time.Second

// blockAdvance reports how far the chain head moved since the previous sample. Height is
// a gauge rather than a counter from zero, so the first sample has no baseline to
// subtract and reports no movement instead of the whole height.
func blockAdvance(prevBlock, block int64) int64 {
	if prevBlock <= 0 {
		return 0
	}
	return block - prevBlock
}

// pruneQueueLen reports the prune queue length, or -1 when the host runs without a
// pruner, so an absent pruner is distinguishable from an empty queue.
func (h *Host) pruneQueueLen() int {
	if h.pruneQueue == nil {
		return -1
	}
	return h.pruneQueue.Len()
}

// reportStats logs throughput until ctx is cancelled. Counters are reported as the
// change since the previous line rather than as running totals, so each line carries a
// rate. block, viewsActive, procMs, pruneQueue, heapMiB, nextGCMiB and goroutines are
// current values instead. /metrics exposes the same counters, but only as a snapshot.
func (h *Host) reportStats(ctx context.Context) {
	ticker := time.NewTicker(statsInterval)
	defer ticker.Stop()

	prev := h.metrics.GetSnapshot()
	prevBlock := h.GetCurrentBlock()

	// objects plus unused is the bytes held by in-use spans.
	samples := []metrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/unused:bytes"},
		{Name: "/gc/heap/goal:bytes"},
		{Name: "/gc/cycles/total:gc-cycles"},
	}

	// Seeded so the first line reports an interval delta like every line after it.
	metrics.Read(samples)
	prevGC := metricUint64(samples[3])

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cur := h.metrics.GetSnapshot()
			block := h.GetCurrentBlock()

			metrics.Read(samples)
			heapInUse := metricUint64(samples[0]) + metricUint64(samples[1])
			gcGoal := metricUint64(samples[2])
			gcCycles := metricUint64(samples[3])

			logger.Sugar.Infof(
				"host stats: block=%d advance=%d docs=%d txs=%d logs=%d blockSigs=%d "+
					"attested=%d attestErrors=%d sigVerified=%d sigFailed=%d "+
					"viewsActive=%d procMs=%.1f pruneQueue=%d "+
					"heapMiB=%d nextGCMiB=%d gc=%d goroutines=%d",
				block, blockAdvance(prevBlock, block),
				cur.DocumentsReceived-prev.DocumentsReceived,
				cur.TransactionsProcessed-prev.TransactionsProcessed,
				cur.LogsProcessed-prev.LogsProcessed,
				cur.BlockSignaturesProcessed-prev.BlockSignaturesProcessed,
				cur.AttestationsCreated-prev.AttestationsCreated,
				cur.AttestationErrors-prev.AttestationErrors,
				cur.SignatureVerifications-prev.SignatureVerifications,
				cur.SignatureFailures-prev.SignatureFailures,
				cur.ViewsActive,
				cur.LastProcessingTime,
				h.pruneQueueLen(),
				heapInUse/bytesPerMiB,
				gcGoal/bytesPerMiB,
				gcCycles-prevGC,
				runtime.NumGoroutine(),
			)

			prev, prevBlock, prevGC = cur, block, gcCycles
		}
	}
}

// metricUint64 reads a uint64 metric. An unrecognised name reads as zero, since
// Value.Uint64 panics on one.
func metricUint64(s metrics.Sample) uint64 {
	if s.Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return s.Value.Uint64()
}
