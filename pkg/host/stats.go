package host

import (
	"context"
	"fmt"
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

// reportStats logs throughput until ctx is cancelled: one host line, then one line per configured
// chain. Counters are reported as the change since the previous line rather than as running totals,
// so each line carries a rate. block, viewsActive, procMs, heapMiB, nextGCMiB and goroutines are
// current values instead, and a chain's queuePeak is the deepest its attestation queue got since the
// previous line. A chain's block is the newest block it attested; the host line repeats it only on a
// host configured with one chain. /metrics exposes the host counters, but only as a snapshot.
func (h *Host) reportStats(ctx context.Context) {
	ticker := time.NewTicker(statsInterval)
	defer ticker.Stop()

	prev := h.metrics.GetSnapshot()
	chains := h.metrics.Chains()
	prevBlocks := make([]int64, len(chains))
	prevDocs := make([]int64, len(chains))
	prevDrops := make([]int64, len(chains))
	for i, c := range chains {
		prevBlocks[i] = c.CurrentBlock()
		prevDocs[i], prevDrops[i] = c.DocumentsReceived.Load(), c.QueueDrops.Load()
	}

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
			blocks := make([]int64, len(chains))
			for i, c := range chains {
				blocks[i] = c.CurrentBlock()
			}
			blockFields := ""
			if len(chains) == 1 {
				blockFields = fmt.Sprintf("block=%d advance=%d ", blocks[0], blockAdvance(prevBlocks[0], blocks[0]))
			}

			metrics.Read(samples)
			heapInUse := metricUint64(samples[0]) + metricUint64(samples[1])
			gcGoal := metricUint64(samples[2])
			gcCycles := metricUint64(samples[3])

			logger.Sugar.Infof(
				"host stats: %sdocs=%d txs=%d logs=%d blockSigs=%d "+
					"attested=%d attestErrors=%d sigVerified=%d sigFailed=%d "+
					"viewsActive=%d procMs=%.1f "+
					"heapMiB=%d nextGCMiB=%d gc=%d goroutines=%d",
				blockFields,
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
				heapInUse/bytesPerMiB,
				gcGoal/bytesPerMiB,
				gcCycles-prevGC,
				runtime.NumGoroutine(),
			)

			for i, c := range chains {
				docs, drops := c.DocumentsReceived.Load(), c.QueueDrops.Load()
				logger.Sugar.Infof("chain stats: chain=%s block=%d advance=%d docs=%d queueDrops=%d queuePeak=%d",
					c.Prefix, blocks[i], blockAdvance(prevBlocks[i], blocks[i]), docs-prevDocs[i], drops-prevDrops[i], c.TakeQueuePeak())
				prevDocs[i], prevDrops[i] = docs, drops
			}

			prev, prevBlocks, prevGC = cur, blocks, gcCycles
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
