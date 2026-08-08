package host

import (
	"context"
	"runtime"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
)

const bytesPerMiB = 1 << 20

// statsInterval matches the interval DefraDB reports its ingest queue on, so the two
// lines line up when reading a log.
const statsInterval = 30 * time.Second

// reportStats logs throughput until ctx is cancelled. Counters are reported as the
// change since the previous line rather than as running totals, so each line carries a
// rate. /metrics exposes the same counters, but only as a snapshot.
func (h *Host) reportStats(ctx context.Context) {
	ticker := time.NewTicker(statsInterval)
	defer ticker.Stop()

	prev := h.metrics.GetSnapshot()
	prevBlock := h.GetCurrentBlock()

	// Seeded so the first line reports an interval delta like every line after it.
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	prevGC := mem.NumGC

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cur := h.metrics.GetSnapshot()
			block := h.GetCurrentBlock()

			// The pruner is optional, so -1 distinguishes "no pruner" from an empty queue.
			// Its length has to cross MaxDocs before anything is pruned, which is what
			// says whether the store is on track to be bounded.
			pruneQueued := -1
			if h.pruneQueue != nil {
				pruneQueued = h.pruneQueue.Len()
			}

			// Brief stop-the-world, acceptable once per interval. Read here because a
			// deployment may not expose pprof, leaving no other view of the heap.
			runtime.ReadMemStats(&mem)

			// Block height is a gauge, not a counter from zero, so it needs a baseline
			// before a delta means anything.
			advance := int64(0)
			if prevBlock > 0 {
				advance = block - prevBlock
			}

			logger.Sugar.Infof(
				"host stats: block=%d advance=%d docs=%d blocks=%d txs=%d logs=%d blockSigs=%d "+
					"attested=%d attestErrors=%d sigVerified=%d sigFailed=%d "+
					"viewsActive=%d procMs=%.1f pruneQueue=%d "+
					"heapMiB=%d nextGCMiB=%d gc=%d goroutines=%d",
				block, advance,
				cur.DocumentsReceived-prev.DocumentsReceived,
				cur.BlocksProcessed-prev.BlocksProcessed,
				cur.TransactionsProcessed-prev.TransactionsProcessed,
				cur.LogsProcessed-prev.LogsProcessed,
				cur.BlockSignaturesProcessed-prev.BlockSignaturesProcessed,
				cur.AttestationsCreated-prev.AttestationsCreated,
				cur.AttestationErrors-prev.AttestationErrors,
				cur.SignatureVerifications-prev.SignatureVerifications,
				cur.SignatureFailures-prev.SignatureFailures,
				cur.ViewsActive,
				cur.LastProcessingTime,
				pruneQueued,
				mem.HeapInuse/bytesPerMiB,
				mem.NextGC/bytesPerMiB,
				mem.NumGC-prevGC,
				runtime.NumGoroutine(),
			)

			prev, prevBlock, prevGC = cur, block, mem.NumGC
		}
	}
}
