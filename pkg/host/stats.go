package host

import (
	"context"
	"fmt"
	"maps"
	"runtime"
	"runtime/metrics"
	"slices"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/shinzonetwork/shinzo-host-client/pkg/server"
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

// reportStats logs throughput to log every interval until ctx is cancelled: one host line, then one
// line per configured chain. Counters are reported as the change since the previous line rather than
// as running totals, so each line carries a rate. block, viewsActive, procMs, heapMiB, nextGCMiB and
// goroutines are current values instead, and a chain's queuePeak is the deepest its attestation queue
// got since the previous line. A chain's docsByTable lists, as table:change, each table a peer has
// sent the chain a document to since the host started. A chain's block is the newest block it
// attested; the host line repeats it only on a host configured with one chain. /metrics exposes each
// chain's counters other than queueDrops as running totals.
func (h *Host) reportStats(ctx context.Context, interval time.Duration, log *zap.SugaredLogger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	chains := h.metrics.Chains()
	prevBlocks := make([]int64, len(chains))
	prevCounters := make([]server.ChainCounters, len(chains))
	prevDrops := make([]int64, len(chains))
	for i, c := range chains {
		prevBlocks[i] = c.CurrentBlock()
		prevCounters[i], prevDrops[i] = c.Counters(), c.QueueDrops.Load()
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

			log.Infof(
				"host stats: %sviewsActive=%d procMs=%.1f heapMiB=%d nextGCMiB=%d gc=%d goroutines=%d",
				blockFields,
				cur.ViewsActive,
				cur.LastProcessingTime,
				heapInUse/bytesPerMiB,
				gcGoal/bytesPerMiB,
				gcCycles-prevGC,
				runtime.NumGoroutine(),
			)

			for i, c := range chains {
				counters, drops := c.Counters(), c.QueueDrops.Load()
				prev := prevCounters[i]
				tables := slices.Sorted(maps.Keys(counters.DocumentsByTable))
				byTable := make([]string, len(tables))
				for j, table := range tables {
					byTable[j] = fmt.Sprintf("%s:%d", table, counters.DocumentsByTable[table]-prev.DocumentsByTable[table])
				}
				log.Infof(
					"chain stats: chain=%s block=%d advance=%d docs=%d docsByTable=%s blockSigs=%d "+
						"attested=%d attestErrors=%d sigVerified=%d sigFailed=%d queueDrops=%d queuePeak=%d",
					c.Prefix, blocks[i], blockAdvance(prevBlocks[i], blocks[i]),
					counters.DocumentsReceived-prev.DocumentsReceived,
					strings.Join(byTable, ","),
					counters.BlockSignaturesProcessed-prev.BlockSignaturesProcessed,
					counters.AttestationsCreated-prev.AttestationsCreated,
					counters.AttestationErrors-prev.AttestationErrors,
					counters.SignatureVerifications-prev.SignatureVerifications,
					counters.SignatureFailures-prev.SignatureFailures,
					drops-prevDrops[i], c.TakeQueuePeak(),
				)
				prevCounters[i], prevDrops[i] = counters, drops
			}

			prevBlocks, prevGC = blocks, gcCycles
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
