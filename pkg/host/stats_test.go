package host

import (
	"context"
	"regexp"
	"runtime/metrics"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

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

// The reporter runs for the life of the host, so a cancelled context has to end it
// rather than leave it ticking. The wait is shorter than statsInterval, so a reporter
// that only stops on its own tick fails here.
func TestReportStatsStopsWithContext(t *testing.T) {
	h := &Host{metrics: server.NewHostMetrics()}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		h.reportStats(ctx, statsInterval, zap.NewNop().Sugar())
		close(done)
	}()
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("reportStats did not return after its context was cancelled")
	}
}

// Each chain line reports the documents received since the previous line, in total and per table, so
// the lines add up to what peers sent after the reporter started.
func TestReportStatsChainLineReportsChanges(t *testing.T) {
	c := &server.ChainMetrics{Prefix: "Testchain__Devnet"}
	h := &Host{metrics: server.NewHostMetrics(c)}
	// Sent before the reporter starts, so every line reports no change for it.
	c.RecordDocument("Block")

	core, logs := observer.New(zap.InfoLevel)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		h.reportStats(ctx, 5*time.Millisecond, zap.New(core).Sugar())
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	// sums adds up the docs and docsByTable fields of every chain line logged so far, and returns
	// the number of lines. It runs inside Eventually's goroutine, so a malformed line is reported
	// with assert rather than require.
	line := regexp.MustCompile(`docs=(\d+) docsByTable=(\S+)`)
	sums := func() (lines int, docs int64, byTable map[string]int64) {
		byTable = make(map[string]int64)
		for _, entry := range logs.FilterMessageSnippet("chain stats:").All() {
			lines++
			m := line.FindStringSubmatch(entry.Message)
			if !assert.NotNil(t, m, entry.Message) {
				continue
			}
			n, err := strconv.ParseInt(m[1], 10, 64)
			assert.NoError(t, err, entry.Message)
			docs += n
			for pair := range strings.SplitSeq(m[2], ",") {
				table, count, _ := strings.Cut(pair, ":")
				n, err := strconv.ParseInt(count, 10, 64)
				assert.NoError(t, err, entry.Message)
				byTable[table] += n
			}
		}
		return lines, docs, byTable
	}

	// The reporter reads its baseline when it starts, so documents count only after its first line.
	require.Eventually(t, func() bool {
		lines, _, _ := sums()
		return lines > 0
	}, 5*time.Second, time.Millisecond)
	for range 3 {
		c.RecordDocument("Log")
	}
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		_, docs, byTable := sums()
		assert.Equal(ct, int64(3), docs)
		assert.Equal(ct, map[string]int64{"Block": 0, "Log": 3}, byTable)
	}, 5*time.Second, time.Millisecond)

	for range 2 {
		c.RecordDocument("Transaction")
	}
	var seen int
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		lines, docs, byTable := sums()
		assert.Equal(ct, int64(5), docs)
		assert.Equal(ct, map[string]int64{"Block": 0, "Log": 3, "Transaction": 2}, byTable)
		seen = lines
	}, 5*time.Second, time.Millisecond)

	// Lines logged after the last document report no change.
	require.Eventually(t, func() bool {
		lines, _, _ := sums()
		return lines >= seen+2
	}, 5*time.Second, time.Millisecond)
	_, docs, byTable := sums()
	require.Equal(t, int64(5), docs)
	require.Equal(t, map[string]int64{"Block": 0, "Log": 3, "Transaction": 2}, byTable)
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

// A tokens-mode chain gets a verification line with its heights and what changed since the previous
// line; a full-mode chain gets none.
func TestReportStatsVerificationLine(t *testing.T) {
	tokens := &server.ChainMetrics{Prefix: "Bsc__Mainnet", Tokens: true}
	full := &server.ChainMetrics{Prefix: "Testchain__Devnet"}
	h := &Host{metrics: server.NewHostMetrics(tokens, full)}
	core, logs := observer.New(zap.InfoLevel)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		h.reportStats(ctx, 5*time.Millisecond, zap.New(core).Sugar())
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()
	// Recorded after the reporter read its baseline, so a later line reports each change once.
	require.Eventually(t, func() bool { return logs.FilterMessageSnippet("chain verification:").Len() > 0 }, time.Second, time.Millisecond)
	tokens.RecordVerified(100)
	tokens.RecordHeld(101, time.Now())
	tokens.RecordSignerDropped("below_k")
	tokens.UnlistedAttestations.Add(1)
	tokens.Repairs.Add(1)

	// The changes may fall into more than one line, so the deltas are summed; the heights are read
	// from the newest line. The sums must stay put over the lines after, which report no change.
	line := regexp.MustCompile(`verifiedHeight=(\d+) verified=(\d+) heldHeight=(\d+) heldFor=\S+ dropped=(\S*) unlisted=(\d+) repairs=(\d+)$`)
	reported := func() (lines int, ok bool) {
		var verified, unlisted, repairs, belowK int64
		var last []string
		for _, entry := range logs.FilterMessageSnippet("chain verification:").All() {
			m := line.FindStringSubmatch(entry.Message)
			if !assert.NotNil(t, m, entry.Message) {
				return lines, false
			}
			lines++
			last = m
			verified += atoi(t, m[2])
			unlisted += atoi(t, m[5])
			repairs += atoi(t, m[6])
			if count, found := strings.CutPrefix(m[4], "below_k:"); found {
				belowK += atoi(t, count)
			}
		}
		return lines, last[1] == "100" && last[3] == "101" && verified == 1 && unlisted == 1 && repairs == 1 && belowK == 1
	}
	var changedAt int
	require.Eventually(t, func() bool {
		lines, ok := reported()
		changedAt = lines
		return ok
	}, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		lines, _ := reported()
		return lines >= changedAt+3
	}, time.Second, time.Millisecond)
	_, ok := reported()
	require.True(t, ok, "a later line reported a change again")
	for _, entry := range logs.FilterMessageSnippet("chain verification:").All() {
		require.Contains(t, entry.Message, "chain=Bsc__Mainnet")
	}
}

// atoi parses a counter from a stats line, reporting a malformed one without stopping the caller.
func atoi(t *testing.T, s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	assert.NoError(t, err, s)
	return n
}
