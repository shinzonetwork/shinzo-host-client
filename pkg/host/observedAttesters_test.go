package host

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Snapshot returns every attester seen within the window, sorted for a stable
// record.
func TestObservedAttesters_SnapshotReturnsLiveSorted(t *testing.T) {
	o := newObservedAttesters(time.Minute)
	base := time.Unix(1_700_000_000, 0)
	o.now = func() time.Time { return base }

	o.Observe("0xcc")
	o.Observe("0xaa")
	o.Observe("0xbb")

	require.Equal(t, []string{"0xaa", "0xbb", "0xcc"}, o.Snapshot())
}

// An attester not seen within the window is excluded and pruned from the map.
func TestObservedAttesters_DropsAndPrunesExpired(t *testing.T) {
	o := newObservedAttesters(time.Minute)
	clk := time.Unix(1_700_000_000, 0)
	o.now = func() time.Time { return clk }

	o.Observe("0xaa")
	clk = clk.Add(2 * time.Minute) // past the window

	require.Empty(t, o.Snapshot(), "an attester past the window is not live")
	require.Empty(t, o.lastSeen, "the expired entry is pruned")
}

// Snapshot keeps attesters seen within the window and drops older ones in the
// same call.
func TestObservedAttesters_KeepsRecentDropsOld(t *testing.T) {
	o := newObservedAttesters(time.Minute)
	clk := time.Unix(1_700_000_000, 0)
	o.now = func() time.Time { return clk }

	o.Observe("0xold")
	clk = clk.Add(90 * time.Second) // 0xold now older than the 1m window
	o.Observe("0xrecent")

	require.Equal(t, []string{"0xrecent"}, o.Snapshot())
}

// Re-observing refreshes the last-seen time, so an attester that keeps signing
// stays live even past one window from its first sighting.
func TestObservedAttesters_ReObserveRefreshesLastSeen(t *testing.T) {
	o := newObservedAttesters(time.Minute)
	clk := time.Unix(1_700_000_000, 0)
	o.now = func() time.Time { return clk }

	o.Observe("0xaa")
	clk = clk.Add(40 * time.Second)
	o.Observe("0xaa")               // refresh
	clk = clk.Add(40 * time.Second) // 80s since first sighting, 40s since refresh

	require.Equal(t, []string{"0xaa"}, o.Snapshot(), "last-seen tracks the latest sighting, not the first")
}

// An empty identity is never recorded.
func TestObservedAttesters_IgnoresEmpty(t *testing.T) {
	o := newObservedAttesters(time.Minute)
	o.Observe("")
	require.Empty(t, o.Snapshot())
	require.Empty(t, o.lastSeen)
}

// Observe and Snapshot are safe to call concurrently (run with -race).
func TestObservedAttesters_ConcurrentAccess(t *testing.T) {
	o := newObservedAttesters(time.Minute)
	var wg sync.WaitGroup
	for i := range 50 {
		id := fmt.Sprintf("0x%02x", i)
		wg.Add(2)
		go func() { defer wg.Done(); o.Observe(id) }()
		go func() { defer wg.Done(); _ = o.Snapshot() }()
	}
	wg.Wait()
	require.Len(t, o.Snapshot(), 50, "every distinct attester observed within the window is live")
}
