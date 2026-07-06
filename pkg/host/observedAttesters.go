package host

import (
	"sort"
	"sync"
	"time"
)

// DefaultAttesterWindow is how far back Snapshot looks for live attesters when no
// window is configured. It must exceed the gap between an indexer's attestations
// so a briefly-quiet one is not dropped, while still aging out one that has left.
const DefaultAttesterWindow = 5 * time.Minute

// observedAttesters tracks the indexers a host has seen attesting, each with the
// time it was last observed. Snapshot returns those seen within the window, so a
// served query records the attesting set the host currently sees across the
// network rather than the indexers for the specific blocks it returned.
//
// Observe runs on the block-signature workers and Snapshot on the serve path, so
// access is guarded by a mutex.
type observedAttesters struct {
	window   time.Duration
	now      func() time.Time
	mu       sync.Mutex
	lastSeen map[string]time.Time
}

// newObservedAttesters returns a set that treats an attester as live for window
// after its last observation.
func newObservedAttesters(window time.Duration) *observedAttesters {
	return &observedAttesters{
		window:   window,
		now:      time.Now,
		lastSeen: make(map[string]time.Time),
	}
}

// Observe records that identity attested now. An empty identity is ignored.
func (o *observedAttesters) Observe(identity string) {
	if identity == "" {
		return
	}
	o.mu.Lock()
	o.lastSeen[identity] = o.now()
	o.mu.Unlock()
}

// Snapshot returns the identities seen within the window, sorted for a stable
// record, and drops those that have aged out.
func (o *observedAttesters) Snapshot() []string {
	cutoff := o.now().Add(-o.window)
	o.mu.Lock()
	defer o.mu.Unlock()

	live := make([]string, 0, len(o.lastSeen))
	for id, seen := range o.lastSeen {
		if seen.Before(cutoff) {
			delete(o.lastSeen, id)
			continue
		}
		live = append(live, id)
	}
	sort.Strings(live)
	return live
}
