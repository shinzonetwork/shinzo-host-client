package host

import (
	"net/http"
	"time"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/verifier"
)

// verifierHooks returns the hooks of the verifier of the tokens-mode chain c: they record its progress
// in c's metrics, raise c's pruning floor, and list the documents repair fetches from c's generators
// through client.
func (h *Host) verifierHooks(c chain.Config, client *http.Client) verifier.Hooks {
	m := h.chainMetrics(c.Prefix)
	return verifier.Hooks{
		Verified: func(height uint64) {
			if m != nil {
				verified := int64(height) //nolint:gosec // the verifier's heights count up by one from the int64 start.block
				m.UpdateCurrentBlock(verified)
				m.RecordVerified(verified)
			}
		},
		Unverified: func(height uint64) {
			if floor := h.floors[c.Prefix]; floor != nil {
				floor.Raise(int64(height)) //nolint:gosec // the verifier's heights count up by one from the int64 start.block
			}
		},
		Held: func(height uint64) {
			if m != nil {
				m.RecordHeld(int64(height), time.Now()) //nolint:gosec // the verifier's heights count up by one from the int64 start.block
			}
		},
		Dropped: func(reason string) {
			if m != nil {
				m.RecordSignerDropped(reason)
			}
		},
		Unlisted: func() {
			if m != nil {
				m.UnlistedAttestations.Add(1)
			}
		},
		Repairing: func() {
			if m != nil {
				m.Repairs.Add(1)
			}
		},
		Documents: generatorDocuments(c.Generators, client),
	}
}
