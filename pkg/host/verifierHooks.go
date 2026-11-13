package host

import (
	"context"
	"net/http"
	"time"

	"github.com/shinzonetwork/shinzo-evidence-codec/rule"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/verifier"
)

// verifierHooks returns the hooks of the verifier of the tokens-mode chain c. A verified block is the
// host observing its agreeing signers attesting on that chain, which the served-query record reads
// when recording is on; a tokens-mode chain has no signature workers to observe them.
func (h *Host) verifierHooks(c chain.Config, client *http.Client) verifier.Hooks {
	m := h.chainMetrics(c.Prefix)
	list := generatorDocuments(c.Generators, client)
	return verifier.Hooks{
		Verified: func(b rule.Block, signers []string) {
			if m != nil {
				height := int64(b.Number) //nolint:gosec // the verifier's heights count up by one from the int64 start.block
				m.UpdateCurrentBlock(height)
				m.RecordVerified(height)
			}
			set := h.attesters[c.Prefix]
			if set == nil {
				return
			}
			for _, s := range signers {
				set.Observe(s)
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
		Documents: func(ctx context.Context, from, to uint64) (map[string][]string, error) {
			if m != nil {
				m.Repairs.Add(1)
			}
			return list(ctx, from, to)
		},
	}
}
