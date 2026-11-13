package host

import (
	"net/http"

	"github.com/shinzonetwork/shinzo-evidence-codec/rule"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/verifier"
)

// verifierHooks returns the hooks of the verifier of the tokens-mode chain c. A verified block is the
// host observing its agreeing signers attesting on that chain, which the served-query record reads
// when recording is on; a tokens-mode chain has no signature workers to observe them.
func (h *Host) verifierHooks(c chain.Config, client *http.Client) verifier.Hooks {
	return verifier.Hooks{
		Verified: func(_ rule.Block, signers []string) {
			set := h.attesters[c.Prefix]
			if set == nil {
				return
			}
			for _, s := range signers {
				set.Observe(s)
			}
		},
		Documents: generatorDocuments(c.Generators, client),
	}
}
