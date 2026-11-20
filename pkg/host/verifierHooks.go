package host

import (
	"net/http"

	"github.com/shinzonetwork/shinzo-host-client/pkg/chain"
	"github.com/shinzonetwork/shinzo-host-client/pkg/verifier"
)

// verifierHooks returns the hooks of the verifier of the tokens-mode chain c. Repair lists the
// documents to fetch from c's generators through client.
func (h *Host) verifierHooks(c chain.Config, client *http.Client) verifier.Hooks {
	return verifier.Hooks{
		Documents: generatorDocuments(c.Generators, client),
	}
}
