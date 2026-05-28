package shinzohub

import (
	"fmt"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

// ViewRegisteredEvent represents a successful view registration on ShinzoHub.
// The view module emits it once the IBC ack from sourcehub confirms the
// registration; the EVM-side ViewCreated log on the ViewRegistry precompile
// (0x210) carries the same state change.
type ViewRegisteredEvent struct {
	ViewID          string    // attr "view_id"
	ContractAddress string    // attr "contract_address": EVM address of the deployed view
	ViewName        string    // attr "view_name" (legacy single-word events only)
	Creator         string    // attr "creator": bech32 address of the view creator
	View            view.View // bundle decoded from the hub registry; populated by downstream hydration
}

// ToString returns a human-readable string representation of the ViewRegisteredEvent.
func (vre *ViewRegisteredEvent) ToString() string {
	return fmt.Sprintf("ViewRegistered: id=%s, address=%s, creator=%s",
		vre.ViewID, vre.ContractAddress, vre.Creator)
}
