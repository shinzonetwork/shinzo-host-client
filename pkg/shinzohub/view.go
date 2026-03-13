package shinzohub

import (
	"fmt"

	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

// ViewRegisteredEvent represents a view registration event
type ViewRegisteredEvent struct {
	Key     string
	Creator string
	View    view.View
}

// EntityRegisteredEvent represents an entity registration event
type EntityRegisteredEvent struct {
	Key    string
	Owner  string
	DID    string
	Pid    string
	Entity string
}

func (vre *ViewRegisteredEvent) ToString() string {
	return fmt.Sprintf("ViewRegistered: key=%s, creator=%s, view=%s", vre.Key, vre.Creator, vre.View.Name)
}

func (ere *EntityRegisteredEvent) ToString() string {
	return fmt.Sprintf("EntityRegistered: key=%s, owner=%s, did=%s, pid=%s", ere.Key, ere.Owner, ere.DID, ere.Pid)
}
