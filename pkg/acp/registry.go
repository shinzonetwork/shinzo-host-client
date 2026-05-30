package acp

import (
	"github.com/shinzonetwork/shinzo-host-client/pkg/view"
)

// ViewRegistry resolves a GraphQL collection name to (a) whether the
// collection is a host-registered view that requires access-control gating
// and (b) the on-chain contract address used as the SourceHub
// relationship-tuple object_id. The two methods are kept separate so the
// middleware can fail closed on a registered view without an address
// rather than silently treat it as ungated.
type ViewRegistry interface {
	IsView(collectionName string) bool
	ContractAddress(collectionName string) (string, bool)
}

// managerAdapter narrows view.Manager to the ViewRegistry shape so the
// middleware depends only on the methods it uses.
type managerAdapter struct {
	m *view.Manager
}

// NewViewRegistry returns a ViewRegistry backed by the host's view.Manager.
func NewViewRegistry(m *view.Manager) ViewRegistry {
	return &managerAdapter{m: m}
}

func (a *managerAdapter) IsView(name string) bool {
	return a.m.IsActive(name)
}

func (a *managerAdapter) ContractAddress(name string) (string, bool) {
	return a.m.ContractAddress(name)
}
