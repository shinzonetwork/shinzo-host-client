package host

import (
	"sync/atomic"

	"github.com/sourcenetwork/defradb/client"

	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/pruner"
)

// RetentionRule is the client.RetentionRule for the collections the pruners delete by height. A
// collection's floor is its chain's pruner cutoff.
type RetentionRule struct {
	chains []prunedChain
	// floors maps a collection ID to the field holding its documents' block height and its chain's
	// cutoff.
	floors atomic.Pointer[map[string]collectionFloor]
}

// prunedChain is one chain's collections the pruner deletes by height, and the cutoff that chain's
// pruner publishes.
type prunedChain struct {
	collections pruner.CollectionConfig
	cutoff      *pruner.Cutoff
}

// collectionFloor is a collection's height field and its chain's cutoff.
type collectionFloor struct {
	field  string
	cutoff *pruner.Cutoff
}

// NewRetentionRule returns a rule for chains that covers no collection until ResolveCollections runs.
func NewRetentionRule(chains []prunedChain) *RetentionRule {
	return &RetentionRule{chains: chains}
}

// ResolveCollections maps the IDs of the chains' pruned collections among cols to their height fields
// and cutoffs, and returns the names of those it mapped. The rule is asked about collection IDs,
// which exist only once the schema is applied. A collection whose height field is missing or not an
// integer is left out, because the rule would refuse all its documents as having no height.
func (r *RetentionRule) ResolveCollections(cols []client.Collection) []string {
	byName := make(map[string]collectionFloor)
	for _, c := range r.chains {
		byName[c.collections.Block.Name] = collectionFloor{field: c.collections.Block.HeightField, cutoff: c.cutoff}
		for _, dep := range c.collections.Dependents {
			byName[dep.Name] = collectionFloor{field: dep.HeightField, cutoff: c.cutoff}
		}
	}

	floors := make(map[string]collectionFloor)
	var names []string
	for _, col := range cols {
		f, ok := byName[col.Name()]
		if !ok {
			continue
		}
		if def, found := col.Version().GetFieldByName(f.field); !found || def.Kind != client.FieldKind_NILLABLE_INT {
			logger.Sugar.Warnf("Retention rule skips %s: it has no integer field %s", col.Name(), f.field)
			continue
		}
		floors[col.CollectionID()] = f
		names = append(names, col.Name())
	}
	r.floors.Store(&floors)
	return names
}

// RetentionFloor implements client.RetentionRule. A collection's floor is zero until its chain's
// pruner first sets its cutoff.
func (r *RetentionRule) RetentionFloor(collectionID string) (string, int64, bool) {
	floors := r.floors.Load()
	if floors == nil {
		return "", 0, false
	}
	f, ok := (*floors)[collectionID]
	if !ok {
		return "", 0, false
	}
	return f.field, f.cutoff.Load(), true
}
