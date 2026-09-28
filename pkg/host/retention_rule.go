package host

import (
	"sync/atomic"

	"github.com/sourcenetwork/defradb/client"

	"github.com/shinzonetwork/shinzo-host-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-host-client/pkg/pruner"
)

// RetentionRule is the client.RetentionRule for the collections the pruner deletes by height, with
// the pruner's cutoff as the floor.
type RetentionRule struct {
	collections pruner.CollectionConfig
	cutoff      *pruner.Cutoff
	// heightFields maps a collection ID to the field holding its documents' block height.
	heightFields atomic.Pointer[map[string]string]
}

// NewRetentionRule returns a rule that covers no collection until ResolveCollections runs.
func NewRetentionRule(collections pruner.CollectionConfig, cutoff *pruner.Cutoff) *RetentionRule {
	return &RetentionRule{collections: collections, cutoff: cutoff}
}

// ResolveCollections maps the IDs of the collections in the pruner's table among cols to their
// height fields and returns the names of those it mapped. The rule is asked about collection IDs,
// which exist only once the schema is applied. A collection whose height field is missing or not an
// integer is left out, because the rule would refuse all its documents as having no height.
func (r *RetentionRule) ResolveCollections(cols []client.Collection) []string {
	byName := map[string]string{r.collections.Block.Name: r.collections.Block.HeightField}
	for _, dep := range r.collections.Dependents {
		byName[dep.Name] = dep.HeightField
	}

	fields := make(map[string]string)
	var names []string
	for _, col := range cols {
		field, ok := byName[col.Name()]
		if !ok {
			continue
		}
		if def, found := col.Version().GetFieldByName(field); !found || def.Kind != client.FieldKind_NILLABLE_INT {
			logger.Sugar.Warnf("Retention rule skips %s: it has no integer field %s", col.Name(), field)
			continue
		}
		fields[col.CollectionID()] = field
		names = append(names, col.Name())
	}
	r.heightFields.Store(&fields)
	return names
}

// RetentionFloor implements client.RetentionRule. The floor is zero until the pruner first sets its
// cutoff.
func (r *RetentionRule) RetentionFloor(collectionID string) (string, int64, bool) {
	fields := r.heightFields.Load()
	if fields == nil {
		return "", 0, false
	}
	field, ok := (*fields)[collectionID]
	if !ok {
		return "", 0, false
	}
	return field, r.cutoff.Load(), true
}
