package host

import (
	"context"
	"sync/atomic"

	"github.com/sourcenetwork/defradb/client"

	"github.com/shinzonetwork/shinzo-host-client/pkg/pruner"
)

// RetentionFilter rejects a replicated document whose block height is at or below the retention
// cutoff.
type RetentionFilter struct {
	collections pruner.CollectionConfig
	cutoff      *pruner.Cutoff
	// heightFields maps a collection ID to the field holding its documents' block height.
	heightFields atomic.Pointer[map[string]string]

	// One count per call, by outcome. A document received without its CAR is checked before the
	// fetch, with no field values, and again once the CAR arrives, so these count checks rather
	// than documents.
	beforeFetch, cutoffUnset, unmapped, noHeight, rejected, allowed atomic.Int64
}

// retentionStats is the filter's cutoff and outcome counts at one moment.
type retentionStats struct {
	cutoff                                                          int64
	beforeFetch, cutoffUnset, unmapped, noHeight, rejected, allowed int64
}

// NewRetentionFilter returns a filter that allows every document until ResolveCollections runs.
func NewRetentionFilter(collections pruner.CollectionConfig, cutoff *pruner.Cutoff) *RetentionFilter {
	return &RetentionFilter{collections: collections, cutoff: cutoff}
}

// ResolveCollections maps the IDs of the collections in the pruner's table among cols to their
// height fields and returns the names of those it mapped. The filter is called with collection IDs,
// which exist only once the schema is applied.
func (f *RetentionFilter) ResolveCollections(cols []client.Collection) []string {
	byName := map[string]string{f.collections.Block.Name: f.collections.Block.HeightField}
	for _, dep := range f.collections.Dependents {
		byName[dep.Name] = dep.HeightField
	}

	fields := make(map[string]string)
	var names []string
	for _, col := range cols {
		if field, ok := byName[col.Name()]; ok {
			fields[col.CollectionID()] = field
			names = append(names, col.Name())
		}
	}
	f.heightFields.Store(&fields)
	return names
}

// AllowReplication implements client.ReplicationFilter. A document is allowed when no cutoff is
// set, its collection is not in the pruner's table, or its height is not among the fields it
// arrived with.
func (f *RetentionFilter) AllowReplication(_ context.Context, collectionID, _ string, fields map[string]any) bool {
	if len(fields) == 0 {
		f.beforeFetch.Add(1)
		return true
	}
	cutoff := f.cutoff.Load()
	if cutoff == 0 {
		f.cutoffUnset.Add(1)
		return true
	}
	heightFields := f.heightFields.Load()
	if heightFields == nil {
		f.unmapped.Add(1)
		return true
	}
	field, ok := (*heightFields)[collectionID]
	if !ok {
		f.unmapped.Add(1)
		return true
	}
	height, ok := fieldUint64(fields, field)
	if !ok {
		f.noHeight.Add(1)
		return true
	}
	if height <= uint64(cutoff) { //nolint:gosec // cutoff is positive here
		f.rejected.Add(1)
		return false
	}
	f.allowed.Add(1)
	return true
}

// stats returns the current cutoff and outcome counts.
func (f *RetentionFilter) stats() retentionStats {
	return retentionStats{
		cutoff:      f.cutoff.Load(),
		beforeFetch: f.beforeFetch.Load(),
		cutoffUnset: f.cutoffUnset.Load(),
		unmapped:    f.unmapped.Load(),
		noHeight:    f.noHeight.Load(),
		rejected:    f.rejected.Load(),
		allowed:     f.allowed.Load(),
	}
}

// replicationFilters allows a document only when every one of its filters does.
type replicationFilters []client.ReplicationFilter

func (fs replicationFilters) AllowReplication(
	ctx context.Context,
	collectionID string,
	docID string,
	fields map[string]any,
) bool {
	for _, f := range fs {
		if !f.AllowReplication(ctx, collectionID, docID, fields) {
			return false
		}
	}
	return true
}
