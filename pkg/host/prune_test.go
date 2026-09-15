package host

import (
	"slices"
	"testing"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
)

func TestPruneCollections_MatchesTheRealSchema(t *testing.T) {
	got := pruneCollections()

	if got.BlockCollection != constants.CollectionBlock {
		t.Fatalf("expected block collection %q, got %q", constants.CollectionBlock, got.BlockCollection)
	}

	want := []string{
		constants.CollectionAccessListEntry,
		constants.CollectionLog,
		constants.CollectionTransaction,
		constants.CollectionBlockSignature,
		constants.CollectionAttestationRecord,
	}
	if !slices.Equal(got.DependentCollections, want) {
		t.Fatalf("expected dependent collections %v, got %v", want, got.DependentCollections)
	}

	if slices.Contains(got.DependentCollections, "Ethereum__Mainnet__BatchSignature") {
		t.Fatal("BatchSignature isn't a real collection in this schema, it shouldn't be pruned")
	}
}
