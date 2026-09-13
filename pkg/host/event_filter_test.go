package host

import (
	"context"
	"sync"
	"testing"

	"github.com/shinzonetwork/shinzo-host-client/hostconfig"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
)

func TestNewEventFilter_DisabledReturnsNil(t *testing.T) {
	f := NewEventFilter(hostconfig.EventFilterConfig{Enabled: false}, &hostconfig.FilterSet{})
	if f != nil {
		t.Fatal("expected a disabled filter to be nil")
	}
}

func TestNewEventFilter_NoEnabledGroupsAllowsEverythingInAllowlistMode(t *testing.T) {
	f := NewEventFilter(hostconfig.EventFilterConfig{Enabled: true, Mode: "allowlist"}, &hostconfig.FilterSet{})

	allowed := f.AllowReplication(context.Background(), constants.CollectionLog, "doc1", map[string]any{
		"address": "0xabc",
		"topics":  []string{"0x111"},
	})
	if !allowed {
		t.Fatal("expected no enabled groups in allowlist mode to allow everything")
	}
}

func TestAllowReplication_AllowlistMatchesByContract(t *testing.T) {
	rules := &hostconfig.FilterSet{
		Groups: []hostconfig.FilterGroup{{
			Name:    "usdc",
			Enabled: true,
			Contracts: []hostconfig.ContractFilter{{
				Address: "0xUSDC",
				Types:   []string{"log"},
			}},
		}},
	}
	f := NewEventFilter(hostconfig.EventFilterConfig{Enabled: true, Mode: "allowlist"}, rules)

	matched := f.AllowReplication(context.Background(), constants.CollectionLog, "doc1", map[string]any{
		"address": "0xUSDC",
	})
	if !matched {
		t.Fatal("expected a matching contract address to be allowed")
	}

	unmatched := f.AllowReplication(context.Background(), constants.CollectionLog, "doc2", map[string]any{
		"address": "0xOther",
	})
	if unmatched {
		t.Fatal("expected a non-matching contract address to be rejected in allowlist mode")
	}
}

func TestAllowReplication_BlocklistRejectsMatches(t *testing.T) {
	rules := &hostconfig.FilterSet{
		Groups: []hostconfig.FilterGroup{{
			Name:    "spam",
			Enabled: true,
			Contracts: []hostconfig.ContractFilter{{
				Address: "0xSPAM",
				Types:   []string{"log"},
			}},
		}},
	}
	f := NewEventFilter(hostconfig.EventFilterConfig{Enabled: true, Mode: "blocklist"}, rules)

	blocked := f.AllowReplication(context.Background(), constants.CollectionLog, "doc1", map[string]any{
		"address": "0xSPAM",
	})
	if blocked {
		t.Fatal("expected a matching contract address to be rejected in blocklist mode")
	}

	allowed := f.AllowReplication(context.Background(), constants.CollectionLog, "doc2", map[string]any{
		"address": "0xFine",
	})
	if !allowed {
		t.Fatal("expected a non-matching contract address to pass in blocklist mode")
	}
}

func TestAllowReplication_StructuralCollectionsAlwaysPass(t *testing.T) {
	rules := &hostconfig.FilterSet{
		Groups: []hostconfig.FilterGroup{{Name: "x", Enabled: true, Contracts: []hostconfig.ContractFilter{{Address: "0xOnly", Types: []string{"log"}}}}},
	}
	f := NewEventFilter(hostconfig.EventFilterConfig{Enabled: true, Mode: "allowlist"}, rules)

	for _, col := range []string{constants.CollectionBlockSignature, constants.CollectionSnapshotSignature} {
		if !f.AllowReplication(context.Background(), col, "doc", map[string]any{}) {
			t.Fatalf("expected %s to always pass regardless of filter rules", col)
		}
	}
}

func TestAllowReplication_BlockRangeGate(t *testing.T) {
	rules := &hostconfig.FilterSet{
		BlockRange: &hostconfig.BlockRangeFilter{Start: 100, End: 200},
	}
	f := NewEventFilter(hostconfig.EventFilterConfig{Enabled: true, Mode: "allowlist"}, rules)

	tooEarly := f.AllowReplication(context.Background(), constants.CollectionLog, "doc1", map[string]any{
		"blockNumber": uint64(50),
		"address":     "0xanything",
	})
	if tooEarly {
		t.Fatal("expected a block before the configured range to be rejected")
	}

	inRange := f.AllowReplication(context.Background(), constants.CollectionLog, "doc2", map[string]any{
		"blockNumber": uint64(150),
	})
	if !inRange {
		t.Fatal("expected a block inside the configured range to pass the range gate")
	}
}

func TestUpdateRules_TakesEffectImmediately(t *testing.T) {
	f := NewEventFilter(hostconfig.EventFilterConfig{Enabled: true, Mode: "allowlist"}, &hostconfig.FilterSet{
		Groups: []hostconfig.FilterGroup{{
			Name:      "only-a",
			Enabled:   true,
			Contracts: []hostconfig.ContractFilter{{Address: "0xA", Types: []string{"log"}}},
		}},
	})

	fields := map[string]any{"address": "0xB"}
	if f.AllowReplication(context.Background(), constants.CollectionLog, "doc1", fields) {
		t.Fatal("expected 0xB to be rejected before the rule update")
	}

	f.UpdateRules(&hostconfig.FilterSet{
		Groups: []hostconfig.FilterGroup{{
			Name:      "only-b",
			Enabled:   true,
			Contracts: []hostconfig.ContractFilter{{Address: "0xB", Types: []string{"log"}}},
		}},
	})

	if !f.AllowReplication(context.Background(), constants.CollectionLog, "doc2", fields) {
		t.Fatal("expected 0xB to be allowed immediately after UpdateRules, no restart")
	}
}

func TestUpdateRules_ConcurrentWithAllowReplication(t *testing.T) {
	f := NewEventFilter(hostconfig.EventFilterConfig{Enabled: true, Mode: "allowlist"}, &hostconfig.FilterSet{
		Groups: []hostconfig.FilterGroup{{
			Name:      "initial",
			Enabled:   true,
			Contracts: []hostconfig.ContractFilter{{Address: "0xA", Types: []string{"log"}}},
		}},
	})

	done := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					f.AllowReplication(context.Background(), constants.CollectionLog, "doc", map[string]any{"address": "0xA"})
				}
			}
		}()
	}

	for i := 0; i < 100; i++ {
		f.UpdateRules(&hostconfig.FilterSet{
			Groups: []hostconfig.FilterGroup{{
				Name:      "updated",
				Enabled:   true,
				Contracts: []hostconfig.ContractFilter{{Address: "0xA", Types: []string{"log"}}},
			}},
		})
	}

	close(done)
	wg.Wait()
}
