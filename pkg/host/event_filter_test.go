package host

import (
	"context"
	"sync"
	"testing"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
)

func TestNewEventFilter_DisabledReturnsNil(t *testing.T) {
	f := NewEventFilter(config.EventFilterConfig{Enabled: false}, &config.FilterSet{})
	if f != nil {
		t.Fatal("expected a disabled filter to be nil")
	}
}

func TestNewEventFilter_NoEnabledGroupsAllowsEverythingInAllowlistMode(t *testing.T) {
	f := NewEventFilter(config.EventFilterConfig{Enabled: true, Mode: "allowlist"}, &config.FilterSet{})

	allowed := f.AllowReplication(context.Background(), constants.CollectionLog, "doc1", map[string]any{
		"address": "0xabc",
		"topics":  []string{"0x111"},
	})
	if !allowed {
		t.Fatal("expected no enabled groups in allowlist mode to allow everything")
	}
}

func TestAllowReplication_AllowlistMatchesByContract(t *testing.T) {
	rules := &config.FilterSet{
		Groups: []config.FilterGroup{{
			Name:    "usdc",
			Enabled: true,
			Contracts: []config.ContractFilter{{
				Address: "0xUSDC",
				Types:   []string{"log"},
			}},
		}},
	}
	f := NewEventFilter(config.EventFilterConfig{Enabled: true, Mode: "allowlist"}, rules)

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
	rules := &config.FilterSet{
		Groups: []config.FilterGroup{{
			Name:    "spam",
			Enabled: true,
			Contracts: []config.ContractFilter{{
				Address: "0xSPAM",
				Types:   []string{"log"},
			}},
		}},
	}
	f := NewEventFilter(config.EventFilterConfig{Enabled: true, Mode: "blocklist"}, rules)

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
	rules := &config.FilterSet{
		Groups: []config.FilterGroup{{Name: "x", Enabled: true, Contracts: []config.ContractFilter{{Address: "0xOnly", Types: []string{"log"}}}}},
	}
	f := NewEventFilter(config.EventFilterConfig{Enabled: true, Mode: "allowlist"}, rules)

	for _, col := range []string{constants.CollectionBlockSignature, constants.CollectionSnapshotSignature} {
		if !f.AllowReplication(context.Background(), col, "doc", map[string]any{}) {
			t.Fatalf("expected %s to always pass regardless of filter rules", col)
		}
	}
}

func TestAllowReplication_BlockRangeGate(t *testing.T) {
	rules := &config.FilterSet{
		BlockRange: &config.BlockRangeFilter{Start: 100, End: 200},
	}
	f := NewEventFilter(config.EventFilterConfig{Enabled: true, Mode: "allowlist"}, rules)

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
	f := NewEventFilter(config.EventFilterConfig{Enabled: true, Mode: "allowlist"}, &config.FilterSet{
		Groups: []config.FilterGroup{{
			Name:      "only-a",
			Enabled:   true,
			Contracts: []config.ContractFilter{{Address: "0xA", Types: []string{"log"}}},
		}},
	})

	fields := map[string]any{"address": "0xB"}
	if f.AllowReplication(context.Background(), constants.CollectionLog, "doc1", fields) {
		t.Fatal("expected 0xB to be rejected before the rule update")
	}

	f.UpdateRules(&config.FilterSet{
		Groups: []config.FilterGroup{{
			Name:      "only-b",
			Enabled:   true,
			Contracts: []config.ContractFilter{{Address: "0xB", Types: []string{"log"}}},
		}},
	})

	if !f.AllowReplication(context.Background(), constants.CollectionLog, "doc2", fields) {
		t.Fatal("expected 0xB to be allowed immediately after UpdateRules, no restart")
	}
}

func TestUpdateRules_ConcurrentWithAllowReplication(t *testing.T) {
	f := NewEventFilter(config.EventFilterConfig{Enabled: true, Mode: "allowlist"}, &config.FilterSet{
		Groups: []config.FilterGroup{{
			Name:      "initial",
			Enabled:   true,
			Contracts: []config.ContractFilter{{Address: "0xA", Types: []string{"log"}}},
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
		f.UpdateRules(&config.FilterSet{
			Groups: []config.FilterGroup{{
				Name:      "updated",
				Enabled:   true,
				Contracts: []config.ContractFilter{{Address: "0xA", Types: []string{"log"}}},
			}},
		})
	}

	close(done)
	wg.Wait()
}

func TestFieldString(t *testing.T) {
	tests := []struct {
		name   string
		fields map[string]any
		want   string
		wantOK bool
	}{
		{"present string", map[string]any{"addr": "0xABC"}, "0xABC", true},
		{"missing key", map[string]any{}, "", false},
		{"non-string value", map[string]any{"addr": 123}, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := fieldString(tt.fields, "addr")
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("fieldString() = (%q, %v), want (%q, %v)", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestFieldUint64(t *testing.T) {
	tests := []struct {
		name   string
		fields map[string]any
		want   uint64
		wantOK bool
	}{
		{"int64 value", map[string]any{"num": int64(42)}, 42, true},
		{"uint64 value", map[string]any{"num": uint64(100)}, 100, true},
		{"float64 value", map[string]any{"num": float64(55)}, 55, true},
		{"int value", map[string]any{"num": 77}, 77, true},
		{"missing key", map[string]any{}, 0, false},
		{"unsupported type", map[string]any{"num": "not-a-number"}, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := fieldUint64(tt.fields, "num")
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("fieldUint64() = (%d, %v), want (%d, %v)", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestFieldStringSlice(t *testing.T) {
	tests := []struct {
		name   string
		fields map[string]any
		want   []string
	}{
		{"[]string value", map[string]any{"topics": []string{"a", "b"}}, []string{"a", "b"}},
		{"[]any with strings", map[string]any{"topics": []any{"x", "y", "z"}}, []string{"x", "y", "z"}},
		{"missing key", map[string]any{}, nil},
		{"unsupported type", map[string]any{"topics": 42}, nil},
		{"mixed types", map[string]any{"topics": []any{"str1", 42, "str2"}}, []string{"str1", "str2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fieldStringSlice(tt.fields, "topics")
			if len(got) != len(tt.want) {
				t.Fatalf("fieldStringSlice() = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("fieldStringSlice() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}
