package host

import (
	"context"
	"testing"

	"github.com/shinzonetwork/shinzo-host-client/config"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// NewEventReplicationFilter
// ---------------------------------------------------------------------------

func TestNewEventReplicationFilter(t *testing.T) {
	tests := []struct {
		name    string
		cfg     config.EventFilterConfig
		wantNil bool
	}{
		{
			name:    "disabled config returns nil",
			cfg:     config.EventFilterConfig{Enabled: false},
			wantNil: true,
		},
		{
			name:    "enabled config returns non-nil",
			cfg:     config.EventFilterConfig{Enabled: true},
			wantNil: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewEventReplicationFilter(tt.cfg)
			if tt.wantNil {
				require.Nil(t, f)
			} else {
				require.NotNil(t, f)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// AllowReplication
// ---------------------------------------------------------------------------

func TestAllowReplication(t *testing.T) {
	tests := []struct {
		name         string
		cfg          config.EventFilterConfig
		collectionID string
		fields       map[string]any
		want         bool
	}{
		{
			name: "block signature always allowed",
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeAllowlist,
			},
			collectionID: constants.CollectionBlockSignature,
			fields:       map[string]any{},
			want:         true,
		},
		{
			name: "snapshot signature always allowed",
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeAllowlist,
			},
			collectionID: constants.CollectionSnapshotSignature,
			fields:       map[string]any{},
			want:         true,
		},
		{
			name: "block collection uses allowBlock",
			cfg: config.EventFilterConfig{
				Enabled:    true,
				Mode:       filterModeAllowlist,
				BlockRange: &config.BlockRangeFilter{MinBlock: 100, MaxBlock: 200},
			},
			collectionID: constants.CollectionBlock,
			fields:       map[string]any{gqlFieldNumber: uint64(150)},
			want:         true,
		},
		{
			name: "block collection rejected by allowBlock",
			cfg: config.EventFilterConfig{
				Enabled:    true,
				Mode:       filterModeAllowlist,
				BlockRange: &config.BlockRangeFilter{MinBlock: 100, MaxBlock: 200},
			},
			collectionID: constants.CollectionBlock,
			fields:       map[string]any{gqlFieldNumber: uint64(50)},
			want:         false,
		},
		{
			name: "transaction uses matchesGroups",
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeAllowlist,
				Groups: []config.FilterGroup{
					{
						Enabled:   true,
						Contracts: []config.ContractFilter{{Address: "0xABC", Types: []string{colTypeTransaction}}},
					},
				},
			},
			collectionID: constants.CollectionTransaction,
			fields:       map[string]any{gqlFieldTo: "0xabc"},
			want:         true,
		},
		{
			name: "log uses matchesGroups",
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeAllowlist,
				Groups: []config.FilterGroup{
					{
						Enabled:   true,
						Contracts: []config.ContractFilter{{Address: "0xDEF", Types: []string{colTypeLog}}},
					},
				},
			},
			collectionID: constants.CollectionLog,
			fields:       map[string]any{gqlFieldAddress: "0xdef", gqlFieldTopics: []string{"0xtopic0"}},
			want:         true,
		},
		{
			name: "accessListEntry uses matchesGroups",
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeAllowlist,
				Groups: []config.FilterGroup{
					{
						Enabled:   true,
						Contracts: []config.ContractFilter{{Address: "0x123", Types: []string{colTypeAccessListEntry}}},
					},
				},
			},
			collectionID: constants.CollectionAccessListEntry,
			fields:       map[string]any{gqlFieldAddress: "0x123"},
			want:         true,
		},
		{
			name: "unknown collection always allowed",
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeAllowlist,
			},
			collectionID: "SomeUnknownCollection",
			fields:       map[string]any{},
			want:         true,
		},
		{
			name: "block range rejection for transaction",
			cfg: config.EventFilterConfig{
				Enabled:    true,
				Mode:       filterModeAllowlist,
				BlockRange: &config.BlockRangeFilter{MinBlock: 100, MaxBlock: 200},
				Groups: []config.FilterGroup{
					{
						Enabled:   true,
						Contracts: []config.ContractFilter{{Address: "0xABC", Types: []string{colTypeTransaction}}},
					},
				},
			},
			collectionID: constants.CollectionTransaction,
			fields:       map[string]any{gqlFieldTo: "0xabc", gqlFieldBlockNumber: uint64(50)},
			want:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewEventReplicationFilter(tt.cfg)
			require.NotNil(t, f)
			got := f.AllowReplication(context.Background(), tt.collectionID, "docID", tt.fields)
			require.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// allowBlock
// ---------------------------------------------------------------------------

func TestAllowBlock(t *testing.T) {
	tests := []struct {
		name   string
		cfg    config.EventFilterConfig
		fields map[string]any
		want   bool
	}{
		{
			name: "nil BlockRange allows all",
			cfg: config.EventFilterConfig{
				Enabled:    true,
				BlockRange: nil,
			},
			fields: map[string]any{gqlFieldNumber: uint64(999)},
			want:   true,
		},
		{
			name: "below min rejects",
			cfg: config.EventFilterConfig{
				Enabled:    true,
				BlockRange: &config.BlockRangeFilter{MinBlock: 100, MaxBlock: 200},
			},
			fields: map[string]any{gqlFieldNumber: uint64(50)},
			want:   false,
		},
		{
			name: "above max rejects when max > 0",
			cfg: config.EventFilterConfig{
				Enabled:    true,
				BlockRange: &config.BlockRangeFilter{MinBlock: 100, MaxBlock: 200},
			},
			fields: map[string]any{gqlFieldNumber: uint64(300)},
			want:   false,
		},
		{
			name: "in range allows",
			cfg: config.EventFilterConfig{
				Enabled:    true,
				BlockRange: &config.BlockRangeFilter{MinBlock: 100, MaxBlock: 200},
			},
			fields: map[string]any{gqlFieldNumber: uint64(150)},
			want:   true,
		},
		{
			name: "missing number field allows",
			cfg: config.EventFilterConfig{
				Enabled:    true,
				BlockRange: &config.BlockRangeFilter{MinBlock: 100, MaxBlock: 200},
			},
			fields: map[string]any{},
			want:   true,
		},
		{
			name: "max=0 means no upper limit",
			cfg: config.EventFilterConfig{
				Enabled:    true,
				BlockRange: &config.BlockRangeFilter{MinBlock: 100, MaxBlock: 0},
			},
			fields: map[string]any{gqlFieldNumber: uint64(999999)},
			want:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewEventReplicationFilter(tt.cfg)
			require.NotNil(t, f)
			got := f.allowBlock(tt.fields)
			require.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// allowTransaction
// ---------------------------------------------------------------------------

func TestAllowTransaction(t *testing.T) {
	tests := []struct {
		name   string
		cfg    config.EventFilterConfig
		fields map[string]any
		want   bool
	}{
		{
			name: testNameMatchingAddressAllowed,
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeAllowlist,
				Groups: []config.FilterGroup{
					{
						Enabled:   true,
						Contracts: []config.ContractFilter{{Address: "0xABC", Types: []string{colTypeTransaction}}},
					},
				},
			},
			fields: map[string]any{gqlFieldTo: "0xabc"},
			want:   true,
		},
		{
			name: "non-matching address rejected in allowlist",
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeAllowlist,
				Groups: []config.FilterGroup{
					{
						Enabled:   true,
						Contracts: []config.ContractFilter{{Address: "0xABC", Types: []string{colTypeTransaction}}},
					},
				},
			},
			fields: map[string]any{gqlFieldTo: "0xDEF"},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewEventReplicationFilter(tt.cfg)
			require.NotNil(t, f)
			got := f.allowTransaction(tt.fields)
			require.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// allowLog
// ---------------------------------------------------------------------------

func TestAllowLog(t *testing.T) {
	tests := []struct {
		name   string
		cfg    config.EventFilterConfig
		fields map[string]any
		want   bool
	}{
		{
			name: testNameMatchingAddressAllowed,
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeAllowlist,
				Groups: []config.FilterGroup{
					{
						Enabled:   true,
						Contracts: []config.ContractFilter{{Address: testHexLogUpper, Types: []string{colTypeLog}}},
					},
				},
			},
			fields: map[string]any{gqlFieldAddress: testHexLogLower, gqlFieldTopics: []string{}},
			want:   true,
		},
		{
			name: "matching topics allowed",
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeAllowlist,
				Groups: []config.FilterGroup{
					{
						Enabled: true,
						Topics:  []config.TopicFilter{{Topic0: testHexSigUpper}},
					},
				},
			},
			fields: map[string]any{gqlFieldAddress: "", gqlFieldTopics: []string{testHexSigLower}},
			want:   true,
		},
		{
			name: "non-matching rejected",
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeAllowlist,
				Groups: []config.FilterGroup{
					{
						Enabled:   true,
						Contracts: []config.ContractFilter{{Address: testHexLogUpper, Types: []string{colTypeLog}}},
					},
				},
			},
			fields: map[string]any{gqlFieldAddress: "0xOTHER", gqlFieldTopics: []string{}},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewEventReplicationFilter(tt.cfg)
			require.NotNil(t, f)
			got := f.allowLog(tt.fields)
			require.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// allowAccessListEntry
// ---------------------------------------------------------------------------

func TestAllowAccessListEntry(t *testing.T) {
	tests := []struct {
		name   string
		cfg    config.EventFilterConfig
		fields map[string]any
		want   bool
	}{
		{
			name: testNameMatchingAddressAllowed,
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeAllowlist,
				Groups: []config.FilterGroup{
					{
						Enabled:   true,
						Contracts: []config.ContractFilter{{Address: testHexAleUpper, Types: []string{colTypeAccessListEntry}}},
					},
				},
			},
			fields: map[string]any{gqlFieldAddress: testHexAleLower},
			want:   true,
		},
		{
			name: "non-matching address rejected",
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeAllowlist,
				Groups: []config.FilterGroup{
					{
						Enabled:   true,
						Contracts: []config.ContractFilter{{Address: testHexAleUpper, Types: []string{colTypeAccessListEntry}}},
					},
				},
			},
			fields: map[string]any{gqlFieldAddress: "0xOTHER"},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewEventReplicationFilter(tt.cfg)
			require.NotNil(t, f)
			got := f.allowAccessListEntry(tt.fields)
			require.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// matchesGroups
// ---------------------------------------------------------------------------

func TestMatchesGroups(t *testing.T) {
	tests := []struct {
		name    string
		cfg     config.EventFilterConfig
		address string
		topics  []string
		colType string
		want    bool
	}{
		{
			name: "allowlist with no enabled groups allows all",
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeAllowlist,
				Groups: []config.FilterGroup{
					{Enabled: false, Contracts: []config.ContractFilter{{Address: "0xABC", Types: []string{colTypeTransaction}}}},
				},
			},
			address: "0xABC",
			topics:  nil,
			colType: colTypeTransaction,
			want:    true,
		},
		{
			name: "allowlist matched allows",
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeAllowlist,
				Groups: []config.FilterGroup{
					{Enabled: true, Contracts: []config.ContractFilter{{Address: "0xABC", Types: []string{colTypeTransaction}}}},
				},
			},
			address: "0xabc",
			topics:  nil,
			colType: colTypeTransaction,
			want:    true,
		},
		{
			name: "allowlist unmatched rejects",
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeAllowlist,
				Groups: []config.FilterGroup{
					{Enabled: true, Contracts: []config.ContractFilter{{Address: "0xABC", Types: []string{colTypeTransaction}}}},
				},
			},
			address: "0xDEF",
			topics:  nil,
			colType: colTypeTransaction,
			want:    false,
		},
		{
			name: "blocklist matched rejects",
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeBlocklist,
				Groups: []config.FilterGroup{
					{Enabled: true, Contracts: []config.ContractFilter{{Address: "0xABC", Types: []string{colTypeTransaction}}}},
				},
			},
			address: "0xabc",
			topics:  nil,
			colType: colTypeTransaction,
			want:    false,
		},
		{
			name: "blocklist unmatched allows",
			cfg: config.EventFilterConfig{
				Enabled: true,
				Mode:    filterModeBlocklist,
				Groups: []config.FilterGroup{
					{Enabled: true, Contracts: []config.ContractFilter{{Address: "0xABC", Types: []string{colTypeTransaction}}}},
				},
			},
			address: "0xDEF",
			topics:  nil,
			colType: colTypeTransaction,
			want:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewEventReplicationFilter(tt.cfg)
			require.NotNil(t, f)
			got := f.matchesGroups(tt.address, tt.topics, tt.colType)
			require.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// groupMatches
// ---------------------------------------------------------------------------

func TestGroupMatches(t *testing.T) {
	tests := []struct {
		name    string
		cfg     config.EventFilterConfig
		group   config.FilterGroup
		address string
		topics  []string
		colType string
		want    bool
	}{
		{
			name: "contract address match case-insensitive",
			cfg:  config.EventFilterConfig{Enabled: true, CascadeFilters: false},
			group: config.FilterGroup{
				Enabled:   true,
				Contracts: []config.ContractFilter{{Address: "0xAbC", Types: []string{colTypeTransaction}}},
			},
			address: "0xabc",
			topics:  nil,
			colType: colTypeTransaction,
			want:    true,
		},
		{
			name: "topic match for logs",
			cfg:  config.EventFilterConfig{Enabled: true, CascadeFilters: false},
			group: config.FilterGroup{
				Enabled: true,
				Topics:  []config.TopicFilter{{Topic0: testHexSigUpper}},
			},
			address: "",
			topics:  []string{testHexSigLower},
			colType: colTypeLog,
			want:    true,
		},
		{
			name: "no match",
			cfg:  config.EventFilterConfig{Enabled: true, CascadeFilters: false},
			group: config.FilterGroup{
				Enabled:   true,
				Contracts: []config.ContractFilter{{Address: "0xABC", Types: []string{colTypeTransaction}}},
			},
			address: "0xDEF",
			topics:  nil,
			colType: colTypeTransaction,
			want:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewEventReplicationFilter(tt.cfg)
			require.NotNil(t, f)
			got := f.groupMatches(&tt.group, tt.address, tt.topics, tt.colType)
			require.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// contractAppliesToType
// ---------------------------------------------------------------------------

func TestContractAppliesToType(t *testing.T) {
	tests := []struct {
		name    string
		cf      config.ContractFilter
		colType string
		cascade bool
		want    bool
	}{
		{
			name:    "direct type match",
			cf:      config.ContractFilter{Types: []string{colTypeLog}},
			colType: colTypeLog,
			cascade: false,
			want:    true,
		},
		{
			name:    "cascade from transaction to log",
			cf:      config.ContractFilter{Types: []string{colTypeTransaction}},
			colType: colTypeLog,
			cascade: true,
			want:    true,
		},
		{
			name:    "cascade from transaction to accessListEntry",
			cf:      config.ContractFilter{Types: []string{colTypeTransaction}},
			colType: colTypeAccessListEntry,
			cascade: true,
			want:    true,
		},
		{
			name:    "no cascade when disabled",
			cf:      config.ContractFilter{Types: []string{colTypeTransaction}},
			colType: colTypeLog,
			cascade: false,
			want:    false,
		},
		{
			name:    "no match at all",
			cf:      config.ContractFilter{Types: []string{colTypeTransaction}},
			colType: colTypeAccessListEntry,
			cascade: false,
			want:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := contractAppliesToType(tt.cf, tt.colType, tt.cascade)
			require.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// topicFilterMatches
// ---------------------------------------------------------------------------

func TestTopicFilterMatches(t *testing.T) {
	tests := []struct {
		name   string
		tf     config.TopicFilter
		topics []string
		want   bool
	}{
		{
			name:   "empty topics returns false",
			tf:     config.TopicFilter{Topic0: testHexSigUpper},
			topics: []string{},
			want:   false,
		},
		{
			name:   "empty Topic0 returns false",
			tf:     config.TopicFilter{Topic0: ""},
			topics: []string{testHexSigUpper},
			want:   false,
		},
		{
			name:   "topic0 only match",
			tf:     config.TopicFilter{Topic0: testHexSigUpper},
			topics: []string{testHexSigLower},
			want:   true,
		},
		{
			name:   "topic0+1+2+3 full match",
			tf:     config.TopicFilter{Topic0: "0xA", Topic1: "0xB", Topic2: "0xC", Topic3: "0xD"},
			topics: []string{"0xa", "0xb", "0xc", "0xd"},
			want:   true,
		},
		{
			name:   "topic1 mismatch",
			tf:     config.TopicFilter{Topic0: "0xA", Topic1: "0xB"},
			topics: []string{"0xa", testHexWrong},
			want:   false,
		},
		{
			name:   "too few topics for topic2",
			tf:     config.TopicFilter{Topic0: "0xA", Topic2: "0xC"},
			topics: []string{"0xa", "0xb"},
			want:   false,
		},
		{
			name:   "too few topics for topic3",
			tf:     config.TopicFilter{Topic0: "0xA", Topic3: "0xD"},
			topics: []string{"0xa", "0xb", "0xc"},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := topicFilterMatches(tt.tf, tt.topics)
			require.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// inBlockRange
// ---------------------------------------------------------------------------

func TestInBlockRange(t *testing.T) {
	tests := []struct {
		name   string
		cfg    config.EventFilterConfig
		fields map[string]any
		want   bool
	}{
		{
			name: "nil range allows",
			cfg: config.EventFilterConfig{
				Enabled:    true,
				BlockRange: nil,
			},
			fields: map[string]any{gqlFieldBlockNumber: uint64(999)},
			want:   true,
		},
		{
			name: "below min rejects",
			cfg: config.EventFilterConfig{
				Enabled:    true,
				BlockRange: &config.BlockRangeFilter{MinBlock: 100, MaxBlock: 200},
			},
			fields: map[string]any{gqlFieldBlockNumber: uint64(50)},
			want:   false,
		},
		{
			name: "above max rejects",
			cfg: config.EventFilterConfig{
				Enabled:    true,
				BlockRange: &config.BlockRangeFilter{MinBlock: 100, MaxBlock: 200},
			},
			fields: map[string]any{gqlFieldBlockNumber: uint64(300)},
			want:   false,
		},
		{
			name: "max=0 means no upper limit",
			cfg: config.EventFilterConfig{
				Enabled:    true,
				BlockRange: &config.BlockRangeFilter{MinBlock: 100, MaxBlock: 0},
			},
			fields: map[string]any{gqlFieldBlockNumber: uint64(999999)},
			want:   true,
		},
		{
			name: "missing blockNumber field allows",
			cfg: config.EventFilterConfig{
				Enabled:    true,
				BlockRange: &config.BlockRangeFilter{MinBlock: 100, MaxBlock: 200},
			},
			fields: map[string]any{},
			want:   true,
		},
		{
			name: "in range allows",
			cfg: config.EventFilterConfig{
				Enabled:    true,
				BlockRange: &config.BlockRangeFilter{MinBlock: 100, MaxBlock: 200},
			},
			fields: map[string]any{gqlFieldBlockNumber: uint64(150)},
			want:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewEventReplicationFilter(tt.cfg)
			require.NotNil(t, f)
			got := f.inBlockRange(tt.fields)
			require.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// fieldString
// ---------------------------------------------------------------------------

func TestFieldString(t *testing.T) {
	tests := []struct {
		name   string
		fields map[string]any
		key    string
		wantS  string
		wantOK bool
	}{
		{
			name:   "present string",
			fields: map[string]any{testMapKeyAddr: "0xABC"},
			key:    testMapKeyAddr,
			wantS:  "0xABC",
			wantOK: true,
		},
		{
			name:   testNameMissingKey,
			fields: map[string]any{},
			key:    testMapKeyAddr,
			wantS:  "",
			wantOK: false,
		},
		{
			name:   "non-string value",
			fields: map[string]any{testMapKeyAddr: 123},
			key:    testMapKeyAddr,
			wantS:  "",
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, ok := fieldString(tt.fields, tt.key)
			require.Equal(t, tt.wantS, s)
			require.Equal(t, tt.wantOK, ok)
		})
	}
}

// ---------------------------------------------------------------------------
// fieldUint64
// ---------------------------------------------------------------------------

func TestFieldUint64(t *testing.T) {
	tests := []struct {
		name   string
		fields map[string]any
		key    string
		wantN  uint64
		wantOK bool
	}{
		{
			name:   "int64 value",
			fields: map[string]any{testNum: int64(42)},
			key:    testNum,
			wantN:  42,
			wantOK: true,
		},
		{
			name:   "uint64 value",
			fields: map[string]any{testNum: uint64(100)},
			key:    testNum,
			wantN:  100,
			wantOK: true,
		},
		{
			name:   "float64 value",
			fields: map[string]any{testNum: float64(55.0)},
			key:    testNum,
			wantN:  55,
			wantOK: true,
		},
		{
			name:   "int value",
			fields: map[string]any{testNum: int(77)},
			key:    testNum,
			wantN:  77,
			wantOK: true,
		},
		{
			name:   testNameMissingKey,
			fields: map[string]any{},
			key:    testNum,
			wantN:  0,
			wantOK: false,
		},
		{
			name:   "unsupported type string",
			fields: map[string]any{testNum: "not-a-number"},
			key:    testNum,
			wantN:  0,
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, ok := fieldUint64(tt.fields, tt.key)
			require.Equal(t, tt.wantN, n)
			require.Equal(t, tt.wantOK, ok)
		})
	}
}

// ---------------------------------------------------------------------------
// fieldStringSlice
// ---------------------------------------------------------------------------

func TestFieldStringSlice(t *testing.T) {
	tests := []struct {
		name   string
		fields map[string]any
		key    string
		want   []string
	}{
		{
			name:   "[]string value",
			fields: map[string]any{gqlFieldTopics: []string{"a", "b"}},
			key:    gqlFieldTopics,
			want:   []string{"a", "b"},
		},
		{
			name:   "[]any with strings",
			fields: map[string]any{gqlFieldTopics: []any{"x", "y", "z"}},
			key:    gqlFieldTopics,
			want:   []string{"x", "y", "z"},
		},
		{
			name:   testNameMissingKey,
			fields: map[string]any{},
			key:    gqlFieldTopics,
			want:   nil,
		},
		{
			name:   "unsupported type int",
			fields: map[string]any{gqlFieldTopics: 42},
			key:    gqlFieldTopics,
			want:   nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fieldStringSlice(tt.fields, tt.key)
			require.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// groupMatches - empty address
// ---------------------------------------------------------------------------

func TestGroupMatches_EmptyAddress(t *testing.T) {
	f := NewEventReplicationFilter(config.EventFilterConfig{
		Enabled:        true,
		CascadeFilters: false,
	})
	require.NotNil(t, f)

	group := config.FilterGroup{
		Enabled:   true,
		Contracts: []config.ContractFilter{{Address: "0xABC", Types: []string{colTypeTransaction}}},
	}

	// Empty address should not match contract filters
	got := f.groupMatches(&group, "", nil, colTypeTransaction)
	require.False(t, got)
}

func TestGroupMatches_TopicsNonLogType(t *testing.T) {
	f := NewEventReplicationFilter(config.EventFilterConfig{
		Enabled:        true,
		CascadeFilters: false,
	})
	require.NotNil(t, f)

	group := config.FilterGroup{
		Enabled: true,
		Topics:  []config.TopicFilter{{Topic0: testHexSigUpper}},
	}

	// Topics are only checked for "log" type
	got := f.groupMatches(&group, "", []string{testHexSigLower}, colTypeTransaction)
	require.False(t, got)
}

func TestGroupMatches_CascadeFromTxToLog(t *testing.T) {
	f := NewEventReplicationFilter(config.EventFilterConfig{
		Enabled:        true,
		CascadeFilters: true,
	})
	require.NotNil(t, f)

	group := config.FilterGroup{
		Enabled:   true,
		Contracts: []config.ContractFilter{{Address: "0xABC", Types: []string{colTypeTransaction}}},
	}

	// With cascade, a transaction filter should also match log types
	got := f.groupMatches(&group, "0xabc", nil, colTypeLog)
	require.True(t, got)
}

func TestGroupMatches_CascadeFromTxToAccessListEntry(t *testing.T) {
	f := NewEventReplicationFilter(config.EventFilterConfig{
		Enabled:        true,
		CascadeFilters: true,
	})
	require.NotNil(t, f)

	group := config.FilterGroup{
		Enabled:   true,
		Contracts: []config.ContractFilter{{Address: "0xABC", Types: []string{colTypeTransaction}}},
	}

	got := f.groupMatches(&group, "0xabc", nil, colTypeAccessListEntry)
	require.True(t, got)
}

// ---------------------------------------------------------------------------
// topicFilterMatches - additional edge cases
// ---------------------------------------------------------------------------

func TestTopicFilterMatches_Topic2Match(t *testing.T) {
	tf := config.TopicFilter{Topic0: "0xA", Topic2: "0xC"}
	topics := []string{"0xa", "0xb", "0xc"}
	require.True(t, topicFilterMatches(tf, topics))
}

func TestTopicFilterMatches_Topic3Match(t *testing.T) {
	tf := config.TopicFilter{Topic0: "0xA", Topic3: "0xD"}
	topics := []string{"0xa", "0xb", "0xc", "0xd"}
	require.True(t, topicFilterMatches(tf, topics))
}

func TestTopicFilterMatches_Topic2Mismatch(t *testing.T) {
	tf := config.TopicFilter{Topic0: "0xA", Topic2: testHexWrong}
	topics := []string{"0xa", "0xb", "0xc"}
	require.False(t, topicFilterMatches(tf, topics))
}

func TestTopicFilterMatches_Topic3Mismatch(t *testing.T) {
	tf := config.TopicFilter{Topic0: "0xA", Topic3: testHexWrong}
	topics := []string{"0xa", "0xb", "0xc", "0xd"}
	require.False(t, topicFilterMatches(tf, topics))
}

func TestTopicFilterMatches_NilTopics(t *testing.T) {
	tf := config.TopicFilter{Topic0: "0xA"}
	require.False(t, topicFilterMatches(tf, nil))
}

// ---------------------------------------------------------------------------
// matchesGroups - blocklist with no enabled groups
// ---------------------------------------------------------------------------

func TestMatchesGroups_BlocklistNoEnabledGroups(t *testing.T) {
	f := NewEventReplicationFilter(config.EventFilterConfig{
		Enabled: true,
		Mode:    filterModeBlocklist,
		Groups: []config.FilterGroup{
			{Enabled: false, Contracts: []config.ContractFilter{{Address: "0xABC", Types: []string{colTypeTransaction}}}},
		},
	})
	require.NotNil(t, f)

	// Blocklist with no matching (all groups disabled) => !false => true
	got := f.matchesGroups("0xABC", nil, colTypeTransaction)
	require.True(t, got)
}

// ---------------------------------------------------------------------------
// fieldStringSlice - []any with non-string items
// ---------------------------------------------------------------------------

func TestFieldStringSlice_MixedTypes(t *testing.T) {
	fields := map[string]any{
		gqlFieldTopics: []any{"str1", 42, "str2"},
	}
	got := fieldStringSlice(fields, gqlFieldTopics)
	require.Equal(t, []string{"str1", "str2"}, got)
}

// ---------------------------------------------------------------------------
// hasEnabledGroups
// ---------------------------------------------------------------------------

func TestHasEnabledGroups_Empty(t *testing.T) {
	f := NewEventReplicationFilter(config.EventFilterConfig{
		Enabled: true,
		Groups:  []config.FilterGroup{},
	})
	require.False(t, f.hasEnabledGroups())
}

func TestHasEnabledGroups_AllDisabled(t *testing.T) {
	f := NewEventReplicationFilter(config.EventFilterConfig{
		Enabled: true,
		Groups: []config.FilterGroup{
			{Enabled: false},
			{Enabled: false},
		},
	})
	require.False(t, f.hasEnabledGroups())
}

func TestHasEnabledGroups_OneEnabled(t *testing.T) {
	f := NewEventReplicationFilter(config.EventFilterConfig{
		Enabled: true,
		Groups: []config.FilterGroup{
			{Enabled: false},
			{Enabled: true},
		},
	})
	require.True(t, f.hasEnabledGroups())
}

// ---------------------------------------------------------------------------
// groupMatches - contract filter with wrong type (no cascade)
// ---------------------------------------------------------------------------

func TestGroupMatches_ContractFilterWrongType(t *testing.T) {
	f := NewEventReplicationFilter(config.EventFilterConfig{
		Enabled:        true,
		CascadeFilters: false,
	})
	require.NotNil(t, f)

	group := config.FilterGroup{
		Enabled: true,
		Contracts: []config.ContractFilter{
			{Address: "0xABC", Types: []string{colTypeLog}}, // Only matches "log", not "transaction"
		},
	}

	// Address matches but type doesn't
	got := f.groupMatches(&group, "0xabc", nil, colTypeTransaction)
	require.False(t, got)
}

// ---------------------------------------------------------------------------
// groupMatches - multiple contracts, first miss second hit
// ---------------------------------------------------------------------------

func TestGroupMatches_MultipleContracts(t *testing.T) {
	f := NewEventReplicationFilter(config.EventFilterConfig{
		Enabled:        true,
		CascadeFilters: false,
	})
	require.NotNil(t, f)

	group := config.FilterGroup{
		Enabled: true,
		Contracts: []config.ContractFilter{
			{Address: "0xDEF", Types: []string{colTypeTransaction}}, // Won't match
			{Address: "0xABC", Types: []string{colTypeTransaction}}, // Will match
		},
	}

	got := f.groupMatches(&group, "0xabc", nil, colTypeTransaction)
	require.True(t, got)
}

// ---------------------------------------------------------------------------
// topicFilterMatches - topic1 too few topics
// ---------------------------------------------------------------------------

func TestTopicFilterMatches_Topic1TooFewTopics(t *testing.T) {
	tf := config.TopicFilter{Topic0: "0xA", Topic1: "0xB"}
	topics := []string{"0xa"} // Only 1 topic, needs 2
	require.False(t, topicFilterMatches(tf, topics))
}

// ---------------------------------------------------------------------------
// groupMatches - log type with topics match (tests the topic path in groupMatches)
// ---------------------------------------------------------------------------

func TestGroupMatches_LogTypeTopicsMatch(t *testing.T) {
	f := NewEventReplicationFilter(config.EventFilterConfig{
		Enabled:        true,
		CascadeFilters: false,
	})
	require.NotNil(t, f)

	group := config.FilterGroup{
		Enabled: true,
		// No contracts, only topics
		Topics: []config.TopicFilter{
			{Topic0: "0xTransfer"},
		},
	}

	// Log type with matching topic0
	got := f.groupMatches(&group, "", []string{testHexTransfer}, colTypeLog)
	require.True(t, got)

	// Non-log type should not check topics
	got = f.groupMatches(&group, "", []string{testHexTransfer}, colTypeTransaction)
	require.False(t, got)
}

// ---------------------------------------------------------------------------
// groupMatches - log type with multiple topic filters, first miss second hit
// ---------------------------------------------------------------------------

func TestGroupMatches_LogMultipleTopicFilters(t *testing.T) {
	f := NewEventReplicationFilter(config.EventFilterConfig{
		Enabled:        true,
		CascadeFilters: false,
	})
	require.NotNil(t, f)

	group := config.FilterGroup{
		Enabled: true,
		Topics: []config.TopicFilter{
			{Topic0: "0xApproval"}, // Won't match
			{Topic0: "0xTransfer"}, // Will match
		},
	}

	got := f.groupMatches(&group, "", []string{testHexTransfer}, colTypeLog)
	require.True(t, got)
}

// ---------------------------------------------------------------------------
// AllowReplication - block with no number field (allows through)
// ---------------------------------------------------------------------------

func TestAllowReplication_BlockNoNumberField(t *testing.T) {
	f := NewEventReplicationFilter(config.EventFilterConfig{
		Enabled:    true,
		Mode:       filterModeAllowlist,
		BlockRange: &config.BlockRangeFilter{MinBlock: 100, MaxBlock: 200},
	})
	require.NotNil(t, f)

	// Block collection with no gqlFieldNumber field should be allowed (can't determine)
	got := f.AllowReplication(context.Background(), constants.CollectionBlock, "docID", map[string]any{})
	require.True(t, got)
}

// ---------------------------------------------------------------------------
// AllowReplication - transaction below block range
// ---------------------------------------------------------------------------

func TestAllowReplication_TransactionBelowBlockRange(t *testing.T) {
	f := NewEventReplicationFilter(config.EventFilterConfig{
		Enabled:    true,
		Mode:       filterModeAllowlist,
		BlockRange: &config.BlockRangeFilter{MinBlock: 100, MaxBlock: 200},
		Groups: []config.FilterGroup{
			{
				Enabled:   true,
				Contracts: []config.ContractFilter{{Address: "0xABC", Types: []string{colTypeTransaction}}},
			},
		},
	})
	require.NotNil(t, f)

	// Transaction with blockNumber below range
	got := f.AllowReplication(context.Background(), constants.CollectionTransaction, "docID",
		map[string]any{gqlFieldTo: "0xabc", gqlFieldBlockNumber: uint64(50)})
	require.False(t, got)

	// Transaction with blockNumber above range
	got = f.AllowReplication(context.Background(), constants.CollectionTransaction, "docID",
		map[string]any{gqlFieldTo: "0xabc", gqlFieldBlockNumber: uint64(300)})
	require.False(t, got)
}

// ---------------------------------------------------------------------------
// AllowReplication - log and accessListEntry block range checks
// ---------------------------------------------------------------------------

func TestAllowReplication_LogBlockRange(t *testing.T) {
	f := NewEventReplicationFilter(config.EventFilterConfig{
		Enabled:    true,
		Mode:       filterModeAllowlist,
		BlockRange: &config.BlockRangeFilter{MinBlock: 100, MaxBlock: 200},
		Groups: []config.FilterGroup{
			{
				Enabled:   true,
				Contracts: []config.ContractFilter{{Address: testHexLogUpper, Types: []string{colTypeLog}}},
			},
		},
	})

	// Log with blockNumber in range, matching address
	got := f.AllowReplication(context.Background(), constants.CollectionLog, "docID",
		map[string]any{gqlFieldAddress: testHexLogLower, gqlFieldBlockNumber: uint64(150)})
	require.True(t, got)

	// Log with blockNumber out of range
	got = f.AllowReplication(context.Background(), constants.CollectionLog, "docID",
		map[string]any{gqlFieldAddress: testHexLogLower, gqlFieldBlockNumber: uint64(50)})
	require.False(t, got)
}

func TestAllowReplication_AccessListEntryBlockRange(t *testing.T) {
	f := NewEventReplicationFilter(config.EventFilterConfig{
		Enabled:    true,
		Mode:       filterModeAllowlist,
		BlockRange: &config.BlockRangeFilter{MinBlock: 100, MaxBlock: 200},
		Groups: []config.FilterGroup{
			{
				Enabled:   true,
				Contracts: []config.ContractFilter{{Address: testHexAleUpper, Types: []string{colTypeAccessListEntry}}},
			},
		},
	})

	// AccessListEntry with blockNumber in range
	got := f.AllowReplication(context.Background(), constants.CollectionAccessListEntry, "docID",
		map[string]any{gqlFieldAddress: testHexAleLower, gqlFieldBlockNumber: uint64(150)})
	require.True(t, got)

	// AccessListEntry with blockNumber below range
	got = f.AllowReplication(context.Background(), constants.CollectionAccessListEntry, "docID",
		map[string]any{gqlFieldAddress: testHexAleLower, gqlFieldBlockNumber: uint64(50)})
	require.False(t, got)
}
