package host

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewQueryService(t *testing.T) {
	qs := NewQueryService()

	require.NotNil(t, qs)
	require.NotNil(t, qs.fieldCache)

	tests := []struct {
		name           string
		collectionType string
		expectPresent  bool
		mustContain    []string
	}{
		{
			name:           "Block fields populated",
			collectionType: "Block",
			expectPresent:  true,
			mustContain:    []string{"hash", "number", "timestamp", "parentHash"},
		},
		{
			name:           "Transaction fields populated",
			collectionType: "Transaction",
			expectPresent:  true,
			mustContain:    []string{"hash", "from", "to", "value", "blockNumber"},
		},
		{
			name:           "Log fields populated",
			collectionType: "Log",
			expectPresent:  true,
			mustContain:    []string{"address", "data", "blockNumber", "transactionHash"},
		},
		{
			name:           "AccessListEntry fields populated",
			collectionType: "AccessListEntry",
			expectPresent:  true,
			mustContain:    []string{"address", "storageKeys"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields, exists := qs.fieldCache[tt.collectionType]
			require.Equal(t, tt.expectPresent, exists)
			for _, expected := range tt.mustContain {
				require.Contains(t, fields, expected, "expected field %q in %s", expected, tt.collectionType)
			}
		})
	}
}

func TestGetFieldsForCollection(t *testing.T) {
	qs := NewQueryService()

	tests := []struct {
		name           string
		collectionType CollectionType
		expectSystem   bool
		mustContain    []string
		expectFallback bool
	}{
		{
			name:           "Block includes system fields",
			collectionType: CollectionBlock,
			expectSystem:   true,
			mustContain:    []string{"_docID", "_version", "hash", "number"},
		},
		{
			name:           "Transaction includes system fields",
			collectionType: CollectionTransaction,
			expectSystem:   true,
			mustContain:    []string{"_docID", "_version", "hash", "from", "to"},
		},
		{
			name:           "Log includes system fields",
			collectionType: CollectionLog,
			expectSystem:   true,
			mustContain:    []string{"_docID", "_version", "address"},
		},
		{
			name:           "AccessListEntry includes system fields",
			collectionType: CollectionAccessListEntry,
			expectSystem:   true,
			mustContain:    []string{"_docID", "_version", "address"},
		},
		{
			name:           "unknown type returns fallback with system fields only",
			collectionType: CollectionType("UnknownCollection"),
			expectFallback: true,
			mustContain:    []string{"_docID", "_version"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := qs.GetFieldsForCollection(tt.collectionType)
			require.NotEmpty(t, fields)
			for _, expected := range tt.mustContain {
				require.Contains(t, fields, expected)
			}
			if tt.expectFallback {
				require.Len(t, fields, 2) // only _docID and _version
			}
			if tt.expectSystem {
				require.Equal(t, "_docID", fields[0])
				require.Equal(t, "_version", fields[1])
				require.Greater(t, len(fields), 2)
			}
		})
	}
}

func TestBuildDynamicQuery(t *testing.T) {
	qs := NewQueryService()

	tests := []struct {
		name           string
		collectionType CollectionType
		filters        map[string]interface{}
		mustContain    []string
		mustNotContain []string
	}{
		{
			name:           "no filters produces simple query",
			collectionType: CollectionBlock,
			filters:        nil,
			mustContain:    []string{"Ethereum__Mainnet__Block {", "_docID", "_version", "hash", "number"},
			mustNotContain: []string{"filter:"},
		},
		{
			name:           "empty filter map produces simple query",
			collectionType: CollectionTransaction,
			filters:        map[string]interface{}{},
			mustContain:    []string{"Ethereum__Mainnet__Transaction {", "_docID"},
			mustNotContain: []string{"filter:"},
		},
		{
			name:           "with string filter",
			collectionType: CollectionTransaction,
			filters:        map[string]interface{}{"from": "0xabc"},
			mustContain:    []string{"Ethereum__Mainnet__Transaction(filter:", "from: {_eq: \"0xabc\"}"},
		},
		{
			name:           "with bool filter",
			collectionType: CollectionTransaction,
			filters:        map[string]interface{}{"status": true},
			mustContain:    []string{"filter:", "status: {_eq: true}"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := qs.BuildDynamicQuery(tt.collectionType, tt.filters)
			require.NotEmpty(t, query)
			for _, s := range tt.mustContain {
				require.Contains(t, query, s)
			}
			for _, s := range tt.mustNotContain {
				require.NotContains(t, query, s)
			}
		})
	}
}

func TestBuildNestedQuery(t *testing.T) {
	qs := NewQueryService()

	tests := []struct {
		name              string
		primary           CollectionType
		nested            []CollectionType
		filters           map[string]interface{}
		mustContain       []string
	}{
		{
			name:    "block with nested transaction",
			primary: CollectionBlock,
			nested:  []CollectionType{CollectionTransaction},
			filters: nil,
			mustContain: []string{
				"Ethereum__Mainnet__Block {",
				"Ethereum__Mainnet__Transaction {",
			},
		},
		{
			name:    "transaction with nested log and access list",
			primary: CollectionTransaction,
			nested:  []CollectionType{CollectionLog, CollectionAccessListEntry},
			filters: nil,
			mustContain: []string{
				"Ethereum__Mainnet__Transaction {",
				"Ethereum__Mainnet__Log {",
				"Ethereum__Mainnet__AccessListEntry {",
			},
		},
		{
			name:    "nested query with filters",
			primary: CollectionBlock,
			nested:  []CollectionType{CollectionTransaction},
			filters: map[string]interface{}{"number": int64(100)},
			mustContain: []string{
				"Ethereum__Mainnet__Block(filter:",
				"Ethereum__Mainnet__Transaction",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := qs.BuildNestedQuery(tt.primary, tt.nested, tt.filters)
			require.NotEmpty(t, query)
			for _, s := range tt.mustContain {
				require.Contains(t, query, s)
			}
		})
	}
}

func TestGetCollectionName(t *testing.T) {
	qs := NewQueryService()

	tests := []struct {
		name           string
		collectionType CollectionType
		expected       string
	}{
		{
			name:           "Block",
			collectionType: CollectionBlock,
			expected:       "Ethereum__Mainnet__Block",
		},
		{
			name:           "Transaction",
			collectionType: CollectionTransaction,
			expected:       "Ethereum__Mainnet__Transaction",
		},
		{
			name:           "Log",
			collectionType: CollectionLog,
			expected:       "Ethereum__Mainnet__Log",
		},
		{
			name:           "AccessListEntry",
			collectionType: CollectionAccessListEntry,
			expected:       "Ethereum__Mainnet__AccessListEntry",
		},
		{
			name:           "unknown type returns raw string",
			collectionType: CollectionType("CustomCollection"),
			expected:       "CustomCollection",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := qs.getCollectionName(tt.collectionType)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildFilterString(t *testing.T) {
	qs := NewQueryService()

	tests := []struct {
		name        string
		filters     map[string]interface{}
		expectEmpty bool
		mustContain []string
	}{
		{
			name:        "empty map returns empty string",
			filters:     map[string]interface{}{},
			expectEmpty: true,
		},
		{
			name:        "nil map returns empty string",
			filters:     nil,
			expectEmpty: true,
		},
		{
			name:    "string value",
			filters: map[string]interface{}{"from": "0xabc"},
			mustContain: []string{
				"{ ",
				"from: {_eq: \"0xabc\"}",
				" }",
			},
		},
		{
			name:    "int64 value",
			filters: map[string]interface{}{"blockNumber": int64(42)},
			mustContain: []string{
				"blockNumber: {_eq: 42}",
			},
		},
		{
			name:    "float64 value",
			filters: map[string]interface{}{"gasPrice": float64(1.5)},
			mustContain: []string{
				"gasPrice: {_eq: 1.500000}",
			},
		},
		{
			name:    "bool value",
			filters: map[string]interface{}{"status": true},
			mustContain: []string{
				"status: {_eq: true}",
			},
		},
		{
			name:    "bool false value",
			filters: map[string]interface{}{"removed": false},
			mustContain: []string{
				"removed: {_eq: false}",
			},
		},
		{
			name:    "unknown type falls back to string representation",
			filters: map[string]interface{}{"custom": []int{1, 2, 3}},
			mustContain: []string{
				"custom: {_eq: \"[1 2 3]\"}",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := qs.buildFilterString(tt.filters)
			if tt.expectEmpty {
				require.Empty(t, result)
			} else {
				require.NotEmpty(t, result)
				for _, s := range tt.mustContain {
					require.Contains(t, result, s)
				}
			}
		})
	}
}

func TestGetCollectionHierarchy(t *testing.T) {
	qs := NewQueryService()

	hierarchy := qs.GetCollectionHierarchy()
	require.NotNil(t, hierarchy)

	tests := []struct {
		name             string
		collectionType   CollectionType
		expectPresent    bool
		expectedChildren []CollectionType
	}{
		{
			name:             "Block has Transaction as child",
			collectionType:   CollectionBlock,
			expectPresent:    true,
			expectedChildren: []CollectionType{CollectionTransaction},
		},
		{
			name:             "Transaction has Log and AccessListEntry as children",
			collectionType:   CollectionTransaction,
			expectPresent:    true,
			expectedChildren: []CollectionType{CollectionLog, CollectionAccessListEntry},
		},
		{
			name:             "Log has no children",
			collectionType:   CollectionLog,
			expectPresent:    true,
			expectedChildren: []CollectionType{},
		},
		{
			name:             "AccessListEntry has no children",
			collectionType:   CollectionAccessListEntry,
			expectPresent:    true,
			expectedChildren: []CollectionType{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			children, exists := hierarchy[tt.collectionType]
			require.Equal(t, tt.expectPresent, exists)
			require.Equal(t, tt.expectedChildren, children)
		})
	}

	// Verify all four keys exist
	require.Len(t, hierarchy, 4)
}

func TestBuildHierarchicalQuery(t *testing.T) {
	qs := NewQueryService()

	tests := []struct {
		name           string
		root           CollectionType
		filters        map[string]interface{}
		mustContain    []string
		mustNotContain []string
	}{
		{
			name:    "block with children and no filters",
			root:    CollectionBlock,
			filters: nil,
			mustContain: []string{
				"Ethereum__Mainnet__Block {",
				"Ethereum__Mainnet__Transaction {",
				"_docID",
				"_version",
			},
			mustNotContain: []string{"filter:"},
		},
		{
			name:    "block with children and filters",
			root:    CollectionBlock,
			filters: map[string]interface{}{"hash": "0xabc"},
			mustContain: []string{
				"Ethereum__Mainnet__Block(filter:",
				"hash: {_eq: \"0xabc\"}",
				"Ethereum__Mainnet__Transaction {",
			},
		},
		{
			name:    "transaction with children (Log and AccessListEntry)",
			root:    CollectionTransaction,
			filters: nil,
			mustContain: []string{
				"Ethereum__Mainnet__Transaction {",
				"Ethereum__Mainnet__Log {",
				"Ethereum__Mainnet__AccessListEntry {",
			},
			mustNotContain: []string{"filter:"},
		},
		{
			name:    "log with no children and no filters",
			root:    CollectionLog,
			filters: nil,
			mustContain: []string{
				"Ethereum__Mainnet__Log {",
				"_docID",
				"_version",
				"address",
			},
			mustNotContain: []string{
				"Ethereum__Mainnet__Transaction",
				"Ethereum__Mainnet__Block",
				"filter:",
			},
		},
		{
			name:    "access list entry with no children and filters",
			root:    CollectionAccessListEntry,
			filters: map[string]interface{}{"address": "0x123"},
			mustContain: []string{
				"Ethereum__Mainnet__AccessListEntry(filter:",
				"address: {_eq: \"0x123\"}",
			},
			mustNotContain: []string{
				"Ethereum__Mainnet__Transaction",
				"Ethereum__Mainnet__Log",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := qs.BuildHierarchicalQuery(tt.root, tt.filters)
			require.NotEmpty(t, query)
			for _, s := range tt.mustContain {
				require.Contains(t, query, s)
			}
			for _, s := range tt.mustNotContain {
				require.NotContains(t, query, s)
			}

			// Verify query is well-formed: braces are balanced
			openCount := strings.Count(query, "{")
			closeCount := strings.Count(query, "}")
			require.Equal(t, openCount, closeCount, "braces should be balanced in query: %s", query)
		})
	}
}

// ---------------------------------------------------------------------------
// extractFields - pointer types and edge cases
// ---------------------------------------------------------------------------

func TestExtractFields_PointerType(t *testing.T) {
	type SimpleStruct struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	qs := NewQueryService()

	// Test with pointer to struct
	fields := qs.extractFields(&SimpleStruct{})
	require.Contains(t, fields, "name")
	require.Contains(t, fields, "age")
}

func TestExtractFields_NoJsonTags(t *testing.T) {
	type NoTags struct {
		Field1 string
		Field2 int
	}

	qs := NewQueryService()
	fields := qs.extractFields(NoTags{})
	require.Empty(t, fields)
}

func TestExtractFields_JsonDashTag(t *testing.T) {
	type WithDash struct {
		Field1 string `json:"field1"`
		Field2 string `json:"-"`
	}

	qs := NewQueryService()
	fields := qs.extractFields(WithDash{})
	require.Contains(t, fields, "field1")
	require.NotContains(t, fields, "-")
}

func TestExtractFields_WithNestedStruct(t *testing.T) {
	type Inner struct {
		InnerField string `json:"inner_field"`
	}
	type Outer struct {
		Nested Inner `json:"nested"`
	}

	qs := NewQueryService()
	fields := qs.extractFields(Outer{})
	// Should contain both the nested fields and the parent tag
	require.Contains(t, fields, "inner_field")
	require.Contains(t, fields, "nested")
}

func TestExtractFields_WithSliceOfStructs(t *testing.T) {
	type Item struct {
		ItemName string `json:"item_name"`
	}
	type Container struct {
		Items []Item `json:"items"`
	}

	qs := NewQueryService()
	fields := qs.extractFields(Container{})
	require.Contains(t, fields, "item_name")
	require.Contains(t, fields, "items")
}

func TestExtractFields_WithSliceOfPrimitives(t *testing.T) {
	type WithStringSlice struct {
		Tags []string `json:"tags"`
	}

	qs := NewQueryService()
	fields := qs.extractFields(WithStringSlice{})
	require.Contains(t, fields, "tags")
}

// ---------------------------------------------------------------------------
// extractNestedFields - pointer types and edge cases
// ---------------------------------------------------------------------------

func TestExtractNestedFields_PointerType(t *testing.T) {
	type Inner struct {
		Field1 string `json:"field1"`
		Field2 int    `json:"field2"`
	}

	qs := NewQueryService()
	fields := qs.extractNestedFields(reflect.TypeOf(&Inner{}))
	require.Contains(t, fields, "field1")
	require.Contains(t, fields, "field2")
}

func TestExtractNestedFields_NoJsonTags(t *testing.T) {
	type NoTags struct {
		Field1 string
	}

	qs := NewQueryService()
	fields := qs.extractNestedFields(reflect.TypeOf(NoTags{}))
	require.Empty(t, fields)
}

func TestExtractNestedFields_WithDashTag(t *testing.T) {
	type WithDash struct {
		Visible string `json:"visible"`
		Hidden  string `json:"-"`
	}

	qs := NewQueryService()
	fields := qs.extractNestedFields(reflect.TypeOf(WithDash{}))
	require.Contains(t, fields, "visible")
	require.NotContains(t, fields, "-")
}

// ---------------------------------------------------------------------------
// buildFilterString - uint64 value
// ---------------------------------------------------------------------------

func TestBuildFilterString_Uint64Value(t *testing.T) {
	qs := NewQueryService()
	result := qs.buildFilterString(map[string]interface{}{"blockNum": uint64(99)})
	require.Contains(t, result, "blockNum: {_eq: 99}")
}

func TestBuildFilterString_IntValue(t *testing.T) {
	qs := NewQueryService()
	result := qs.buildFilterString(map[string]interface{}{"count": 42})
	require.Contains(t, result, "count: {_eq: 42}")
}
