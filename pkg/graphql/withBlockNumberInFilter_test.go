package graphql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithBlockNumberInFilter(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		blockNumbers  []uint64
		expectedQuery string
		expectError   bool
	}{
		{
			name:         "simple Log query without filter - single block",
			query:        "Log { address topics data transactionHash }",
			blockNumbers: []uint64{1000},
			expectedQuery: "Log (filter: { blockNumber: { _in: [1000] } }){ address topics data transactionHash }",
			expectError:  false,
		},
		{
			name:         "simple Log query without filter - multiple blocks",
			query:        "Log { address topics data transactionHash }",
			blockNumbers: []uint64{1000, 1001, 1004},
			expectedQuery: "Log (filter: { blockNumber: { _in: [1000, 1001, 1004] } }){ address topics data transactionHash }",
			expectError:  false,
		},
		{
			name:         "Log query with existing filter",
			query:        "Log(filter: { address: { _eq: \"0x123\" } }) { address topics data }",
			blockNumbers: []uint64{2000, 2001},
			expectedQuery: "Log(filter: { address: { _eq: \"0x123\" } }, blockNumber: { _in: [2000, 2001] }) { address topics data }",
			expectError:  false,
		},
		{
			name:         "Log query with empty existing filter",
			query:        "Log(filter: { }) { address topics data }",
			blockNumbers: []uint64{3000},
			expectedQuery: "Log(filter: { }, blockNumber: { _in: [3000] }) { address topics data }",
			expectError:  false,
		},
		{
			name:         "Transaction query without filter",
			query:        "Transaction { hash from to value }",
			blockNumbers: []uint64{1500, 1501, 1502},
			expectedQuery: "Transaction (filter: { blockNumber: { _in: [1500, 1501, 1502] } }){ hash from to value }",
			expectError:  false,
		},
		{
			name:         "Block query without filter",
			query:        "Block { hash number timestamp }",
			blockNumbers: []uint64{5000, 5001},
			expectedQuery: "Block (filter: { number: { _in: [5000, 5001] } }){ hash number timestamp }",
			expectError:  false,
		},
		{
			name:         "Block query with existing filter",
			query:        "Block(filter: { hash: { _eq: \"0x789\" } }) { hash number timestamp }",
			blockNumbers: []uint64{2000, 2001, 2002},
			expectedQuery: "Block(filter: { hash: { _eq: \"0x789\" } }, number: { _in: [2000, 2001, 2002] }) { hash number timestamp }",
			expectError:  false,
		},
		{
			name:         "AccessListEntry query without filter",
			query:        "AccessListEntry { address storageKeys }",
			blockNumbers: []uint64{1000, 1001},
			expectedQuery: "AccessListEntry (filter: { transaction: { blockNumber: { _in: [1000, 1001] } } }){ address storageKeys }",
			expectError:  false,
		},
		{
			name:         "AccessListEntry query with existing filter",
			query:        "AccessListEntry(filter: { address: { _eq: \"0x456\" } }) { address storageKeys }",
			blockNumbers: []uint64{1000, 1004},
			expectedQuery: "AccessListEntry(filter: { address: { _eq: \"0x456\" } }, transaction: { blockNumber: { _in: [1000, 1004] } }) { address storageKeys }",
			expectError:  false,
		},
		{
			name:         "query with whitespace",
			query:        "  Log  {  address  topics  }  ",
			blockNumbers: []uint64{100, 101},
			expectedQuery: "Log  (filter: { blockNumber: { _in: [100, 101] } }){  address  topics  }",
			expectError:  false,
		},
		{
			name:        "empty query",
			query:       "",
			blockNumbers: []uint64{1000},
			expectError: true,
		},
		{
			name:        "empty block numbers",
			query:       "Log { address }",
			blockNumbers: []uint64{},
			expectError: true,
		},
		{
			name:        "nil block numbers",
			query:       "Log { address }",
			blockNumbers: nil,
			expectError: true,
		},
		{
			name:        "invalid query format",
			query:       "InvalidQuery",
			blockNumbers: []uint64{1000},
			expectError: true,
		},
		{
			name:        "query without selection set",
			query:       "Log",
			blockNumbers: []uint64{1000},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := WithBlockNumberInFilter(tt.query, tt.blockNumbers)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedQuery, result)
		})
	}
}

func TestWithBlockNumberInFilter_ComplexFilters(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		blockNumbers []uint64
		expectedQuery string
	}{
		{
			name:         "Log with multiple existing filters",
			query:        "Log(filter: { address: { _eq: \"0x123\" }, topics: { _in: [\"0xabc\"] } }) { address topics data }",
			blockNumbers: []uint64{4000, 4001, 4002},
			expectedQuery: "Log(filter: { address: { _eq: \"0x123\" }, topics: { _in: [\"0xabc\"] } }, blockNumber: { _in: [4000, 4001, 4002] }) { address topics data }",
		},
		{
			name:         "Log with nested filter structure",
			query:        "Log(filter: { address: { _eq: \"0x123\" }, _and: [{ topics: { _in: [\"0xabc\"] } }] }) { address topics data }",
			blockNumbers: []uint64{5000, 5001},
			expectedQuery: "Log(filter: { address: { _eq: \"0x123\" }, _and: [{ topics: { _in: [\"0xabc\"] } }] }, blockNumber: { _in: [5000, 5001] }) { address topics data }",
		},
		{
			name:         "Transaction with complex existing filter",
			query:        "Transaction(filter: { _and: [{ from: { _eq: \"0x123\" } }, { to: { _eq: \"0x456\" } }] }) { hash from to }",
			blockNumbers: []uint64{1000, 1001, 1002},
			expectedQuery: "Transaction(filter: { _and: [{ from: { _eq: \"0x123\" } }, { to: { _eq: \"0x456\" } }] }, blockNumber: { _in: [1000, 1001, 1002] }) { hash from to }",
		},
		{
			name:         "Block with existing filter",
			query:        "Block(filter: { hash: { _eq: \"0x789\" } }) { hash number timestamp }",
			blockNumbers: []uint64{2000, 2001},
			expectedQuery: "Block(filter: { hash: { _eq: \"0x789\" } }, number: { _in: [2000, 2001] }) { hash number timestamp }",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := WithBlockNumberInFilter(tt.query, tt.blockNumbers)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedQuery, result)
		})
	}
}

func TestWithBlockNumberInFilter_EdgeCases(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		blockNumbers []uint64
		expectedQuery string
		expectError   bool
	}{
		{
			name:         "single block number",
			query:        "Log { address topics }",
			blockNumbers: []uint64{1000},
			expectedQuery: "Log (filter: { blockNumber: { _in: [1000] } }){ address topics }",
			expectError:  false,
		},
		{
			name:         "many block numbers",
			query:        "Log { address topics }",
			blockNumbers: []uint64{1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009},
			expectedQuery: "Log (filter: { blockNumber: { _in: [1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009] } }){ address topics }",
			expectError:  false,
		},
		{
			name:         "zero block number",
			query:        "Log { address topics }",
			blockNumbers: []uint64{0},
			expectedQuery: "Log (filter: { blockNumber: { _in: [0] } }){ address topics }",
			expectError:  false,
		},
		{
			name:         "large block numbers",
			query:        "Log { address topics }",
			blockNumbers: []uint64{18446744073709551615}, // max uint64
			expectedQuery: "Log (filter: { blockNumber: { _in: [18446744073709551615] } }){ address topics }",
			expectError:  false,
		},
		{
			name:         "unsorted block numbers",
			query:        "Log { address topics }",
			blockNumbers: []uint64{1005, 1001, 1003, 1000, 1002},
			expectedQuery: "Log (filter: { blockNumber: { _in: [1005, 1001, 1003, 1000, 1002] } }){ address topics }",
			expectError:  false,
		},
		{
			name:         "duplicate block numbers",
			query:        "Log { address topics }",
			blockNumbers: []uint64{1000, 1001, 1000, 1002},
			expectedQuery: "Log (filter: { blockNumber: { _in: [1000, 1001, 1000, 1002] } }){ address topics }",
			expectError:  false,
		},
		{
			name:         "Block query with multiple block numbers",
			query:        "Block { number hash }",
			blockNumbers: []uint64{100, 200, 300},
			expectedQuery: "Block (filter: { number: { _in: [100, 200, 300] } }){ number hash }",
			expectError:  false,
		},
		{
			name:         "AccessListEntry with multiple block numbers",
			query:        "AccessListEntry { address }",
			blockNumbers: []uint64{1000, 1001, 1004},
			expectedQuery: "AccessListEntry (filter: { transaction: { blockNumber: { _in: [1000, 1001, 1004] } } }){ address }",
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := WithBlockNumberInFilter(tt.query, tt.blockNumbers)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedQuery, result)
		})
	}
}

func TestWithBlockNumberInFilter_AllCollectionTypes(t *testing.T) {
	blockNumbers := []uint64{1000, 1001, 1004}

	tests := []struct {
		name         string
		query        string
		expectedQuery string
	}{
		{
			name:         "Log collection",
			query:        "Log { address topics }",
			expectedQuery: "Log (filter: { blockNumber: { _in: [1000, 1001, 1004] } }){ address topics }",
		},
		{
			name:         "Transaction collection",
			query:        "Transaction { hash from to }",
			expectedQuery: "Transaction (filter: { blockNumber: { _in: [1000, 1001, 1004] } }){ hash from to }",
		},
		{
			name:         "Block collection",
			query:        "Block { number hash }",
			expectedQuery: "Block (filter: { number: { _in: [1000, 1001, 1004] } }){ number hash }",
		},
		{
			name:         "AccessListEntry collection",
			query:        "AccessListEntry { address storageKeys }",
			expectedQuery: "AccessListEntry (filter: { transaction: { blockNumber: { _in: [1000, 1001, 1004] } } }){ address storageKeys }",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := WithBlockNumberInFilter(tt.query, blockNumbers)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedQuery, result)
		})
	}
}

