package graphql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithBlockNumberFilter(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		startingBlock uint64
		endingBlock   uint64
		expectedQuery string
		expectError   bool
	}{
		{
			name:          "simple query without filter",
			query:         "Log { address topics data transactionHash }",
			startingBlock: 1000,
			endingBlock:   9999999,
			expectedQuery: "Log (filter: { _and: [ { blockNumber: { _ge: 1000 } }, { blockNumber: { _le: 9999999 } } ] }){ address topics data transactionHash }",
			expectError:   false,
		},
		{
			name:          "query with existing filter",
			query:         "Log(filter: { address: { _eq: \"0x123\" } }) { address topics data }",
			startingBlock: 2000,
			endingBlock:   5000,
			expectedQuery: "Log(filter: { address: { _eq: \"0x123\" } }, _and: [ { blockNumber: { _ge: 2000 } }, { blockNumber: { _le: 5000 } } ]) { address topics data }",
			expectError:   false,
		},
		{
			name:          "query with empty existing filter",
			query:         "Log(filter: { }) { address topics data }",
			startingBlock: 3000,
			endingBlock:   6000,
			expectedQuery: "Log(filter: { }, _and: [ { blockNumber: { _ge: 3000 } }, { blockNumber: { _le: 6000 } } ]) { address topics data }",
			expectError:   false,
		},
		{
			name:          "transaction query without filter",
			query:         "Transaction { hash from to value }",
			startingBlock: 1500,
			endingBlock:   8000,
			expectedQuery: "Transaction (filter: { _and: [ { blockNumber: { _ge: 1500 } }, { blockNumber: { _le: 8000 } } ] }){ hash from to value }",
			expectError:   false,
		},
		{
			name:          "block query without filter",
			query:         "Block { hash number timestamp }",
			startingBlock: 5000,
			endingBlock:   10000,
			expectedQuery: "Block (filter: { _and: [ { number: { _ge: 5000 } }, { number: { _le: 10000 } } ] }){ hash number timestamp }",
			expectError:   false,
		},
		{
			name:          "access list entry query without filter",
			query:         "AccessListEntry { address storageKeys }",
			startingBlock: 1000,
			endingBlock:   2000,
			expectedQuery: "AccessListEntry (filter: { _and: [ { transaction: { blockNumber: { _ge: 1000 } } }, { transaction: { blockNumber: { _le: 2000 } } } ] }){ address storageKeys }",
			expectError:   false,
		},
		{
			name:          "query with whitespace",
			query:         "  Log  {  address  topics  }  ",
			startingBlock: 100,
			endingBlock:   500,
			expectedQuery: "Log  (filter: { _and: [ { blockNumber: { _ge: 100 } }, { blockNumber: { _le: 500 } } ] }){  address  topics  }",
			expectError:   false,
		},
		{
			name:        "empty query",
			query:       "",
			expectError: true,
		},
		{
			name:        "invalid query format",
			query:       "InvalidQuery",
			expectError: true,
		},
		{
			name:        "query without selection set",
			query:       "Log",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := WithBlockNumberFilter(tt.query, tt.startingBlock, tt.endingBlock)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedQuery, result)
		})
	}
}

func TestWithBlockNumberFilter_ComplexFilters(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		startingBlock uint64
		endingBlock   uint64
		expectedQuery string
	}{
		{
			name:          "multiple existing filters",
			query:         "Log(filter: { address: { _eq: \"0x123\" }, topics: { _in: [\"0xabc\"] } }) { address topics data }",
			startingBlock: 4000,
			endingBlock:   7000,
			expectedQuery: "Log(filter: { address: { _eq: \"0x123\" }, topics: { _in: [\"0xabc\"] } }, _and: [ { blockNumber: { _ge: 4000 } }, { blockNumber: { _le: 7000 } } ]) { address topics data }",
		},
		{
			name:          "nested filter structure",
			query:         "Log(filter: { address: { _eq: \"0x123\" }, _and: [{ topics: { _in: [\"0xabc\"] } }] }) { address topics data }",
			startingBlock: 5000,
			endingBlock:   9000,
			expectedQuery: "Log(filter: { address: { _eq: \"0x123\" }, _and: [{ topics: { _in: [\"0xabc\"] } }] }, _and: [ { blockNumber: { _ge: 5000 } }, { blockNumber: { _le: 9000 } } ]) { address topics data }",
		},
		{
			name:          "access list entry with existing filter",
			query:         "AccessListEntry(filter: { address: { _eq: \"0x456\" } }) { address storageKeys }",
			startingBlock: 1000,
			endingBlock:   3000,
			expectedQuery: "AccessListEntry(filter: { address: { _eq: \"0x456\" } }, _and: [ { transaction: { blockNumber: { _ge: 1000 } } }, { transaction: { blockNumber: { _le: 3000 } } } ]) { address storageKeys }",
		},
		{
			name:          "block query with existing filter",
			query:         "Block(filter: { hash: { _eq: \"0x789\" } }) { hash number timestamp }",
			startingBlock: 2000,
			endingBlock:   4000,
			expectedQuery: "Block(filter: { hash: { _eq: \"0x789\" } }, _and: [ { number: { _ge: 2000 } }, { number: { _le: 4000 } } ]) { hash number timestamp }",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := WithBlockNumberFilter(tt.query, tt.startingBlock, tt.endingBlock)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedQuery, result)
		})
	}
}

func TestWithBlockNumberFilter_EdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		startingBlock uint64
		endingBlock   uint64
		expectedQuery string
		expectError   bool
	}{
		{
			name:          "same starting and ending block",
			query:         "Log { address topics }",
			startingBlock: 1000,
			endingBlock:   1000,
			expectedQuery: "Log (filter: { _and: [ { blockNumber: { _ge: 1000 } }, { blockNumber: { _le: 1000 } } ] }){ address topics }",
			expectError:   false,
		},
		{
			name:          "ending block less than starting block",
			query:         "Log { address topics }",
			startingBlock: 5000,
			endingBlock:   1000,
			expectedQuery: "Log (filter: { _and: [ { blockNumber: { _ge: 5000 } }, { blockNumber: { _le: 1000 } } ] }){ address topics }",
			expectError:   false,
		},
		{
			name:          "zero starting block",
			query:         "Log { address topics }",
			startingBlock: 0,
			endingBlock:   1000,
			expectedQuery: "Log (filter: { _and: [ { blockNumber: { _ge: 0 } }, { blockNumber: { _le: 1000 } } ] }){ address topics }",
			expectError:   false,
		},
		{
			name:          "large block numbers",
			query:         "Log { address topics }",
			startingBlock: 18446744073709551615, // max uint64
			endingBlock:   18446744073709551615,
			expectedQuery: "Log (filter: { _and: [ { blockNumber: { _ge: 18446744073709551615 } }, { blockNumber: { _le: 18446744073709551615 } } ] }){ address topics }",
			expectError:   false,
		},
		{
			name:          "transaction with complex existing filter",
			query:         "Transaction(filter: { _and: [{ from: { _eq: \"0x123\" } }, { to: { _eq: \"0x456\" } }] }) { hash from to }",
			startingBlock: 1000,
			endingBlock:   2000,
			expectedQuery: "Transaction(filter: { _and: [{ from: { _eq: \"0x123\" } }, { to: { _eq: \"0x456\" } }] }, _and: [ { blockNumber: { _ge: 1000 } }, { blockNumber: { _le: 2000 } } ]) { hash from to }",
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := WithBlockNumberFilter(tt.query, tt.startingBlock, tt.endingBlock)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedQuery, result)
		})
	}
}
