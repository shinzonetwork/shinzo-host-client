package graphql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddBlockNumberFilter(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		startingBlock uint64
		expectedQuery string
		expectError   bool
	}{
		{
			name:          "simple query without filter",
			query:         "Log { address topics data transactionHash }",
			startingBlock: 1000,
			expectedQuery: "Log (filter: { blockNumber: { _ge: 1000 } }){ address topics data transactionHash }",
			expectError:   false,
		},
		{
			name:          "query with existing filter",
			query:         "Log(filter: { address: { _eq: \"0x123\" } }) { address topics data }",
			startingBlock: 2000,
			expectedQuery: "Log(filter: { address: { _eq: \"0x123\" } }, blockNumber: { _ge: 2000 }) { address topics data }",
			expectError:   false,
		},
		{
			name:          "query with empty existing filter",
			query:         "Log(filter: { }) { address topics data }",
			startingBlock: 3000,
			expectedQuery: "Log(filter: { }, blockNumber: { _ge: 3000 }) { address topics data }",
			expectError:   false,
		},
		{
			name:          "transaction query without filter",
			query:         "Transaction { hash from to value }",
			startingBlock: 1500,
			expectedQuery: "Transaction (filter: { blockNumber: { _ge: 1500 } }){ hash from to value }",
			expectError:   false,
		},
		{
			name:          "block query without filter",
			query:         "Block { hash number timestamp }",
			startingBlock: 5000,
			expectedQuery: "Block (filter: { blockNumber: { _ge: 5000 } }){ hash number timestamp }",
			expectError:   false,
		},
		{
			name:          "query with whitespace",
			query:         "  Log  {  address  topics  }  ",
			startingBlock: 100,
			expectedQuery: "Log  (filter: { blockNumber: { _ge: 100 } }){  address  topics  }",
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
			result, err := AddBlockNumberFilter(tt.query, tt.startingBlock)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedQuery, result)
		})
	}
}

func TestAddBlockNumberFilter_ComplexFilters(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		startingBlock uint64
		expectedQuery string
	}{
		{
			name:          "multiple existing filters",
			query:         "Log(filter: { address: { _eq: \"0x123\" }, topics: { _in: [\"0xabc\"] } }) { address topics data }",
			startingBlock: 4000,
			expectedQuery: "Log(filter: { address: { _eq: \"0x123\" }, topics: { _in: [\"0xabc\"] } }, blockNumber: { _ge: 4000 }) { address topics data }",
		},
		{
			name:          "nested filter structure",
			query:         "Log(filter: { address: { _eq: \"0x123\" }, _and: [{ topics: { _in: [\"0xabc\"] } }] }) { address topics data }",
			startingBlock: 5000,
			expectedQuery: "Log(filter: { address: { _eq: \"0x123\" }, _and: [{ topics: { _in: [\"0xabc\"] } }] }, blockNumber: { _ge: 5000 }) { address topics data }",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := AddBlockNumberFilter(tt.query, tt.startingBlock)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedQuery, result)
		})
	}
}
