package graphql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/stretchr/testify/require"
)

func TestAddBlockNumberFilter(t *testing.T) {
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
			query:         constants.CollectionLog + " { address topics data transactionHash }",
			startingBlock: 1000,
			endingBlock:   9999999,
			expectedQuery: constants.CollectionLog + " (filter: { _and: [ { blockNumber: { _ge: 1000 } }, { blockNumber: { _le: 9999999 } } ] }, order: { blockNumber: DESC }){ address topics data transactionHash }",
			expectError:   false,
		},
		{
			name:          "query with existing filter",
			query:         constants.CollectionLog + "(filter: { address: { _eq: \"0x123\" } }) { address topics data }",
			startingBlock: 2000,
			endingBlock:   5000,
			expectedQuery: constants.CollectionLog + "(filter: { address: { _eq: \"0x123\" } }, _and: [ { blockNumber: { _ge: 2000 } }, { blockNumber: { _le: 5000 } } ], order: { blockNumber: DESC }) { address topics data }",
			expectError:   false,
		},
		{
			name:          "query with empty existing filter",
			query:         constants.CollectionLog + "(filter: { }) { address topics data }",
			startingBlock: 3000,
			endingBlock:   6000,
			expectedQuery: constants.CollectionLog + "(filter: { }, _and: [ { blockNumber: { _ge: 3000 } }, { blockNumber: { _le: 6000 } } ], order: { blockNumber: DESC }) { address topics data }",
			expectError:   false,
		},
		{
			name:          "transaction query without filter",
			query:         constants.CollectionTransaction + " { hash from to value }",
			startingBlock: 1500,
			endingBlock:   8000,
			expectedQuery: constants.CollectionTransaction + " (filter: { _and: [ { blockNumber: { _ge: 1500 } }, { blockNumber: { _le: 8000 } } ] }, order: { blockNumber: DESC }){ hash from to value }",
			expectError:   false,
		},
		{
			name:          "block query without filter",
			query:         constants.CollectionBlock + " { hash number timestamp }",
			startingBlock: 5000,
			endingBlock:   10000,
			expectedQuery: constants.CollectionBlock + " (filter: { _and: [ { number: { _ge: 5000 } }, { number: { _le: 10000 } } ] }, order: { number: DESC }){ hash number timestamp }",
			expectError:   false,
		},
		{
			name:          "access list entry query without filter",
			query:         constants.CollectionAccessListEntry + " { address storageKeys }",
			startingBlock: 1000,
			endingBlock:   2000,
			expectedQuery: constants.CollectionAccessListEntry + " (filter: { _and: [ { transaction: { blockNumber: { _ge: 1000 } } }, { transaction: { blockNumber: { _le: 2000 } } } ] }, order: { transaction: { blockNumber: DESC } }){ address storageKeys }",
			expectError:   false,
		},
		{
			name:          "query with whitespace",
			query:         constants.CollectionLog + "  {  address  topics  }  ",
			startingBlock: 100,
			endingBlock:   500,
			expectedQuery: constants.CollectionLog + "  (filter: { _and: [ { blockNumber: { _ge: 100 } }, { blockNumber: { _le: 500 } } ] }, order: { blockNumber: DESC }){  address  topics  }",
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
			query:       constants.CollectionLog,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := AddBlockNumberFilter(tt.query, tt.startingBlock, tt.endingBlock)

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
		endingBlock   uint64
		expectedQuery string
	}{
		{
			name:          "multiple existing filters",
			query:         constants.CollectionLog + "(filter: { address: { _eq: \"0x123\" }, topics: { _in: [\"0xabc\"] } }) { address topics data }",
			startingBlock: 4000,
			endingBlock:   7000,
			expectedQuery: constants.CollectionLog + "(filter: { address: { _eq: \"0x123\" }, topics: { _in: [\"0xabc\"] } }, _and: [ { blockNumber: { _ge: 4000 } }, { blockNumber: { _le: 7000 } } ], order: { blockNumber: DESC }) { address topics data }",
		},
		{
			name:          "nested filter structure",
			query:         constants.CollectionLog + "(filter: { address: { _eq: \"0x123\" }, _and: [{ topics: { _in: [\"0xabc\"] } }] }) { address topics data }",
			startingBlock: 5000,
			endingBlock:   9000,
			expectedQuery: constants.CollectionLog + "(filter: { address: { _eq: \"0x123\" }, _and: [{ topics: { _in: [\"0xabc\"] } }] }, _and: [ { blockNumber: { _ge: 5000 } }, { blockNumber: { _le: 9000 } } ], order: { blockNumber: DESC }) { address topics data }",
		},
		{
			name:          "access list entry with existing filter",
			query:         constants.CollectionAccessListEntry + "(filter: { address: { _eq: \"0x456\" } }) { address storageKeys }",
			startingBlock: 1000,
			endingBlock:   3000,
			expectedQuery: constants.CollectionAccessListEntry + "(filter: { address: { _eq: \"0x456\" } }, _and: [ { transaction: { blockNumber: { _ge: 1000 } } }, { transaction: { blockNumber: { _le: 3000 } } } ], order: { transaction: { blockNumber: DESC } }) { address storageKeys }",
		},
		{
			name:          "block query with existing filter",
			query:         constants.CollectionBlock + "(filter: { hash: { _eq: \"0x789\" } }) { hash number timestamp }",
			startingBlock: 2000,
			endingBlock:   4000,
			expectedQuery: constants.CollectionBlock + "(filter: { hash: { _eq: \"0x789\" } }, _and: [ { number: { _ge: 2000 } }, { number: { _le: 4000 } } ], order: { number: DESC }) { hash number timestamp }",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := AddBlockNumberFilter(tt.query, tt.startingBlock, tt.endingBlock)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedQuery, result)
		})
	}
}

func TestAddBlockNumberFilter_EdgeCases(t *testing.T) {
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
			query:         constants.CollectionLog + " { address topics }",
			startingBlock: 1000,
			endingBlock:   1000,
			expectedQuery: constants.CollectionLog + " (filter: { _and: [ { blockNumber: { _ge: 1000 } }, { blockNumber: { _le: 1000 } } ] }, order: { blockNumber: DESC }){ address topics }",
			expectError:   false,
		},
		{
			name:          "ending block less than starting block",
			query:         constants.CollectionLog + " { address topics }",
			startingBlock: 5000,
			endingBlock:   1000,
			expectedQuery: constants.CollectionLog + " (filter: { _and: [ { blockNumber: { _ge: 5000 } }, { blockNumber: { _le: 1000 } } ] }, order: { blockNumber: DESC }){ address topics }",
			expectError:   false,
		},
		{
			name:          "zero starting block",
			query:         constants.CollectionLog + " { address topics }",
			startingBlock: 0,
			endingBlock:   1000,
			expectedQuery: constants.CollectionLog + " (filter: { _and: [ { blockNumber: { _ge: 0 } }, { blockNumber: { _le: 1000 } } ] }, order: { blockNumber: DESC }){ address topics }",
			expectError:   false,
		},
		{
			name:          "large block numbers",
			query:         constants.CollectionLog + " { address topics }",
			startingBlock: 18446744073709551615, // max uint64
			endingBlock:   18446744073709551615,
			expectedQuery: constants.CollectionLog + " (filter: { _and: [ { blockNumber: { _ge: 18446744073709551615 } }, { blockNumber: { _le: 18446744073709551615 } } ] }, order: { blockNumber: DESC }){ address topics }",
			expectError:   false,
		},
		{
			name:          "transaction with complex existing filter",
			query:         constants.CollectionTransaction + "(filter: { _and: [{ from: { _eq: \"0x123\" } }, { to: { _eq: \"0x456\" } }] }) { hash from to }",
			startingBlock: 1000,
			endingBlock:   2000,
			expectedQuery: constants.CollectionTransaction + "(filter: { _and: [{ from: { _eq: \"0x123\" } }, { to: { _eq: \"0x456\" } }] }, _and: [ { blockNumber: { _ge: 1000 } }, { blockNumber: { _le: 2000 } } ], order: { blockNumber: DESC }) { hash from to }",
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := AddBlockNumberFilter(tt.query, tt.startingBlock, tt.endingBlock)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedQuery, result)
		})
	}
}
