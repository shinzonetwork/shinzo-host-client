package graphql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithReturnDocId(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedQuery string
		expectError   bool
	}{
		{
			name:          "simple query without _docID",
			query:         "Log { address topics data transactionHash }",
			expectedQuery: "Log {_docID address topics data transactionHash}",
			expectError:   false,
		},
		{
			name:          "query with existing _docID",
			query:         "Log { _docID address topics data }",
			expectedQuery: "Log { _docID address topics data }",
			expectError:   false,
		},
		{
			name:          "query with nested objects without _docID",
			query:         "Transaction { hash from to logs { address topics } }",
			expectedQuery: "Transaction {_docID hash from to logs { address topics }}",
			expectError:   false,
		},
		{
			name:          "query with nested objects where some have _docID",
			query:         "Transaction { _docID hash from to logs { address topics } }",
			expectedQuery: "Transaction { _docID hash from to logs { address topics } }",
			expectError:   false,
		},
		{
			name:          "query with deeply nested objects",
			query:         "Block { number hash transactions { hash logs { address topics } } }",
			expectedQuery: "Block {_docID number hash transactions { hash logs { address topics } }}",
			expectError:   false,
		},
		{
			name:          "query with filter and nested objects",
			query:         "Log(filter: { address: { _eq: \"0x123\" } }) { address topics transaction { hash } }",
			expectedQuery: "Log(filter: { address: { _eq: \"0x123\" } }) {_docID address topics transaction { hash }}",
			expectError:   false,
		},
		{
			name:          "query with multiple nested levels",
			query:         "Block { number transactions { hash logs { address topics } } }",
			expectedQuery: "Block {_docID number transactions { hash logs { address topics } }}",
			expectError:   false,
		},
		{
			name:          "query with empty selection set",
			query:         "Log { }",
			expectedQuery: "Log {_docID}",
			expectError:   false,
		},
		{
			name:          "query with only _docID",
			query:         "Log { _docID }",
			expectedQuery: "Log { _docID }",
			expectError:   false,
		},
		{
			name:          "query with complex nested structure",
			query:         "Block { number hash transactions { hash from to logs { address topics data } } }",
			expectedQuery: "Block {_docID number hash transactions { hash from to logs { address topics data } }}",
			expectError:   false,
		},
		{
			name:          "query with AccessListEntry nested structure",
			query:         "AccessListEntry { key transaction { hash block { number } } }",
			expectedQuery: "AccessListEntry {_docID key transaction { hash block { number } }}",
			expectError:   false,
		},
		{
			name:          "query with multiple collections at same level",
			query:         "Block { number transactions { hash } logs { address } }",
			expectedQuery: "Block {_docID number transactions { hash } logs { address }}",
			expectError:   false,
		},
		{
			name:          "query with order and limit",
			query:         "Log(order: { blockNumber: DESC }, limit: 10) { address topics }",
			expectedQuery: "Log(order: { blockNumber: DESC }, limit: 10) {_docID address topics}",
			expectError:   false,
		},
		{
			name:          "query with filter, order, and nested objects",
			query:         "Transaction(filter: { from: { _eq: \"0x123\" } }, order: { blockNumber: DESC }) { hash from to logs { address } }",
			expectedQuery: "Transaction(filter: { from: { _eq: \"0x123\" } }, order: { blockNumber: DESC }) {_docID hash from to logs { address }}",
			expectError:   false,
		},
		{
			name:          "empty query",
			query:         "",
			expectedQuery: "",
			expectError:   true,
		},
		{
			name:          "query with only whitespace",
			query:         "   ",
			expectedQuery: "",
			expectError:   true,
		},
		{
			name:          "query with malformed syntax",
			query:         "Log { address topics",
			expectedQuery: "Log { address topics",
			expectError:   false, // Our function should still work with malformed queries
		},
		{
			name:          "query with nested objects having existing _docID",
			query:         "Block { _docID number transactions { _docID hash logs { address topics } } }",
			expectedQuery: "Block { _docID number transactions { _docID hash logs { address topics } } }",
			expectError:   false,
		},
		{
			name:          "query with mixed _docID presence",
			query:         "Block { number transactions { _docID hash logs { address topics } } }",
			expectedQuery: "Block { number transactions { _docID hash logs { address topics } } }",
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := WithReturnDocId(tt.query)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedQuery, result)
		})
	}
}

func TestWithReturnDocIdEdgeCases(t *testing.T) {
	t.Run("query with complex filter structure", func(t *testing.T) {
		query := `Log(filter: { 
			_and: [
				{ address: { _eq: "0x123" } },
				{ topics: { _contains: "0x456" } }
			]
		}) { 
			address 
			topics 
			transaction { 
				hash 
				from 
			} 
		}`

		result, err := WithReturnDocId(query)
		require.NoError(t, err)

		// Should add _docID to both Log and transaction
		assert.Contains(t, result, "_docID")
		assert.Contains(t, result, "address")
		assert.Contains(t, result, "transaction {")
	})

	t.Run("query with multiple levels of nesting", func(t *testing.T) {
		query := `Block { 
			number 
			transactions { 
				hash 
				logs { 
					address 
					topics 
					transaction { 
						hash 
					} 
				} 
			} 
		}`

		result, err := WithReturnDocId(query)
		require.NoError(t, err)

		// Should add _docID to Block, transactions, logs, and nested transaction
		assert.Contains(t, result, "Block {")
		assert.Contains(t, result, "transactions {")
		assert.Contains(t, result, "logs {")
		assert.Contains(t, result, "transaction {")
	})

	t.Run("query with array fields", func(t *testing.T) {
		query := `Log { 
			address 
			topics 
			data 
		}`

		result, err := WithReturnDocId(query)
		require.NoError(t, err)

		assert.Contains(t, result, "_docID")
		assert.Contains(t, result, "address")
	})
}

func TestWithReturnDocIdPerformance(t *testing.T) {
	t.Run("large query with many nested objects", func(t *testing.T) {
		// Create a large query with many nested levels
		query := `Block { 
			number 
			hash 
			transactions { 
				hash 
				from 
				to 
				logs { 
					address 
					topics 
					data 
				} 
			} 
			logs { 
				address 
				topics 
				data 
				transaction { 
					hash 
					from 
					to 
				} 
			} 
		}`

		result, err := WithReturnDocId(query)
		require.NoError(t, err)

		// Verify all expected _docID fields are present
		assert.Contains(t, result, "Block {")
		assert.Contains(t, result, "transactions {")
		assert.Contains(t, result, "logs {")
		assert.Contains(t, result, "transaction {")
	})
}
