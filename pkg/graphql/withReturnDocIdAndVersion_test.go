package graphql

import (
	"testing"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithReturnDocIdAndVersion(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedQuery string
		expectError   bool
	}{
		{
			name:          "simple query without _docID or _version",
			query:         constants.CollectionLog + " { address topics data transactionHash }",
			expectedQuery: constants.CollectionLog + " {_docID _version { cid signature { type identity value } } address topics data transactionHash}",
			expectError:   false,
		},
		{
			name:          "query with existing _docID but no _version",
			query:         constants.CollectionLog + " { _docID address topics data }",
			expectedQuery: constants.CollectionLog + " {_docID _version { cid signature { type identity value } } address topics data}",
			expectError:   false,
		},
		{
			name:          "query with existing _version but no _docID",
			query:         constants.CollectionLog + " { _version { cid signature { type identity value } } address topics data }",
			expectedQuery: constants.CollectionLog + " {_docID  _version { cid signature { type identity value } } address topics data}",
			expectError:   false,
		},
		{
			name:          "query with existing _docID and _version",
			query:         constants.CollectionLog + " { _docID _version { cid signature { type identity value } } address topics data }",
			expectedQuery: constants.CollectionLog + " { _docID _version { cid signature { type identity value } } address topics data }",
			expectError:   false,
		},
		{
			name:          "query with nested objects without _docID or _version",
			query:         constants.CollectionTransaction + " { hash from to logs { address topics } }",
			expectedQuery: constants.CollectionTransaction + " {_docID _version { cid signature { type identity value } } hash from to logs { address topics }}",
			expectError:   false,
		},
		{
			name:          "query with nested objects where some have _docID",
			query:         constants.CollectionTransaction + " { _docID hash from to logs { address topics } }",
			expectedQuery: constants.CollectionTransaction + " {_docID _version { cid signature { type identity value } } hash from to logs { address topics }}",
			expectError:   false,
		},
		{
			name:          "query with deeply nested objects",
			query:         constants.CollectionBlock + " { number hash transactions { hash logs { address topics } } }",
			expectedQuery: constants.CollectionBlock + " {_docID _version { cid signature { type identity value } } number hash transactions { hash logs { address topics } }}",
			expectError:   false,
		},
		{
			name:          "query with filter and nested objects",
			query:         constants.CollectionLog + "(filter: { address: { _eq: \"0x123\" } }) { address topics transaction { hash } }",
			expectedQuery: constants.CollectionLog + "(filter: { address: { _eq: \"0x123\" } }) {_docID _version { cid signature { type identity value } } address topics transaction { hash }}",
			expectError:   false,
		},
		{
			name:          "query with multiple nested levels",
			query:         constants.CollectionBlock + " { number transactions { hash logs { address topics } } }",
			expectedQuery: constants.CollectionBlock + " {_docID _version { cid signature { type identity value } } number transactions { hash logs { address topics } }}",
			expectError:   false,
		},
		{
			name:          "query with empty selection set",
			query:         constants.CollectionLog + " { }",
			expectedQuery: constants.CollectionLog + " {_docID _version { cid signature { type identity value } }}",
			expectError:   false,
		},
		{
			name:          "query with only _docID",
			query:         constants.CollectionLog + " { _docID }",
			expectedQuery: constants.CollectionLog + " {_docID _version { cid signature { type identity value } }}",
			expectError:   false,
		},
		{
			name:          "query with only _version",
			query:         constants.CollectionLog + " { _version { cid signature { type identity value } } }",
			expectedQuery: constants.CollectionLog + " {_docID  _version { cid signature { type identity value } }}",
			expectError:   false,
		},
		{
			name:          "query with complex nested structure",
			query:         constants.CollectionBlock + " { number hash transactions { hash from to logs { address topics data } } }",
			expectedQuery: constants.CollectionBlock + " {_docID _version { cid signature { type identity value } } number hash transactions { hash from to logs { address topics data } }}",
			expectError:   false,
		},
		{
			name:          "query with Ethereum__Mainnet__AccessListEntry nested structure",
			query:         constants.CollectionAccessListEntry + " { key transaction { hash block { number } } }",
			expectedQuery: constants.CollectionAccessListEntry + " {_docID _version { cid signature { type identity value } } key transaction { hash block { number } }}",
			expectError:   false,
		},
		{
			name:          "query with multiple collections at same level",
			query:         constants.CollectionBlock + " { number transactions { hash } logs { address } }",
			expectedQuery: constants.CollectionBlock + " {_docID _version { cid signature { type identity value } } number transactions { hash } logs { address }}",
			expectError:   false,
		},
		{
			name:          "query with order and limit",
			query:         constants.CollectionLog + " (order: { blockNumber: DESC }, limit: 10) { address topics }",
			expectedQuery: constants.CollectionLog + " (order: { blockNumber: DESC }, limit: 10) {_docID _version { cid signature { type identity value } } address topics}",
			expectError:   false,
		},
		{
			name:          "query with filter, order, and nested objects",
			query:         constants.CollectionTransaction + "(filter: { from: { _eq: \"0x123\" } }, order: { blockNumber: DESC }) { hash from to logs { address } }",
			expectedQuery: constants.CollectionTransaction + "(filter: { from: { _eq: \"0x123\" } }, order: { blockNumber: DESC }) {_docID _version { cid signature { type identity value } } hash from to logs { address }}",
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
			query:         constants.CollectionLog + " { address topics",
			expectedQuery: constants.CollectionLog + " { address topics",
			expectError:   false, // Our function should still work with malformed queries
		},
		{
			name:          "query with nested objects having existing _docID",
			query:         constants.CollectionBlock + " { _docID number transactions { _docID hash logs { address topics } } }",
			expectedQuery: constants.CollectionBlock + " {_docID _version { cid signature { type identity value } } number transactions { _docID hash logs { address topics } }}",
			expectError:   false,
		},
		{
			name:          "query with mixed _docID presence",
			query:         constants.CollectionBlock + " { number transactions { _docID hash logs { address topics } } }",
			expectedQuery: constants.CollectionBlock + " {_docID _version { cid signature { type identity value } } number transactions { _docID hash logs { address topics } }}",
			expectError:   false,
		},
		{
			name:          "query with incomplete _version (no nested fields)",
			query:         constants.CollectionLog + " { _version address topics data }",
			expectedQuery: constants.CollectionLog + " {_docID _version { cid signature { type identity value } } address topics data}",
			expectError:   false,
		},
		{
			name:          "query with incomplete _version (only cid)",
			query:         constants.CollectionLog + " { _version { cid } address topics data }",
			expectedQuery: constants.CollectionLog + " {_docID _version { cid signature { type identity value } } address topics data}",
			expectError:   false,
		},
		{
			name:          "query with incomplete _version (only signature with type)",
			query:         constants.CollectionLog + " { _version { signature { type } } address topics data }",
			expectedQuery: constants.CollectionLog + " {_docID _version { cid signature { type identity value } } address topics data}",
			expectError:   false,
		},
		{
			name:          "query with incomplete _version (signature without all fields)",
			query:         constants.CollectionLog + " { _version { cid signature { type identity } } address topics data }",
			expectedQuery: constants.CollectionLog + " {_docID _version { cid signature { type identity value } } address topics data}",
			expectError:   false,
		},
		{
			name:          "query with incomplete _version and existing _docID",
			query:         constants.CollectionLog + " { _docID _version { cid } address topics data }",
			expectedQuery: constants.CollectionLog + " {_docID _version { cid signature { type identity value } } address topics data}",
			expectError:   false,
		},
		{
			name:          "query with incomplete _version (only _version field)",
			query:         constants.CollectionLog + " { _version }",
			expectedQuery: constants.CollectionLog + " {_docID _version { cid signature { type identity value } } }",
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := WithReturnDocIdAndVersion(tt.query)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedQuery, result)
		})
	}
}

func TestWithReturnDocIdAndVersionEdgeCases(t *testing.T) {
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

		result, err := WithReturnDocIdAndVersion(query)
		require.NoError(t, err)

		// Should add _docID and _version to Log
		assert.Contains(t, result, "_docID")
		assert.Contains(t, result, "_version")
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

		result, err := WithReturnDocIdAndVersion(query)
		require.NoError(t, err)

		// Should add _docID and _version to Block
		assert.Contains(t, result, "_docID")
		assert.Contains(t, result, "_version")
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

		result, err := WithReturnDocIdAndVersion(query)
		require.NoError(t, err)

		assert.Contains(t, result, "_docID")
		assert.Contains(t, result, "_version")
		assert.Contains(t, result, "address")
	})
}

func TestWithReturnDocIdAndVersionPerformance(t *testing.T) {
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

		result, err := WithReturnDocIdAndVersion(query)
		require.NoError(t, err)

		// Verify all expected _docID and _version fields are present
		assert.Contains(t, result, "_docID")
		assert.Contains(t, result, "_version")
		assert.Contains(t, result, "Block {")
		assert.Contains(t, result, "transactions {")
		assert.Contains(t, result, "logs {")
		assert.Contains(t, result, "transaction {")
	})
}
