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

// ---------------------------------------------------------------------------
// findMatchingCloseBrace – direct unit tests
// ---------------------------------------------------------------------------

func TestFindMatchingCloseBrace(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		openBracePos int
		expected     int
	}{
		{
			name:         "simple braces",
			query:        "{ hello }",
			openBracePos: 0,
			expected:     8,
		},
		{
			name:         "nested braces",
			query:        "{ a { b } }",
			openBracePos: 0,
			expected:     10,
		},
		{
			name:         "position out of range",
			query:        "{ hello }",
			openBracePos: 100,
			expected:     -1,
		},
		{
			name:         "position at non-brace character",
			query:        "hello { world }",
			openBracePos: 0,
			expected:     -1,
		},
		{
			name:         "unmatched opening brace",
			query:        "{ hello { world",
			openBracePos: 0,
			expected:     -1,
		},
		{
			name:         "empty string",
			query:        "",
			openBracePos: 0,
			expected:     -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findMatchingCloseBrace(tt.query, tt.openBracePos)
			require.Equal(t, tt.expected, result)
		})
	}
}

// ---------------------------------------------------------------------------
// addDocIdAndVersionToSelectionSets – additional edge cases
// ---------------------------------------------------------------------------

func TestAddDocIdAndVersionToSelectionSets_NoBraces(t *testing.T) {
	// Query with no braces at all
	result, err := addDocIdAndVersionToSelectionSets("Log address topics")
	require.NoError(t, err)
	require.Equal(t, "Log address topics", result, "no braces should return as-is")
}

func TestAddDocIdAndVersionToSelectionSets_AllBracesInsideParens(t *testing.T) {
	// All braces are inside parentheses, so no main selection set brace found
	query := "Log(filter: { address: { _eq: \"0x123\" } })"
	result, err := addDocIdAndVersionToSelectionSets(query)
	require.NoError(t, err)
	require.Equal(t, query, result, "all braces inside parens should return as-is")
}

func TestAddDocIdAndVersionToSelectionSets_UnmatchedMainBrace(t *testing.T) {
	// Main selection set opening brace with no matching closing brace
	query := "Log { address topics"
	result, err := addDocIdAndVersionToSelectionSets(query)
	require.NoError(t, err)
	require.Equal(t, query, result, "unmatched main brace should return as-is")
}

func TestWithReturnDocIdAndVersion_VersionMiddleOfContent(t *testing.T) {
	// _version in the middle of content (not at beginning), with incomplete structure
	query := constants.CollectionLog + " { address _version { cid } topics }"
	result, err := WithReturnDocIdAndVersion(query)
	require.NoError(t, err)
	require.Contains(t, result, "_docID")
	require.Contains(t, result, "_version { cid signature { type identity value } }")
	require.Contains(t, result, "address")
	require.Contains(t, result, "topics")
}

func TestWithReturnDocIdAndVersion_DocIDMiddleOfContent(t *testing.T) {
	// _docID not at the start of content
	query := constants.CollectionLog + " { address _docID topics }"
	result, err := WithReturnDocIdAndVersion(query)
	require.NoError(t, err)
	require.Contains(t, result, "_docID")
	require.Contains(t, result, "_version { cid signature { type identity value } }")
}

// ---------------------------------------------------------------------------
// addDocIdAndVersionToSelectionSets – additional branch coverage
// ---------------------------------------------------------------------------

func TestAddDocIdAndVersionToSelectionSets_CompleteVersionAtStart(t *testing.T) {
	// _version with complete pattern at the very beginning of content, followed by a space and more content
	completeVersion := "_version { cid signature { type identity value } }"
	query := constants.CollectionLog + " { " + completeVersion + " address topics }"
	result, err := addDocIdAndVersionToSelectionSets(query)
	require.NoError(t, err)
	require.Contains(t, result, "_docID")
	require.Contains(t, result, "_version { cid signature { type identity value } }")
	require.Contains(t, result, "address")
	require.Contains(t, result, "topics")
}

func TestAddDocIdAndVersionToSelectionSets_CompleteVersionOnly(t *testing.T) {
	// Content is exactly the complete version pattern (nothing else)
	completeVersion := "_version { cid signature { type identity value } }"
	query := constants.CollectionLog + " { " + completeVersion + " }"
	result, err := addDocIdAndVersionToSelectionSets(query)
	require.NoError(t, err)
	require.Contains(t, result, "_docID")
	require.Contains(t, result, "_version { cid signature { type identity value } }")
}

func TestAddDocIdAndVersionToSelectionSets_CompleteVersionNoTrailingSpace(t *testing.T) {
	// _version at start of content with no trailing space (followed immediately by closing brace)
	completeVersion := "_version { cid signature { type identity value } }"
	query := constants.CollectionLog + " {" + completeVersion + "}"
	result, err := addDocIdAndVersionToSelectionSets(query)
	require.NoError(t, err)
	require.Contains(t, result, "_docID")
}

func TestAddDocIdAndVersionToSelectionSets_VersionFallbackRemoval(t *testing.T) {
	// Test where _version is in the middle of content with the complete pattern preceded by a space
	// This exercises the fallback path: strings.Index(remainingContent, " "+completePattern+" ")
	completeVersion := "_version { cid signature { type identity value } }"
	query := constants.CollectionLog + " { address " + completeVersion + " topics }"
	result, err := addDocIdAndVersionToSelectionSets(query)
	require.NoError(t, err)
	require.Contains(t, result, "_docID")
	require.Contains(t, result, "_version { cid signature { type identity value } }")
	require.Contains(t, result, "address")
	require.Contains(t, result, "topics")
}

func TestAddDocIdAndVersionToSelectionSets_VersionFallbackEndOfContent(t *testing.T) {
	// _version with complete pattern at the end of content preceded by a space
	// This exercises: strings.Index(remainingContent, " "+completePattern) at the end
	completeVersion := "_version { cid signature { type identity value } }"
	query := constants.CollectionLog + " { address topics " + completeVersion + " }"
	result, err := addDocIdAndVersionToSelectionSets(query)
	require.NoError(t, err)
	require.Contains(t, result, "_docID")
	require.Contains(t, result, "_version { cid signature { type identity value } }")
	require.Contains(t, result, "address")
	require.Contains(t, result, "topics")
}

func TestAddDocIdAndVersionToSelectionSets_IncompleteVersionMiddleNoNested(t *testing.T) {
	// _version without braces in the middle of content - triggers the "Just _version with no nested structure" path
	query := constants.CollectionLog + " { address _version topics }"
	result, err := addDocIdAndVersionToSelectionSets(query)
	require.NoError(t, err)
	require.Contains(t, result, "_docID")
	require.Contains(t, result, "_version { cid signature { type identity value } }")
	require.Contains(t, result, "address")
	require.Contains(t, result, "topics")
}

func TestAddDocIdAndVersionToSelectionSets_DocIDOnlyContent(t *testing.T) {
	// Content is exactly _docID
	query := constants.CollectionLog + " {_docID}"
	result, err := addDocIdAndVersionToSelectionSets(query)
	require.NoError(t, err)
	require.Contains(t, result, "_docID")
	require.Contains(t, result, "_version { cid signature { type identity value } }")
}

func TestAddDocIdAndVersionToSelectionSets_DocIDWithSpace(t *testing.T) {
	// Content starts with "_docID " followed by more content
	query := constants.CollectionLog + " { _docID address topics }"
	result, err := addDocIdAndVersionToSelectionSets(query)
	require.NoError(t, err)
	require.Contains(t, result, "_docID")
	require.Contains(t, result, "_version { cid signature { type identity value } }")
	require.Contains(t, result, "address")
}

func TestAddDocIdAndVersionToSelectionSets_DocIDAndCompleteVersion(t *testing.T) {
	// Both _docID and complete _version already present - should return as-is
	completeVersion := "_version { cid signature { type identity value } }"
	query := constants.CollectionLog + " { _docID " + completeVersion + " address }"
	result, err := addDocIdAndVersionToSelectionSets(query)
	require.NoError(t, err)
	require.Contains(t, result, "_docID")
	require.Contains(t, result, completeVersion)
	require.Contains(t, result, "address")
}

func TestAddDocIdAndVersionToSelectionSets_DocIDPrefixNoSpace(t *testing.T) {
	// _docID immediately followed by another token (no separating space).
	// This exercises the HasPrefix("_docID") fallback in lines 141-142.
	query := constants.CollectionLog + " {_docIDextra field1 field2}"
	result, err := addDocIdAndVersionToSelectionSets(query)
	require.NoError(t, err)
	require.Contains(t, result, "_docID")
	require.Contains(t, result, "_version { cid signature { type identity value } }")
}

func TestWithReturnDocIdAndVersion_NeedVersionHasVersionNeedDocID(t *testing.T) {
	// Has _version (complete) but no _docID => needDocID true, needVersion false, hasVersion true.
	// This exercises the second half of the condition on line 228:
	//   (hasVersion && needDocID) for the spacing logic.
	completeVersion := "_version { cid signature { type identity value } }"
	query := constants.CollectionLog + " { " + completeVersion + " address topics }"
	result, err := WithReturnDocIdAndVersion(query)
	require.NoError(t, err)
	require.Contains(t, result, "_docID")
	require.Contains(t, result, completeVersion)
	require.Contains(t, result, "address")
	require.Contains(t, result, "topics")
}

func TestWithReturnDocIdAndVersion_EmptyContentAfterDocIDRemoval(t *testing.T) {
	// Content is just "_docID" with no other fields
	// After removing _docID, remaining content is empty
	query := constants.CollectionLog + " { _docID }"
	result, err := WithReturnDocIdAndVersion(query)
	require.NoError(t, err)
	require.Contains(t, result, "_docID")
	require.Contains(t, result, "_version { cid signature { type identity value } }")
}

func TestWithReturnDocIdAndVersion_IncompleteVersionUnmatchedBrace(t *testing.T) {
	// _version with an opening brace but no matching close brace within the selection set.
	// The outer `}` is consumed as the main selection set close, so findMatchingCloseBrace
	// returns -1 and the query is returned as-is.
	query := constants.CollectionLog + " { address _version { cid }"
	result, err := WithReturnDocIdAndVersion(query)
	require.NoError(t, err)
	// The function can't properly parse this malformed query, returns as-is
	require.Contains(t, result, "_version")
}
