package host

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSortDescendingBlockNumber_EmptySlice(t *testing.T) {
	documents := []map[string]any{}
	result, err := sortDescendingBlockNumber(documents)
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestSortDescendingBlockNumber_SingleDocument(t *testing.T) {
	documents := []map[string]any{
		{"blockNumber": uint64(100), "data": "test"},
	}
	result, err := sortDescendingBlockNumber(documents)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, uint64(100), result[0]["blockNumber"])
}

func TestSortDescendingBlockNumber_BlockNumberField_LogTransaction(t *testing.T) {
	// Test with blockNumber field (Log, Transaction)
	documents := []map[string]any{
		{"blockNumber": uint64(100), "address": "0x123"},
		{"blockNumber": uint64(300), "address": "0x456"},
		{"blockNumber": uint64(200), "address": "0x789"},
	}
	result, err := sortDescendingBlockNumber(documents)
	require.NoError(t, err)
	require.Len(t, result, 3)
	require.Equal(t, uint64(300), result[0]["blockNumber"])
	require.Equal(t, uint64(200), result[1]["blockNumber"])
	require.Equal(t, uint64(100), result[2]["blockNumber"])
}

func TestSortDescendingBlockNumber_NumberField_Block(t *testing.T) {
	// Test with number field (Block)
	documents := []map[string]any{
		{"number": uint64(50), "hash": "0xabc"},
		{"number": uint64(150), "hash": "0xdef"},
		{"number": uint64(100), "hash": "0xghi"},
	}
	result, err := sortDescendingBlockNumber(documents)
	require.NoError(t, err)
	require.Len(t, result, 3)
	require.Equal(t, uint64(150), result[0]["number"])
	require.Equal(t, uint64(100), result[1]["number"])
	require.Equal(t, uint64(50), result[2]["number"])
}

func TestSortDescendingBlockNumber_NestedTransactionBlockNumber_AccessListEntry(t *testing.T) {
	// Test with nested transaction.blockNumber (AccessListEntry)
	documents := []map[string]any{
		{
			"address": "0x111",
			"transaction": map[string]any{
				"blockNumber": uint64(500),
				"hash":        "0xtx1",
			},
		},
		{
			"address": "0x222",
			"transaction": map[string]any{
				"blockNumber": uint64(300),
				"hash":        "0xtx2",
			},
		},
		{
			"address": "0x333",
			"transaction": map[string]any{
				"blockNumber": uint64(400),
				"hash":        "0xtx3",
			},
		},
	}
	result, err := sortDescendingBlockNumber(documents)
	require.NoError(t, err)
	require.Len(t, result, 3)
	require.Equal(t, uint64(500), result[0]["transaction"].(map[string]any)["blockNumber"])
	require.Equal(t, uint64(400), result[1]["transaction"].(map[string]any)["blockNumber"])
	require.Equal(t, uint64(300), result[2]["transaction"].(map[string]any)["blockNumber"])
}

func TestSortDescendingBlockNumber_DifferentNumericTypes(t *testing.T) {
	// Test with different numeric types that might come from JSON unmarshaling
	documents := []map[string]any{
		{"blockNumber": int(100)},
		{"blockNumber": int64(300)},
		{"blockNumber": int32(200)},
		{"blockNumber": uint32(250)},
		{"blockNumber": float64(150)}, // JSON numbers often come as float64
		{"blockNumber": float32(350)},
	}
	result, err := sortDescendingBlockNumber(documents)
	require.NoError(t, err)
	require.Len(t, result, 6)
	// Should be sorted: 350, 300, 250, 200, 150, 100
	require.Equal(t, float32(350), result[0]["blockNumber"])
	require.Equal(t, int64(300), result[1]["blockNumber"])
	require.Equal(t, uint32(250), result[2]["blockNumber"])
	require.Equal(t, int32(200), result[3]["blockNumber"])
	require.Equal(t, float64(150), result[4]["blockNumber"])
	require.Equal(t, int(100), result[5]["blockNumber"])
}

func TestSortDescendingBlockNumber_AlreadySortedDescending(t *testing.T) {
	// Test that already sorted descending order is maintained
	documents := []map[string]any{
		{"blockNumber": uint64(300)},
		{"blockNumber": uint64(200)},
		{"blockNumber": uint64(100)},
	}
	result, err := sortDescendingBlockNumber(documents)
	require.NoError(t, err)
	require.Len(t, result, 3)
	require.Equal(t, uint64(300), result[0]["blockNumber"])
	require.Equal(t, uint64(200), result[1]["blockNumber"])
	require.Equal(t, uint64(100), result[2]["blockNumber"])
}

func TestSortDescendingBlockNumber_AlreadySortedAscending(t *testing.T) {
	// Test that ascending order is reversed to descending
	documents := []map[string]any{
		{"blockNumber": uint64(100)},
		{"blockNumber": uint64(200)},
		{"blockNumber": uint64(300)},
	}
	result, err := sortDescendingBlockNumber(documents)
	require.NoError(t, err)
	require.Len(t, result, 3)
	require.Equal(t, uint64(300), result[0]["blockNumber"])
	require.Equal(t, uint64(200), result[1]["blockNumber"])
	require.Equal(t, uint64(100), result[2]["blockNumber"])
}

func TestSortDescendingBlockNumber_Unsorted(t *testing.T) {
	// Test with completely unsorted data
	documents := []map[string]any{
		{"blockNumber": uint64(42)},
		{"blockNumber": uint64(1000)},
		{"blockNumber": uint64(1)},
		{"blockNumber": uint64(500)},
		{"blockNumber": uint64(250)},
	}
	result, err := sortDescendingBlockNumber(documents)
	require.NoError(t, err)
	require.Len(t, result, 5)
	require.Equal(t, uint64(1000), result[0]["blockNumber"])
	require.Equal(t, uint64(500), result[1]["blockNumber"])
	require.Equal(t, uint64(250), result[2]["blockNumber"])
	require.Equal(t, uint64(42), result[3]["blockNumber"])
	require.Equal(t, uint64(1), result[4]["blockNumber"])
}

func TestSortDescendingBlockNumber_ZeroBlockNumber(t *testing.T) {
	// Test with zero block number
	documents := []map[string]any{
		{"blockNumber": uint64(100)},
		{"blockNumber": uint64(0)},
		{"blockNumber": uint64(50)},
	}
	result, err := sortDescendingBlockNumber(documents)
	require.NoError(t, err)
	require.Len(t, result, 3)
	require.Equal(t, uint64(100), result[0]["blockNumber"])
	require.Equal(t, uint64(50), result[1]["blockNumber"])
	require.Equal(t, uint64(0), result[2]["blockNumber"])
}

func TestSortDescendingBlockNumber_LargeNumbers(t *testing.T) {
	// Test with very large block numbers
	documents := []map[string]any{
		{"blockNumber": uint64(18446744073709551615)}, // max uint64
		{"blockNumber": uint64(1000000000)},
		{"blockNumber": uint64(5000000000)},
	}
	result, err := sortDescendingBlockNumber(documents)
	require.NoError(t, err)
	require.Len(t, result, 3)
	require.Equal(t, uint64(18446744073709551615), result[0]["blockNumber"])
	require.Equal(t, uint64(5000000000), result[1]["blockNumber"])
	require.Equal(t, uint64(1000000000), result[2]["blockNumber"])
}

func TestSortDescendingBlockNumber_DuplicateBlockNumbers(t *testing.T) {
	// Test with duplicate block numbers (should maintain relative order)
	documents := []map[string]any{
		{"blockNumber": uint64(100), "id": "a"},
		{"blockNumber": uint64(200), "id": "b"},
		{"blockNumber": uint64(100), "id": "c"},
		{"blockNumber": uint64(200), "id": "d"},
	}
	result, err := sortDescendingBlockNumber(documents)
	require.NoError(t, err)
	require.Len(t, result, 4)
	// Both 200s should come first, then both 100s
	require.Equal(t, uint64(200), result[0]["blockNumber"])
	require.Equal(t, uint64(200), result[1]["blockNumber"])
	require.Equal(t, uint64(100), result[2]["blockNumber"])
	require.Equal(t, uint64(100), result[3]["blockNumber"])
}

func TestSortDescendingBlockNumber_DoesNotMutateOriginal(t *testing.T) {
	// Test that the original slice is not mutated
	original := []map[string]any{
		{"blockNumber": uint64(100)},
		{"blockNumber": uint64(300)},
		{"blockNumber": uint64(200)},
	}
	originalCopy := make([]map[string]any, len(original))
	for i, doc := range original {
		originalCopy[i] = make(map[string]any)
		for k, v := range doc {
			originalCopy[i][k] = v
		}
	}

	result, err := sortDescendingBlockNumber(original)
	require.NoError(t, err)
	require.Len(t, result, 3)

	// Original should be unchanged
	require.Equal(t, uint64(100), original[0]["blockNumber"])
	require.Equal(t, uint64(300), original[1]["blockNumber"])
	require.Equal(t, uint64(200), original[2]["blockNumber"])

	// Result should be sorted
	require.Equal(t, uint64(300), result[0]["blockNumber"])
	require.Equal(t, uint64(200), result[1]["blockNumber"])
	require.Equal(t, uint64(100), result[2]["blockNumber"])
}

func TestSortDescendingBlockNumber_InvalidDocuments_NoError(t *testing.T) {
	// Test that documents with invalid blockNumber don't cause the function to error
	// The function gracefully handles invalid documents by maintaining their relative order
	documents := []map[string]any{
		{"blockNumber": uint64(100)},        // Valid
		{"data": "test"},                     // Missing blockNumber
		{"blockNumber": nil},                 // Nil blockNumber
		{"blockNumber": "not-a-number"},     // Invalid type
		{"blockNumber": int64(-100)},        // Negative number
		{"blockNumber": uint64(200)},        // Valid
	}
	result, err := sortDescendingBlockNumber(documents)
	require.NoError(t, err)
	require.Len(t, result, 6)
	// Function should complete without error even with invalid documents
	// The exact ordering with mixed valid/invalid is implementation-dependent
}

func TestSortDescendingBlockNumber_AllInvalid_MaintainsOrder(t *testing.T) {
	// Test that when all documents are invalid, original order is maintained
	documents := []map[string]any{
		{"data": "test1"},
		{"blockNumber": nil},
		{"blockNumber": "invalid"},
	}
	result, err := sortDescendingBlockNumber(documents)
	require.NoError(t, err)
	require.Len(t, result, 3)
	// Order should be maintained since all are invalid
	require.Equal(t, "test1", result[0]["data"])
	require.Nil(t, result[1]["blockNumber"])
	require.Equal(t, "invalid", result[2]["blockNumber"])
}

func TestSortDescendingBlockNumber_MixedFieldTypes(t *testing.T) {
	// Test with mixed field types (blockNumber, number, transaction.blockNumber)
	documents := []map[string]any{
		{"blockNumber": uint64(100)}, // Log/Transaction
		{"number": uint64(300)},       // Block
		{
			"transaction": map[string]any{
				"blockNumber": uint64(200),
			},
		}, // AccessListEntry
	}
	result, err := sortDescendingBlockNumber(documents)
	require.NoError(t, err)
	require.Len(t, result, 3)
	// Should be sorted: 300, 200, 100
	require.Equal(t, uint64(300), result[0]["number"])
	require.Equal(t, uint64(200), result[1]["transaction"].(map[string]any)["blockNumber"])
	require.Equal(t, uint64(100), result[2]["blockNumber"])
}

func TestSortDescendingBlockNumber_Priority_BlockNumberOverNumber(t *testing.T) {
	// Test that blockNumber takes priority over number when both exist
	documents := []map[string]any{
		{"blockNumber": uint64(100), "number": uint64(500)}, // Should use blockNumber
		{"blockNumber": uint64(200), "number": uint64(400)}, // Should use blockNumber
	}
	result, err := sortDescendingBlockNumber(documents)
	require.NoError(t, err)
	require.Len(t, result, 2)
	// Should be sorted by blockNumber (200, 100), not number
	require.Equal(t, uint64(200), result[0]["blockNumber"])
	require.Equal(t, uint64(100), result[1]["blockNumber"])
}

