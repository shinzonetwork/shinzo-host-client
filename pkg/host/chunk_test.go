package host

import (
	"testing"

	"github.com/shinzonetwork/app-sdk/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// Initialize logger for tests
	logger.Init(true)
}

func TestProcessingChunk_IsProcessingComplete(t *testing.T) {
	tests := []struct {
		name           string
		chunk          *ProcessingChunk
		expectedResult bool
	}{
		{
			name: "complete chunk with no missing blocks",
			chunk: &ProcessingChunk{
				StartBlock:    1,
				EndBlock:      100,
				MissingBlocks: []uint64{},
			},
			expectedResult: true,
		},
		{
			name: "incomplete chunk with missing blocks",
			chunk: &ProcessingChunk{
				StartBlock:    1,
				EndBlock:      100,
				MissingBlocks: []uint64{50, 75},
			},
			expectedResult: false,
		},
		{
			name: "chunk with nil missing blocks",
			chunk: &ProcessingChunk{
				StartBlock:    1,
				EndBlock:      100,
				MissingBlocks: nil,
			},
			expectedResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.chunk.IsProcessingComplete()
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestProcessingChunk_RemoveProcessedBlocks(t *testing.T) {
	tests := []struct {
		name           string
		chunk          *ProcessingChunk
		processed      []uint64
		expectedMissing []uint64
	}{
		{
			name: "remove some blocks",
			chunk: &ProcessingChunk{
				StartBlock:    1,
				EndBlock:      100,
				MissingBlocks: []uint64{10, 20, 30, 40, 50},
			},
			processed:      []uint64{20, 40},
			expectedMissing: []uint64{10, 30, 50},
		},
		{
			name: "remove all blocks",
			chunk: &ProcessingChunk{
				StartBlock:    1,
				EndBlock:      100,
				MissingBlocks: []uint64{10, 20, 30},
			},
			processed:      []uint64{10, 20, 30},
			expectedMissing: []uint64{},
		},
		{
			name: "remove no blocks (empty processed list)",
			chunk: &ProcessingChunk{
				StartBlock:    1,
				EndBlock:      100,
				MissingBlocks: []uint64{10, 20, 30},
			},
			processed:      []uint64{},
			expectedMissing: []uint64{10, 20, 30},
		},
		{
			name: "remove blocks not in missing list",
			chunk: &ProcessingChunk{
				StartBlock:    1,
				EndBlock:      100,
				MissingBlocks: []uint64{10, 20, 30},
			},
			processed:      []uint64{40, 50},
			expectedMissing: []uint64{10, 20, 30},
		},
		{
			name: "remove duplicate processed blocks",
			chunk: &ProcessingChunk{
				StartBlock:    1,
				EndBlock:      100,
				MissingBlocks: []uint64{10, 20, 30},
			},
			processed:      []uint64{20, 20, 30},
			expectedMissing: []uint64{10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.chunk.RemoveProcessedBlocks(tt.processed)
			assert.Equal(t, tt.expectedMissing, tt.chunk.MissingBlocks)
		})
	}
}

func TestGetNextChunkStart(t *testing.T) {
	tests := []struct {
		name           string
		viewName       string
		chunks         []*ProcessingChunk
		expectedResult uint64
	}{
		{
			name:           "no chunks - returns 1",
			viewName:       "testView",
			chunks:         []*ProcessingChunk{},
			expectedResult: 1,
		},
		{
			name:     "single chunk",
			viewName: "testView",
			chunks: []*ProcessingChunk{
				{StartBlock: 1, EndBlock: 100, MissingBlocks: []uint64{}},
			},
			expectedResult: 101,
		},
		{
			name:     "multiple chunks",
			viewName: "testView",
			chunks: []*ProcessingChunk{
				{StartBlock: 1, EndBlock: 100, MissingBlocks: []uint64{}},
				{StartBlock: 101, EndBlock: 200, MissingBlocks: []uint64{}},
				{StartBlock: 201, EndBlock: 300, MissingBlocks: []uint64{}},
			},
			expectedResult: 301,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host := &Host{
				ViewProcessedChunks: map[string][]*ProcessingChunk{
					tt.viewName: tt.chunks,
				},
			}
			result := host.getNextChunkStart(tt.viewName)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestUpdateChunksForView_CreateFirstChunk(t *testing.T) {
	host := &Host{
		ViewProcessedChunks: map[string][]*ProcessingChunk{},
	}

	host.updateChunksForView("testView", 1, 100, []uint64{1, 2, 3}, []uint64{4, 5, 6})

	chunks := host.ViewProcessedChunks["testView"]
	require.Len(t, chunks, 1)
	chunk := chunks[0]
	assert.Equal(t, uint64(1), chunk.StartBlock)
	assert.Equal(t, uint64(100), chunk.EndBlock)
	// Missing blocks should be 4, 5, 6 (1, 2, 3 were processed)
	assert.Equal(t, []uint64{4, 5, 6}, chunk.MissingBlocks)
}

func TestUpdateChunksForView_ExtendContiguousChunk(t *testing.T) {
	host := &Host{
		ViewProcessedChunks: map[string][]*ProcessingChunk{
			"testView": {
				{StartBlock: 1, EndBlock: 100, MissingBlocks: []uint64{50}},
			},
		},
	}

	host.updateChunksForView("testView", 101, 200, []uint64{101, 102}, []uint64{103, 104})

	chunks := host.ViewProcessedChunks["testView"]
	require.Len(t, chunks, 1)
	chunk := chunks[0]
	assert.Equal(t, uint64(1), chunk.StartBlock)
	assert.Equal(t, uint64(200), chunk.EndBlock)
	// Should have original missing (50) plus new missing (103, 104), minus processed (101, 102)
	assert.Contains(t, chunk.MissingBlocks, uint64(50))
	assert.Contains(t, chunk.MissingBlocks, uint64(103))
	assert.Contains(t, chunk.MissingBlocks, uint64(104))
	assert.NotContains(t, chunk.MissingBlocks, uint64(101))
	assert.NotContains(t, chunk.MissingBlocks, uint64(102))
}

func TestUpdateChunksForView_CreateNewChunkWhenExceedingMaxMissing(t *testing.T) {
	// Create a chunk with maxMissingBlocksPerChunk missing blocks
	missingBlocks := make([]uint64, maxMissingBlocksPerChunk)
	for i := uint64(0); i < maxMissingBlocksPerChunk; i++ {
		missingBlocks[i] = i + 1
	}

	host := &Host{
		ViewProcessedChunks: map[string][]*ProcessingChunk{
			"testView": {
				{StartBlock: 1, EndBlock: 100, MissingBlocks: missingBlocks},
			},
		},
	}

	// Try to extend with more missing blocks - should create new chunk
	newMissing := []uint64{101, 102}
	host.updateChunksForView("testView", 101, 200, []uint64{}, newMissing)

	chunks := host.ViewProcessedChunks["testView"]
	require.Len(t, chunks, 2)
	
	// First chunk should be unchanged
	assert.Equal(t, uint64(1), chunks[0].StartBlock)
	assert.Equal(t, uint64(100), chunks[0].EndBlock)
	
	// Second chunk should be created
	assert.Equal(t, uint64(101), chunks[1].StartBlock)
	assert.Equal(t, uint64(200), chunks[1].EndBlock)
	assert.Equal(t, newMissing, chunks[1].MissingBlocks)
}

func TestUpdateChunksForView_ExtendChunkWithGap(t *testing.T) {
	host := &Host{
		ViewProcessedChunks: map[string][]*ProcessingChunk{
			"testView": {
				{StartBlock: 1, EndBlock: 100, MissingBlocks: []uint64{50}},
			},
		},
	}

	// Extend chunk with a gap (101-150 should be marked as missing)
	host.updateChunksForView("testView", 151, 200, []uint64{151}, []uint64{152})

	chunks := host.ViewProcessedChunks["testView"]
	require.Len(t, chunks, 1)
	chunk := chunks[0]
	assert.Equal(t, uint64(1), chunk.StartBlock)
	assert.Equal(t, uint64(200), chunk.EndBlock)
	// Should have gap blocks (101-150) as missing
	assert.Contains(t, chunk.MissingBlocks, uint64(101))
	assert.Contains(t, chunk.MissingBlocks, uint64(150))
	assert.Contains(t, chunk.MissingBlocks, uint64(152))
	assert.NotContains(t, chunk.MissingBlocks, uint64(151))
}

func TestUpdateChunksForView_UpdateOverlappingChunk(t *testing.T) {
	host := &Host{
		ViewProcessedChunks: map[string][]*ProcessingChunk{
			"testView": {
				{StartBlock: 1, EndBlock: 100, MissingBlocks: []uint64{50, 75}},
			},
		},
	}

	// Update overlapping range
	host.updateChunksForView("testView", 50, 75, []uint64{50, 75}, []uint64{})

	chunks := host.ViewProcessedChunks["testView"]
	require.Len(t, chunks, 1)
	chunk := chunks[0]
	assert.Equal(t, uint64(1), chunk.StartBlock)
	assert.Equal(t, uint64(100), chunk.EndBlock)
	// Missing blocks should be removed
	assert.NotContains(t, chunk.MissingBlocks, uint64(50))
	assert.NotContains(t, chunk.MissingBlocks, uint64(75))
}

func TestMergeContiguousChunks_MergeCompleteChunks(t *testing.T) {
	host := &Host{
		ViewProcessedChunks: map[string][]*ProcessingChunk{
			"testView": {
				{StartBlock: 1, EndBlock: 100, MissingBlocks: []uint64{}},
				{StartBlock: 101, EndBlock: 200, MissingBlocks: []uint64{}},
				{StartBlock: 201, EndBlock: 300, MissingBlocks: []uint64{}},
			},
		},
	}

	host.mergeContiguousChunks("testView")

	chunks := host.ViewProcessedChunks["testView"]
	require.Len(t, chunks, 1)
	chunk := chunks[0]
	assert.Equal(t, uint64(1), chunk.StartBlock)
	assert.Equal(t, uint64(300), chunk.EndBlock)
	assert.True(t, chunk.IsProcessingComplete())
}

func TestMergeContiguousChunks_MergeChunksWithinMaxMissing(t *testing.T) {
	host := &Host{
		ViewProcessedChunks: map[string][]*ProcessingChunk{
			"testView": {
				{StartBlock: 1, EndBlock: 100, MissingBlocks: []uint64{50}},
				{StartBlock: 101, EndBlock: 200, MissingBlocks: []uint64{150}},
			},
		},
	}

	host.mergeContiguousChunks("testView")

	chunks := host.ViewProcessedChunks["testView"]
	require.Len(t, chunks, 1)
	chunk := chunks[0]
	assert.Equal(t, uint64(1), chunk.StartBlock)
	assert.Equal(t, uint64(200), chunk.EndBlock)
	assert.Contains(t, chunk.MissingBlocks, uint64(50))
	assert.Contains(t, chunk.MissingBlocks, uint64(150))
}

func TestMergeContiguousChunks_DoNotMergeWhenExceedingMaxMissing(t *testing.T) {
	// Create chunks that would exceed maxMissingBlocksPerChunk when merged
	missing1 := make([]uint64, maxMissingBlocksPerChunk/2+1)
	for i := uint64(0); i < uint64(len(missing1)); i++ {
		missing1[i] = i + 1
	}
	missing2 := make([]uint64, maxMissingBlocksPerChunk/2+1)
	for i := uint64(0); i < uint64(len(missing2)); i++ {
		missing2[i] = i + 101
	}

	host := &Host{
		ViewProcessedChunks: map[string][]*ProcessingChunk{
			"testView": {
				{StartBlock: 1, EndBlock: 100, MissingBlocks: missing1},
				{StartBlock: 101, EndBlock: 200, MissingBlocks: missing2},
			},
		},
	}

	host.mergeContiguousChunks("testView")

	chunks := host.ViewProcessedChunks["testView"]
	// Should not merge because total missing would exceed maxMissingBlocksPerChunk
	require.Len(t, chunks, 2)
}

func TestMergeContiguousChunks_MergeChunksWithGap(t *testing.T) {
	host := &Host{
		ViewProcessedChunks: map[string][]*ProcessingChunk{
			"testView": {
				{StartBlock: 1, EndBlock: 100, MissingBlocks: []uint64{}},
				{StartBlock: 101, EndBlock: 200, MissingBlocks: []uint64{150}}, // Contiguous with 1 missing
			},
		},
	}

	host.mergeContiguousChunks("testView")

	chunks := host.ViewProcessedChunks["testView"]
	require.Len(t, chunks, 1)
	chunk := chunks[0]
	assert.Equal(t, uint64(1), chunk.StartBlock)
	assert.Equal(t, uint64(200), chunk.EndBlock)
	// Should have the missing block from the second chunk
	assert.Contains(t, chunk.MissingBlocks, uint64(150))
	assert.Len(t, chunk.MissingBlocks, 1)
}

func TestMergeContiguousChunks_NoChunks(t *testing.T) {
	host := &Host{
		ViewProcessedChunks: map[string][]*ProcessingChunk{
			"testView": {},
		},
	}

	host.mergeContiguousChunks("testView")

	chunks := host.ViewProcessedChunks["testView"]
	assert.Len(t, chunks, 0)
}

func TestMergeContiguousChunks_SingleChunk(t *testing.T) {
	host := &Host{
		ViewProcessedChunks: map[string][]*ProcessingChunk{
			"testView": {
				{StartBlock: 1, EndBlock: 100, MissingBlocks: []uint64{50}},
			},
		},
	}

	host.mergeContiguousChunks("testView")

	chunks := host.ViewProcessedChunks["testView"]
	require.Len(t, chunks, 1)
	assert.Equal(t, uint64(1), chunks[0].StartBlock)
	assert.Equal(t, uint64(100), chunks[0].EndBlock)
}

func TestMergeContiguousChunks_NonContiguousChunks(t *testing.T) {
	host := &Host{
		ViewProcessedChunks: map[string][]*ProcessingChunk{
			"testView": {
				{StartBlock: 1, EndBlock: 100, MissingBlocks: []uint64{}},
				{StartBlock: 201, EndBlock: 300, MissingBlocks: []uint64{}}, // Gap: 101-200 (not contiguous)
				{StartBlock: 301, EndBlock: 400, MissingBlocks: []uint64{}}, // Contiguous with previous
			},
		},
	}

	host.mergeContiguousChunks("testView")

	chunks := host.ViewProcessedChunks["testView"]
	// First chunk is separate, second and third should merge (they're contiguous)
	require.Len(t, chunks, 2)
	
	// First chunk should remain separate
	assert.Equal(t, uint64(1), chunks[0].StartBlock)
	assert.Equal(t, uint64(100), chunks[0].EndBlock)
	
	// Second and third should merge
	assert.Equal(t, uint64(201), chunks[1].StartBlock)
	assert.Equal(t, uint64(400), chunks[1].EndBlock)
}

func TestMergeContiguousChunks_MixedCompleteAndIncomplete(t *testing.T) {
	host := &Host{
		ViewProcessedChunks: map[string][]*ProcessingChunk{
			"testView": {
				{StartBlock: 1, EndBlock: 100, MissingBlocks: []uint64{}},      // Complete
				{StartBlock: 101, EndBlock: 200, MissingBlocks: []uint64{150}}, // Incomplete
				{StartBlock: 201, EndBlock: 300, MissingBlocks: []uint64{}},     // Complete
			},
		},
	}

	host.mergeContiguousChunks("testView")

	chunks := host.ViewProcessedChunks["testView"]
	// First two should merge (complete + incomplete with only 1 missing = 1 total)
	// Third should merge with the result (complete + incomplete with 1 missing = 1 total)
	require.Len(t, chunks, 1)
	chunk := chunks[0]
	assert.Equal(t, uint64(1), chunk.StartBlock)
	assert.Equal(t, uint64(300), chunk.EndBlock)
	assert.Contains(t, chunk.MissingBlocks, uint64(150))
	assert.Len(t, chunk.MissingBlocks, 1)
}

