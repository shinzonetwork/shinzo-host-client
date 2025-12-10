package host

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlockRangeTracker_Add(t *testing.T) {
	tests := []struct {
		name           string
		initialRanges  []BlockRange
		additions      []uint64
		expectedRanges []BlockRange
	}{
		{
			name:           "Add to empty tracker",
			initialRanges:  []BlockRange{},
			additions:      []uint64{10},
			expectedRanges: []BlockRange{{Start: 10, End: 10}},
		},
		{
			name:           "Add duplicate block",
			initialRanges:  []BlockRange{{Start: 10, End: 10}},
			additions:      []uint64{10},
			expectedRanges: []BlockRange{{Start: 10, End: 10}},
		},
		{
			name:           "Extend range at the end",
			initialRanges:  []BlockRange{{Start: 10, End: 15}},
			additions:      []uint64{16},
			expectedRanges: []BlockRange{{Start: 10, End: 16}},
		},
		{
			name:           "Extend range at the beginning",
			initialRanges:  []BlockRange{{Start: 10, End: 15}},
			additions:      []uint64{9},
			expectedRanges: []BlockRange{{Start: 9, End: 15}},
		},
		{
			name:           "Insert new range between existing ranges",
			initialRanges:  []BlockRange{{Start: 10, End: 15}, {Start: 20, End: 25}},
			additions:      []uint64{17},
			expectedRanges: []BlockRange{{Start: 10, End: 15}, {Start: 17, End: 17}, {Start: 20, End: 25}},
		},
		{
			name:           "Merge two ranges by filling the gap",
			initialRanges:  []BlockRange{{Start: 10, End: 15}, {Start: 17, End: 20}},
			additions:      []uint64{16},
			expectedRanges: []BlockRange{{Start: 10, End: 20}},
		},
		{
			name:           "Complex sequence - out of order",
			initialRanges:  []BlockRange{},
			additions:      []uint64{10, 12, 15, 11, 14, 13},
			expectedRanges: []BlockRange{{Start: 10, End: 15}},
		},
		{
			name:           "Complex sequence - merging multiple ranges",
			initialRanges:  []BlockRange{{Start: 10, End: 10}, {Start: 12, End: 12}, {Start: 14, End: 14}},
			additions:      []uint64{11, 13},
			expectedRanges: []BlockRange{{Start: 10, End: 14}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := NewBlockRangeTracker()
			tracker.ranges = tt.initialRanges
			for _, num := range tt.additions {
				tracker.Add(num)
			}
			require.Equal(t, tt.expectedRanges, tracker.ranges)
		})
	}
}

func TestBlockRangeTracker_GetUnprocessedRanges(t *testing.T) {
	tests := []struct {
		name             string
		ranges           []BlockRange
		latestKnownBlock uint64
		expectedGaps     []BlockRange
	}{
		{
			name:             "No ranges, no new blocks",
			ranges:           []BlockRange{},
			latestKnownBlock: 0,
			expectedGaps:     nil,
		},
		{
			name:             "No ranges, new blocks available",
			ranges:           []BlockRange{},
			latestKnownBlock: 10,
			expectedGaps:     []BlockRange{{Start: 1, End: 10}},
		},
		{
			name:             "Single range, no gaps",
			ranges:           []BlockRange{{Start: 1, End: 10}},
			latestKnownBlock: 10,
			expectedGaps:     nil,
		},
		{
			name:             "Single range, gap at the end",
			ranges:           []BlockRange{{Start: 1, End: 10}},
			latestKnownBlock: 15,
			expectedGaps:     []BlockRange{{Start: 11, End: 15}},
		},
		{
			name:             "Multiple ranges with gaps in between and at the end",
			ranges:           []BlockRange{{Start: 1, End: 10}, {Start: 15, End: 20}},
			latestKnownBlock: 25,
			expectedGaps:     []BlockRange{{Start: 11, End: 14}, {Start: 21, End: 25}},
		},
		{
			name:             "Gap at the beginning",
			ranges:           []BlockRange{{Start: 10, End: 20}},
			latestKnownBlock: 20,
			expectedGaps:     []BlockRange{{Start: 1, End: 9}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := NewBlockRangeTracker()
			tracker.ranges = tt.ranges
			gaps := tracker.GetUnprocessedRanges(tt.latestKnownBlock)
			if !reflect.DeepEqual(tt.expectedGaps, gaps) {
				t.Errorf("expected gaps %v, but got %v", tt.expectedGaps, gaps)
			}
		})
	}
}
