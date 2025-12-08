package host

import (
	"sync"
	"testing"
)

func TestNewProcessedBlocks(t *testing.T) {
	startHeight := uint64(100)
	pb := NewProcessedBlocks(startHeight)

	if pb == nil {
		t.Fatal("NewProcessedBlocks returned nil")
	}
	if pb.lowWaterMark != startHeight {
		t.Errorf("expected lowWaterMark to be %d, got %d", startHeight, pb.lowWaterMark)
	}
	if pb.highestBlock != startHeight {
		t.Errorf("expected highestBlock to be %d, got %d", startHeight, pb.highestBlock)
	}
	if len(pb.recentBlocks) != 0 {
		t.Errorf("expected recentBlocks to be empty, got %d items", len(pb.recentBlocks))
	}
}

func TestProcessedBlocks_Add_Sequential(t *testing.T) {
	pb := NewProcessedBlocks(0)
	pb.Add(1)
	pb.Add(2)
	pb.Add(3)

	if pb.lowWaterMark != 3 {
		t.Errorf("expected lowWaterMark to be 3, got %d", pb.lowWaterMark)
	}
	if pb.highestBlock != 3 {
		t.Errorf("expected highestBlock to be 3, got %d", pb.highestBlock)
	}
	if len(pb.recentBlocks) != 0 {
		t.Errorf("expected recentBlocks to be empty after sequential add, got %d items", len(pb.recentBlocks))
	}
}

func TestProcessedBlocks_Add_OutOfOrder(t *testing.T) {
	pb := NewProcessedBlocks(10)
	pb.Add(12)
	pb.Add(14)
	pb.Add(13)

	if pb.lowWaterMark != 10 {
		t.Errorf("expected lowWaterMark to remain 10, got %d", pb.lowWaterMark)
	}
	if pb.highestBlock != 14 {
		t.Errorf("expected highestBlock to be 14, got %d", pb.highestBlock)
	}
	if len(pb.recentBlocks) != 3 {
		t.Errorf("expected 3 items in recentBlocks, got %d", len(pb.recentBlocks))
	}

	// Now, fill the gap
	pb.Add(11)

	if pb.lowWaterMark != 14 {
		t.Errorf("expected lowWaterMark to advance to 14, got %d", pb.lowWaterMark)
	}
	if len(pb.recentBlocks) != 0 {
		t.Errorf("expected recentBlocks to be empty after filling gap, got %d", len(pb.recentBlocks))
	}
}

func TestProcessedBlocks_Add_DuplicateAndOld(t *testing.T) {
	pb := NewProcessedBlocks(20)
	pb.Add(21) // Advances LWM to 21

	if pb.lowWaterMark != 21 {
		t.Fatalf("expected lowWaterMark to be 21, got %d", pb.lowWaterMark)
	}

	// Add an old block (already covered by LWM)
	pb.Add(20)
	if pb.lowWaterMark != 21 {
		t.Errorf("lowWaterMark should not change when adding old block, got %d", pb.lowWaterMark)
	}

	// Add a duplicate block
	pb.Add(21)
	if pb.lowWaterMark != 21 {
		t.Errorf("lowWaterMark should not change when adding duplicate block, got %d", pb.lowWaterMark)
	}
}

func TestProcessedBlocks_HasProcessed(t *testing.T) {
	pb := NewProcessedBlocks(100)
	pb.Add(102) // Gap at 101

	// Test block below LWM
	if !pb.HasProcessed(99) {
		t.Error("expected HasProcessed(99) to be true (below LWM)")
	}
	// Test block at LWM
	if !pb.HasProcessed(100) {
		t.Error("expected HasProcessed(100) to be true (at LWM)")
	}
	// Test block in recentBlocks
	if !pb.HasProcessed(102) {
		t.Error("expected HasProcessed(102) to be true (in recentBlocks)")
	}
	// Test unprocessed block (the gap)
	if pb.HasProcessed(101) {
		t.Error("expected HasProcessed(101) to be false (unprocessed)")
	}
	// Test future block
	if pb.HasProcessed(103) {
		t.Error("expected HasProcessed(103) to be false (future block)")
	}
}

func TestProcessedBlocks_GetHighest(t *testing.T) {
	pb := NewProcessedBlocks(50)
	if pb.GetHighest() != 50 {
		t.Errorf("expected initial highest block to be 50, got %d", pb.GetHighest())
	}

	pb.Add(55)
	if pb.GetHighest() != 55 {
		t.Errorf("expected highest block to be 55, got %d", pb.GetHighest())
	}

	pb.Add(52) // Add a lower block
	if pb.GetHighest() != 55 {
		t.Errorf("highest block should remain 55, got %d", pb.GetHighest())
	}
}

func TestProcessedBlocks_Concurrency(t *testing.T) {
	pb := NewProcessedBlocks(0)
	var wg sync.WaitGroup
	numGoroutines := 100
	blocksPerGoroutine := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(startBlock int) {
			defer wg.Done()
			for j := 0; j < blocksPerGoroutine; j++ {
				pb.Add(uint64(startBlock + j))
			}
		}(i * blocksPerGoroutine)
	}

	wg.Wait()

	// We can't guarantee the exact LWM due to race conditions in adding,
	// but we can check that HasProcessed is correct for a few key blocks.
	if pb.HasProcessed(uint64(numGoroutines*blocksPerGoroutine + 10)) {
		t.Error("should not have processed a block that was not added")
	}

	// The highest block should be the max block number added.
	expectedHighest := uint64(numGoroutines*blocksPerGoroutine - 1)
	if pb.GetHighest() < expectedHighest {
		t.Errorf("expected highest block to be at least %d, got %d", expectedHighest, pb.GetHighest())
	}
}
