package acp

import (
	"context"
	"errors"
	"testing"
	"time"
)

type stubHeightReader struct {
	height uint64
	err    error
	calls  int
}

func (s *stubHeightReader) LatestBlockNumber(context.Context) (uint64, error) {
	s.calls++
	return s.height, s.err
}

func TestEpochClockComputesEpoch(t *testing.T) {
	c := NewEpochClock(&stubHeightReader{height: 2500}, 1000, time.Minute)
	epoch, err := c.Epoch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if epoch != 2 {
		t.Errorf("epoch = %d, want 2 (2500/1000)", epoch)
	}
}

func TestEpochClockThrottlesHeightReads(t *testing.T) {
	h := &stubHeightReader{height: 2500}
	c := NewEpochClock(h, 1000, time.Minute)
	clock := time.Unix(1_000_000, 0)
	c.now = func() time.Time { return clock }

	for range 3 {
		if _, err := c.Epoch(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if h.calls != 1 {
		t.Errorf("expected 1 height read within the interval, got %d", h.calls)
	}
}

func TestEpochClockRefreshesAfterInterval(t *testing.T) {
	h := &stubHeightReader{height: 2500}
	c := NewEpochClock(h, 1000, time.Minute)
	clock := time.Unix(1_000_000, 0)
	c.now = func() time.Time { return clock }

	if _, err := c.Epoch(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(2 * time.Minute) // past the interval
	h.height = 3500
	epoch, err := c.Epoch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if epoch != 3 {
		t.Errorf("epoch = %d, want 3 after the refresh", epoch)
	}
	if h.calls != 2 {
		t.Errorf("expected a second height read after the interval, got %d", h.calls)
	}
}

func TestEpochClockFirstReadErrorPropagates(t *testing.T) {
	c := NewEpochClock(&stubHeightReader{err: errors.New("no height")}, 1000, time.Minute)
	if _, err := c.Epoch(context.Background()); err == nil {
		t.Fatal("expected the first height read error to propagate, got nil")
	}
}

func TestEpochClockKeepsLastEpochOnRefreshError(t *testing.T) {
	h := &stubHeightReader{height: 2500}
	c := NewEpochClock(h, 1000, time.Minute)
	clock := time.Unix(1_000_000, 0)
	c.now = func() time.Time { return clock }

	if _, err := c.Epoch(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(2 * time.Minute)
	h.err = errors.New("hub down")
	epoch, err := c.Epoch(context.Background())
	if err != nil {
		t.Fatalf("a refresh failure must not error once an epoch is known: %v", err)
	}
	if epoch != 2 {
		t.Errorf("epoch = %d, want the last known 2", epoch)
	}
}
