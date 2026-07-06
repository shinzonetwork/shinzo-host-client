package acp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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

// blockingHeightReader blocks in LatestBlockNumber until release is closed, after
// freePasses immediate reads. It counts every call so a test can assert reads are
// coalesced, and signals entered (non-blocking) when a read starts blocking.
type blockingHeightReader struct {
	height     uint64
	freePasses int
	calls      atomic.Int64
	entered    chan struct{}
	release    chan struct{}
}

func (b *blockingHeightReader) LatestBlockNumber(context.Context) (uint64, error) {
	if int(b.calls.Add(1)) <= b.freePasses {
		return b.height, nil
	}
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.release
	return b.height, nil
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

// TestEpochClockCoalescesConcurrentReads checks a burst of concurrent refreshes
// collapses into a single hub read rather than one per caller.
func TestEpochClockCoalescesConcurrentReads(t *testing.T) {
	h := &blockingHeightReader{height: 5000, entered: make(chan struct{}, 1), release: make(chan struct{})}
	c := NewEpochClock(h, 1000, time.Minute)

	const n = 8
	var wg sync.WaitGroup
	epochs := make([]uint64, n)
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() {
			epochs[i], errs[i] = c.Epoch(context.Background())
		})
	}

	<-h.entered                        // one read is in flight
	time.Sleep(100 * time.Millisecond) // let the rest reach Do and coalesce onto it
	close(h.release)
	wg.Wait()

	if got := h.calls.Load(); got != 1 {
		t.Errorf("expected 1 coalesced height read, got %d", got)
	}
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: %v", i, errs[i])
		}
		if epochs[i] != 5 {
			t.Errorf("goroutine %d: epoch = %d, want 5", i, epochs[i])
		}
	}
}

// TestEpochClockRefreshDoesNotHoldLock checks that while a refresh is blocked in the
// hub read, a cached-and-fresh call still returns at once: the lock is not held
// across the read, so a slow hub does not stall every query.
func TestEpochClockRefreshDoesNotHoldLock(t *testing.T) {
	h := &blockingHeightReader{height: 6000, freePasses: 1, entered: make(chan struct{}, 1), release: make(chan struct{})}
	c := NewEpochClock(h, 1000, time.Minute)

	var nowNanos atomic.Int64
	base := time.Unix(1_000_000, 0)
	nowNanos.Store(base.UnixNano())
	c.now = func() time.Time { return time.Unix(0, nowNanos.Load()) }

	// Prime the cache with the one immediate read.
	if _, err := c.Epoch(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Past the interval: the next call refreshes and blocks in the hub read.
	nowNanos.Store(base.Add(2 * time.Minute).UnixNano())
	refreshing := make(chan struct{})
	go func() {
		_, _ = c.Epoch(context.Background())
		close(refreshing)
	}()
	<-h.entered

	// Back within the interval: this call must hit the cache and return immediately.
	nowNanos.Store(base.UnixNano())
	got := make(chan uint64, 1)
	go func() {
		epoch, _ := c.Epoch(context.Background())
		got <- epoch
	}()
	select {
	case epoch := <-got:
		if epoch != 6 {
			t.Errorf("cached epoch = %d, want 6", epoch)
		}
	case <-time.After(time.Second):
		t.Fatal("a cached call blocked while a refresh was in flight; the lock is held across the read")
	}

	close(h.release)
	<-refreshing
}
