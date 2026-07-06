package acp

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// HeightReader returns the hub's latest block number.
type HeightReader interface {
	LatestBlockNumber(ctx context.Context) (uint64, error)
}

// EpochSource reports the current settlement epoch. EpochClock is the production
// implementation.
type EpochSource interface {
	Epoch(ctx context.Context) (uint64, error)
}

// EpochClock derives the current settlement epoch as latestBlock / epochLength.
// It re-reads the height lazily and at most once per interval, so a burst of
// queries triggers a single height read. A refresh failure returns the last
// known epoch.
type EpochClock struct {
	height      HeightReader
	epochLength uint64
	interval    time.Duration
	now         func() time.Time

	group singleflight.Group

	mu        sync.Mutex
	epoch     uint64
	haveEpoch bool
	lastFetch time.Time
}

// NewEpochClock returns a clock that derives the epoch from the hub height, with
// epochLength blocks per epoch and at most one height read per interval.
func NewEpochClock(height HeightReader, epochLength uint64, interval time.Duration) *EpochClock {
	return &EpochClock{
		height:      height,
		epochLength: epochLength,
		interval:    interval,
		now:         time.Now,
	}
}

// Epoch returns the current epoch number, re-reading the height only after the
// interval has elapsed. Concurrent refreshes coalesce into a single hub read. The
// first read must reach the hub, so its error is returned; a later refresh failure
// returns the last known epoch.
func (c *EpochClock) Epoch(ctx context.Context) (uint64, error) {
	c.mu.Lock()
	if c.haveEpoch && c.now().Sub(c.lastFetch) < c.interval {
		epoch := c.epoch
		c.mu.Unlock()
		return epoch, nil
	}
	c.mu.Unlock()

	epoch, err, _ := c.group.Do("epoch", func() (any, error) {
		return c.refresh(ctx)
	})
	if err != nil {
		return 0, err
	}
	return epoch.(uint64), nil
}

// refresh reads the hub height and commits the derived epoch under the lock. A
// read error keeps the last known epoch, unless none is known yet.
func (c *EpochClock) refresh(ctx context.Context) (uint64, error) {
	block, err := c.height.LatestBlockNumber(ctx)

	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		if c.haveEpoch {
			return c.epoch, nil
		}
		return 0, err
	}
	c.epoch = block / c.epochLength
	c.haveEpoch = true
	c.lastFetch = c.now()
	return c.epoch, nil
}
