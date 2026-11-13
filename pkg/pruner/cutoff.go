package pruner

import "sync/atomic"

// Cutoff is the block height at or below which the pruner deletes documents and the retention rule
// refuses them. Zero means no cutoff has been set.
type Cutoff struct {
	height atomic.Int64
}

// Load returns the cutoff height.
func (c *Cutoff) Load() int64 {
	return c.height.Load()
}

// Raise moves the cutoff up to height. A lower height is ignored, so a height once refused stays
// refused.
func (c *Cutoff) Raise(height int64) {
	for {
		current := c.height.Load()
		if height <= current || c.height.CompareAndSwap(current, height) {
			return
		}
	}
}

// Floor is a tokens-mode chain's lowest height not yet verified, which its block verifier writes and
// its pruner reads. Zero means the verifier has not set it.
type Floor struct {
	height atomic.Int64
}

// Load returns the floor height.
func (f *Floor) Load() int64 {
	return f.height.Load()
}

// Raise moves the floor up to height. A lower height is ignored, since a verified height stays
// verified.
func (f *Floor) Raise(height int64) {
	for {
		current := f.height.Load()
		if height <= current || f.height.CompareAndSwap(current, height) {
			return
		}
	}
}
