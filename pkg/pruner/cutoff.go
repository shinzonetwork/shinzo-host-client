package pruner

import "sync/atomic"

// Cutoff is the block height at or below which the pruner deletes documents and the replication
// filter rejects them. Zero means no cutoff has been set.
type Cutoff struct {
	height atomic.Int64
}

// Load returns the cutoff height.
func (c *Cutoff) Load() int64 {
	return c.height.Load()
}

// Raise moves the cutoff up to height. A lower height is ignored, so a height the filter has
// rejected stays rejected.
func (c *Cutoff) Raise(height int64) {
	for {
		current := c.height.Load()
		if height <= current || c.height.CompareAndSwap(current, height) {
			return
		}
	}
}
