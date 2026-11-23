package pruner

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCutoffRaise(t *testing.T) {
	var c Cutoff
	c.Raise(100)
	c.Raise(50)
	require.Equal(t, int64(100), c.Load(), "a lower height must not lower the cutoff")

	c.Raise(150)
	require.Equal(t, int64(150), c.Load())
}

func TestFloorRaise(t *testing.T) {
	var f Floor
	f.Raise(100)
	f.Raise(50)
	require.Equal(t, int64(100), f.Load(), "a lower height must not lower the floor")

	f.Raise(150)
	require.Equal(t, int64(150), f.Load())
}

// Raises from many goroutines at once never leave the floor below a height raised, and end at the
// highest one. Each goroutine raises its own rising run of heights, so the raises interleave
// throughout.
func TestFloorRaiseConcurrently(t *testing.T) {
	const raises = 1_000_000
	goroutines := runtime.GOMAXPROCS(0)
	var f Floor
	var lowered atomic.Bool
	var raisers sync.WaitGroup
	for g := range goroutines {
		raisers.Go(func() {
			for h := int64(g + 1); h <= raises; h += int64(goroutines) {
				f.Raise(h)
				if f.Load() < h {
					lowered.Store(true)
				}
			}
		})
	}
	raisers.Wait()

	require.False(t, lowered.Load(), "the floor went below a height already raised")
	require.Equal(t, int64(raises), f.Load())
}
