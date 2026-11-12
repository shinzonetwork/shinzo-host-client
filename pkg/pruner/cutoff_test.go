package pruner

import (
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
