package defradb

import (
	"context"
	"errors"
	"testing"

	"github.com/sourcenetwork/defradb/node"
	"github.com/stretchr/testify/require"
)

// errorApplier fails every schema apply with err.
type errorApplier struct{ err error }

func (a errorApplier) ApplySchema(context.Context, *node.Node) error { return a.err }

// Any schema apply error stops the start, including DefraDB's error for a collection that already
// exists.
func TestStartDefraInstance_SchemaApplyError(t *testing.T) {
	applyErr := errors.New("collection already exists")
	cfg := *DefaultConfig

	_, err := StartDefraInstanceWithTestConfig(t, &cfg, errorApplier{err: applyErr})
	require.ErrorIs(t, err, applyErr)
}
