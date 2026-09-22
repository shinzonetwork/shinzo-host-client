package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunInitNotImplemented(t *testing.T) {
	require.ErrorIs(t, runInit(initCmd(), nil), errNotImplemented)
}
