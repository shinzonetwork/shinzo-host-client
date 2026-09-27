package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunStartNotImplemented(t *testing.T) {
	require.ErrorIs(t, runStart(startCmd(), nil), errNotImplemented)
}
