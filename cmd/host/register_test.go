package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunRegisterNotImplemented(t *testing.T) {
	require.ErrorIs(t, runRegister(registerCmd(), nil), errNotImplemented)
}
