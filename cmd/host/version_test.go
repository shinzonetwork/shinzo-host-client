package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunVersionPrintsVersion(t *testing.T) {
	cmd := versionCmd()

	var buf bytes.Buffer
	cmd.SetOut(&buf)

	require.NoError(t, runVersion(cmd, nil))
	require.Contains(t, buf.String(), version)
}
