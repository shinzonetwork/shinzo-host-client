package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunVersionPrintsVersion(t *testing.T) {
	var buf bytes.Buffer
	versionCmd.SetOut(&buf)

	if err := runVersion(versionCmd, nil); err != nil {
		t.Fatalf("runVersion: %v", err)
	}

	if !strings.Contains(buf.String(), "go1.") {
		t.Errorf("output %q missing go version", buf.String())
	}
}
