package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunVersionPrintsVersion(t *testing.T) {
	cmd := versionCmd()

	var buf bytes.Buffer
	cmd.SetOut(&buf)

	if err := runVersion(cmd, nil); err != nil {
		t.Fatalf("runVersion: %v", err)
	}

	if !strings.Contains(buf.String(), "go1.") {
		t.Errorf("output %q missing go version", buf.String())
	}
}
