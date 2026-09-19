package main

import "testing"

func TestRunStartNotImplemented(t *testing.T) {
	if err := runStart(startCmd, nil); err == nil {
		t.Error("expected a not implemented error got nil")
	}
}
