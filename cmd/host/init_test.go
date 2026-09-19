package main

import "testing"

func TestRunInitNotImplemented(t *testing.T) {
	if err := runInit(initCmd, nil); err == nil {
		t.Error("expected not implemented error got nil")
	}
}
