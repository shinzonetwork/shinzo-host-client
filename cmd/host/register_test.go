package main

import "testing"

func TestRunRegisterNotImplemented(t *testing.T) {
	if err := runRegister(registerCmd, nil); err == nil {
		t.Error("expected a not implemented error got nil")
	}
}
