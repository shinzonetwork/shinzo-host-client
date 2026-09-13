package logger

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"go.uber.org/zap/zapcore"
)

func TestNew_DefaultsToStdout(t *testing.T) {
	log, sync, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if log == nil {
		t.Fatal("New returned a nil logger")
	}
	if err := sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func TestNew_FileOutputIsPlainJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.log")

	log, sync, err := New(Config{
		Development: true,
		OutputPaths: []string{path},
		Fields:      map[string]any{"service": "shinzo-host"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	log.Sugar().Infow("hello", "k", "v")
	if err := sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	data := readFile(t, path)

	if bytes.ContainsRune(data, 0x1b) {
		t.Fatalf("found an ANSI escape byte in file output:\n%s", data)
	}

	line := firstNonEmptyLine(t, data)
	var decoded map[string]any
	if err := json.Unmarshal([]byte(line), &decoded); err != nil {
		t.Fatalf("file output isn't valid JSON: %v\nline: %s", err, line)
	}
	if decoded["msg"] != "hello" {
		t.Fatalf("expected msg=hello, got %v", decoded["msg"])
	}
	if decoded["service"] != "shinzo-host" {
		t.Fatalf("expected Fields to be attached to every line, got %v", decoded["service"])
	}
}

func TestNew_LevelFiltersBelowConfigured(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.log")

	log, sync, err := New(Config{
		Level:       "warn",
		OutputPaths: []string{path},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	log.Sugar().Info("should be filtered out")
	log.Sugar().Warn("should appear")
	if err := sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	data := string(readFile(t, path))
	if strings.Contains(data, "should be filtered out") {
		t.Fatalf("info line got through at warn level:\n%s", data)
	}
	if !strings.Contains(data, "should appear") {
		t.Fatalf("warn line missing:\n%s", data)
	}
}

func TestNew_InvalidOutputPathErrors(t *testing.T) {

	_, _, err := New(Config{OutputPaths: []string{t.TempDir()}})
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
}

func TestParseLevel(t *testing.T) {
	cases := []struct {
		name        string
		level       string
		development bool
		want        zapcore.Level
	}{
		{"empty, production", "", false, zapcore.InfoLevel},
		{"empty, development", "", true, zapcore.DebugLevel},
		{"explicit level wins over development", "error", true, zapcore.ErrorLevel},
		{"garbage falls back to info", "not-a-level", false, zapcore.InfoLevel},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseLevel(tc.level, tc.development); got != tc.want {
				t.Fatalf("parseLevel(%q, %v) = %v, want %v", tc.level, tc.development, got, tc.want)
			}
		})
	}
}

func TestIsBenignSyncError(t *testing.T) {
	benign := []error{syscall.EINVAL, syscall.ENOTTY, syscall.EBADF}
	for _, err := range benign {
		if !isBenignSyncError(err) {
			t.Errorf("expected %v to be treated as benign", err)
		}
	}

	if isBenignSyncError(errors.New("a real error")) {
		t.Error("a plain error should not be treated as benign")
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path) //nolint:gosec
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return data
}

func firstNonEmptyLine(t *testing.T, data []byte) string {
	t.Helper()
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) != "" {
			return line
		}
	}
	t.Fatalf("no non-empty line found in:\n%s", data)
	return ""
}
