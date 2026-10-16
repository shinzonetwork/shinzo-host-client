package logger

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestNewDefaultsToStdout(t *testing.T) {
	log, sync, err := New(Config{})
	require.NoError(t, err)
	require.NotNil(t, log)
	require.NoError(t, sync())
}

func TestNewFileOutputIsPlainJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.log")

	log, sync, err := New(Config{
		Development: true,
		OutputPaths: []string{path},
		Fields:      map[string]any{"service": "shinzo-host"},
	})
	require.NoError(t, err)

	log.Sugar().Infow("hello", "k", "v")
	require.NoError(t, sync())

	data := readFile(t, path)
	require.NotContains(t, string(data), "\x1b", "file output should never contain an ANSI escape byte")

	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(firstNonEmptyLine(t, data)), &decoded))
	require.Equal(t, "hello", decoded["msg"])
	require.Equal(t, "shinzo-host", decoded["service"], "Fields should be attached to every line")
}

func TestNewLevelFiltersBelowConfigured(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.log")

	log, sync, err := New(Config{
		Level:       "warn",
		OutputPaths: []string{path},
	})
	require.NoError(t, err)

	log.Sugar().Info("should be filtered out")
	log.Sugar().Warn("should appear")
	require.NoError(t, sync())

	data := string(readFile(t, path))
	require.NotContains(t, data, "should be filtered out")
	require.Contains(t, data, "should appear")
}

func TestNewInvalidOutputPathErrors(t *testing.T) {
	_, _, err := New(Config{OutputPaths: []string{t.TempDir()}})
	require.Error(t, err)
}

func TestNewRejectsInvalidLevel(t *testing.T) {
	_, _, err := New(Config{Level: "not-a-level"})
	require.Error(t, err)
}

func TestIsRealTerminalFilePathsAreNeverTerminals(t *testing.T) {
	require.False(t, isRealTerminal(filepath.Join(t.TempDir(), "out.log")))
	require.False(t, isRealTerminal(""))
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
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseLevel(tc.level, tc.development)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestParseLevelRejectsGarbage(t *testing.T) {
	_, err := parseLevel("not-a-level", false)
	require.Error(t, err)
}

func TestIsBenignSyncError(t *testing.T) {
	for _, err := range []error{syscall.EINVAL, syscall.ENOTTY, syscall.EBADF} {
		require.True(t, isBenignSyncError(err), "expected %v to be treated as benign", err)
	}

	require.False(t, isBenignSyncError(errors.New("a real error")))
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path) //nolint:gosec // path is a t.TempDir() file this test just wrote itself
	require.NoError(t, err)
	return data
}

func firstNonEmptyLine(t *testing.T, data []byte) string {
	t.Helper()
	for line := range strings.SplitSeq(string(data), "\n") {
		if strings.TrimSpace(line) != "" {
			return line
		}
	}
	t.Fatalf("no non-empty line found in:\n%s", data)
	return ""
}
