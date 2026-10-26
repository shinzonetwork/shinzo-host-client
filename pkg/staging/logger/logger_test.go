package logger

import (
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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

// trackingSink is a zap.Sink that records whether Close was called, so
// a test can tell whether an already-opened output actually got closed.
type trackingSink struct {
	closed *bool
}

func (s *trackingSink) Write(p []byte) (int, error) { return len(p), nil }
func (s *trackingSink) Sync() error                 { return nil }
func (s *trackingSink) Close() error {
	*s.closed = true
	return nil
}

var (
	trackingSchemesOnce sync.Once //nolint:gochecknoglobals // zap.RegisterSink errors on re-registration; must happen exactly once per process.
	trackingOKClosed    bool      //nolint:gochecknoglobals // written by the registered "ok" sink's Close, read back by the test.
)

// registerTrackingSchemes wires up two fake schemes once per process:
// "ok", which opens a trackingSink writing into trackingOKClosed, and
// "bad", which always fails to open.
func registerTrackingSchemes(t *testing.T) {
	t.Helper()
	trackingSchemesOnce.Do(func() {
		require.NoError(t, zap.RegisterSink("ok", func(*url.URL) (zap.Sink, error) {
			return &trackingSink{closed: &trackingOKClosed}, nil
		}))
		require.NoError(t, zap.RegisterSink("bad", func(*url.URL) (zap.Sink, error) {
			return nil, errors.New("fake open failure")
		}))
	})
}

// New must close every output it already opened if a later one fails to
// open, instead of returning immediately and leaking the earlier ones.
func TestNewClosesAlreadyOpenedOutputsWhenALaterOneFails(t *testing.T) {
	registerTrackingSchemes(t)
	trackingOKClosed = false

	_, _, err := New(Config{OutputPaths: []string{"ok://first", "bad://second"}})
	require.Error(t, err)
	require.True(t, trackingOKClosed, "the already-opened \"ok\" output should have been closed")
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
