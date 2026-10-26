package logger

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestConsoleEncoderRendersLogfmtStyle(t *testing.T) {
	enc := newConsoleEncoder()

	entry := zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Time:    time.Date(2026, 1, 1, 16, 32, 0, 0, time.UTC),
		Message: "connected to bootstrap peer",
	}
	fields := []zapcore.Field{
		zap.String("peer", "12D3KooW..."),
		zap.Int("attempt", 1),
	}

	buf, err := enc.EncodeEntry(entry, fields)
	require.NoError(t, err)
	line := buf.String()

	require.Contains(t, line, "connected to bootstrap peer")
	require.Contains(t, line, "peer=12D3KooW...")
	require.Contains(t, line, "attempt=1")
	require.NotContains(t, line, "{")
	require.NotContains(t, line, "}")
	require.NotContains(t, strings.ToLower(line), "caller")
}

func TestConsoleEncoderIncludesCallerWhenDefined(t *testing.T) {
	enc := newConsoleEncoder()

	entry := zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Message: "hello",
		Caller: zapcore.EntryCaller{
			Defined: true,
			File:    "/src/pkg/staging/logger/logger.go",
			Line:    42,
		},
	}

	buf, err := enc.EncodeEntry(entry, nil)
	require.NoError(t, err)
	require.Contains(t, buf.String(), "logger.go:42")
}

func TestConsoleEncoderOmitsCallerWhenUndefined(t *testing.T) {
	enc := newConsoleEncoder()

	entry := zapcore.Entry{Level: zapcore.InfoLevel, Message: "hello"}

	buf, err := enc.EncodeEntry(entry, nil)
	require.NoError(t, err)
	require.NotContains(t, strings.ToLower(buf.String()), ".go:")
}

func TestConsoleEncoderIncludesStackWhenPresent(t *testing.T) {
	enc := newConsoleEncoder()

	entry := zapcore.Entry{
		Level:   zapcore.ErrorLevel,
		Message: "something broke",
		Stack:   "goroutine 1 [running]:\nmain.main()",
	}

	buf, err := enc.EncodeEntry(entry, nil)
	require.NoError(t, err)
	require.Contains(t, buf.String(), "goroutine 1 [running]:")
}

func TestConsoleEncoderOmitsStackWhenEmpty(t *testing.T) {
	enc := newConsoleEncoder()

	entry := zapcore.Entry{Level: zapcore.InfoLevel, Message: "hello"}

	buf, err := enc.EncodeEntry(entry, nil)
	require.NoError(t, err)
	require.NotContains(t, buf.String(), "goroutine")
}

func TestConsoleEncoderQuotesValuesWithSpaces(t *testing.T) {
	enc := newConsoleEncoder()

	entry := zapcore.Entry{Level: zapcore.WarnLevel, Message: "generated a new node seed"}
	fields := []zapcore.Field{
		zap.String("mnemonic", "one two three"),
	}

	buf, err := enc.EncodeEntry(entry, fields)
	require.NoError(t, err)
	require.Contains(t, buf.String(), `mnemonic="one two three"`)
}

func TestConsoleEncoderCloneIsIndependent(t *testing.T) {
	base := newConsoleEncoder()
	base.AddString("service", "shinzo-host")

	clone := base.Clone()
	clone.AddString("extra", "only-on-clone")

	entry := zapcore.Entry{Level: zapcore.InfoLevel, Message: "hello"}

	baseBuf, err := base.EncodeEntry(entry, nil)
	require.NoError(t, err)
	require.NotContains(t, baseBuf.String(), "extra=", "base encoder should be unaffected by the clone's fields")

	cloneBuf, err := clone.EncodeEntry(entry, nil)
	require.NoError(t, err)
	require.Contains(t, cloneBuf.String(), "extra=only-on-clone", "clone should carry its own fields")
}

func TestLevelTextAllLevelsRenderDistinctly(t *testing.T) {
	for _, noColor := range []bool{false, true} {
		seen := map[string]bool{}
		for _, lvl := range []zapcore.Level{zapcore.DebugLevel, zapcore.InfoLevel, zapcore.WarnLevel, zapcore.ErrorLevel} {
			text := levelText(lvl, noColor)
			require.False(t, seen[text], "noColor=%v: level %v produced a non-distinct rendering: %q", noColor, lvl, text)
			seen[text] = true
		}
	}
}

func TestLevelTextNoColorHasNoANSICodes(t *testing.T) {
	for _, lvl := range []zapcore.Level{zapcore.DebugLevel, zapcore.InfoLevel, zapcore.WarnLevel, zapcore.ErrorLevel} {
		require.NotContains(t, levelText(lvl, true), "\x1b")
	}
}

func TestLevelTextColorHasANSICodes(t *testing.T) {
	for _, lvl := range []zapcore.Level{zapcore.DebugLevel, zapcore.InfoLevel, zapcore.WarnLevel, zapcore.ErrorLevel} {
		require.Contains(t, levelText(lvl, false), "\x1b")
	}
}

func TestNewConsoleEncoderRespectsNoColorEnvVar(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	enc := newConsoleEncoder()

	buf, err := enc.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "hello"}, nil)
	require.NoError(t, err)
	require.NotContains(t, buf.String(), "\x1b")
}

func TestConsoleEncoderCloneCarriesNoColor(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	base := newConsoleEncoder()
	clone := base.Clone()

	buf, err := clone.EncodeEntry(zapcore.Entry{Level: zapcore.InfoLevel, Message: "hello"}, nil)
	require.NoError(t, err)
	require.NotContains(t, buf.String(), "\x1b", "clone should carry the base encoder's noColor setting")
}
