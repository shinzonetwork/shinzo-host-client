package logger

import (
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestConsoleEncoder_RendersLogfmtStyle(t *testing.T) {
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
	if err != nil {
		t.Fatalf("EncodeEntry: %v", err)
	}
	line := buf.String()

	if !strings.Contains(line, "connected to bootstrap peer") {
		t.Fatalf("expected the message in the output, got: %s", line)
	}
	if !strings.Contains(line, "peer=12D3KooW...") {
		t.Fatalf("expected peer=... in the output, got: %s", line)
	}
	if !strings.Contains(line, "attempt=1") {
		t.Fatalf("expected attempt=1 in the output, got: %s", line)
	}
	if strings.Contains(line, "{") || strings.Contains(line, "}") {
		t.Fatalf("expected no JSON braces in console output, got: %s", line)
	}
	if strings.Contains(strings.ToLower(line), "caller") {
		t.Fatalf("expected no caller info in console output, got: %s", line)
	}
}

func TestConsoleEncoder_QuotesValuesWithSpaces(t *testing.T) {
	enc := newConsoleEncoder()

	entry := zapcore.Entry{Level: zapcore.WarnLevel, Message: "generated a new node seed"}
	fields := []zapcore.Field{
		zap.String("mnemonic", "one two three"),
	}

	buf, err := enc.EncodeEntry(entry, fields)
	if err != nil {
		t.Fatalf("EncodeEntry: %v", err)
	}
	line := buf.String()

	if !strings.Contains(line, `mnemonic="one two three"`) {
		t.Fatalf("expected a quoted multi-word value, got: %s", line)
	}
}

func TestConsoleEncoder_CloneIsIndependent(t *testing.T) {
	base := newConsoleEncoder()
	base.AddString("service", "shinzo-host")

	clone := base.Clone()
	clone.AddString("extra", "only-on-clone")

	entry := zapcore.Entry{Level: zapcore.InfoLevel, Message: "hello"}

	baseBuf, err := base.EncodeEntry(entry, nil)
	if err != nil {
		t.Fatalf("EncodeEntry (base): %v", err)
	}
	if strings.Contains(baseBuf.String(), "extra=") {
		t.Fatalf("expected the base encoder to be unaffected by the clone's fields, got: %s", baseBuf.String())
	}

	cloneBuf, err := clone.EncodeEntry(entry, nil)
	if err != nil {
		t.Fatalf("EncodeEntry (clone): %v", err)
	}
	if !strings.Contains(cloneBuf.String(), "extra=only-on-clone") {
		t.Fatalf("expected the clone to carry its own fields, got: %s", cloneBuf.String())
	}
}

func TestLevelText_AllLevelsRenderDistinctly(t *testing.T) {
	seen := map[string]bool{}
	for _, lvl := range []zapcore.Level{zapcore.DebugLevel, zapcore.InfoLevel, zapcore.WarnLevel, zapcore.ErrorLevel} {
		text := levelText(lvl)
		if seen[text] {
			t.Fatalf("level %v produced a non-distinct rendering: %q", lvl, text)
		}
		seen[text] = true
	}
}
