package logger

import (
	"fmt"
	"maps"
	"os"
	"sort"
	"strings"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var consoleBufferPool = buffer.NewPool() //nolint:gochecknoglobals // zap's own pattern: a package-level sync.Pool for encoder buffers.

const consoleTimeFormat = "3:04PM"

func newConsoleEncoder() zapcore.Encoder {
	return &consoleEncoder{
		MapObjectEncoder: zapcore.NewMapObjectEncoder(),
		// Checked once at construction, not per entry: https://no-color.org,
		// a widely respected convention for operators who want plain text
		// (piping into a tool that doesn't strip ANSI codes, accessibility,
		// etc.) even on a real terminal.
		noColor: os.Getenv("NO_COLOR") != "",
	}
}

// consoleEncoder renders one short, human-readable line per entry:
// time, level, message, then any fields as key=value pairs.
type consoleEncoder struct {
	*zapcore.MapObjectEncoder

	noColor bool
}

func (e *consoleEncoder) Clone() zapcore.Encoder {
	clone := zapcore.NewMapObjectEncoder()
	maps.Copy(clone.Fields, e.Fields)
	return &consoleEncoder{MapObjectEncoder: clone, noColor: e.noColor}
}

func (e *consoleEncoder) EncodeEntry(entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	line := consoleBufferPool.Get()

	line.AppendString(entry.Time.Format(consoleTimeFormat))
	line.AppendString(" ")
	line.AppendString(levelText(entry.Level, e.noColor))
	line.AppendString(" ")
	line.AppendString(entry.Message)

	merged := zapcore.NewMapObjectEncoder()
	maps.Copy(merged.Fields, e.Fields)
	for _, f := range fields {
		f.AddTo(merged)
	}

	keys := make([]string, 0, len(merged.Fields))
	for k := range merged.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		line.AppendString(" ")
		line.AppendString(k)
		line.AppendString("=")
		line.AppendString(formatValue(merged.Fields[k]))
	}

	line.AppendString("\n")
	return line, nil
}

func formatValue(v any) string {
	s := fmt.Sprintf("%v", v)
	if strings.ContainsAny(s, " \t\n") {
		return `"` + s + `"`
	}
	return s
}

func levelText(level zapcore.Level, noColor bool) string {
	if noColor {
		return plainLevelText(level)
	}

	switch level { //nolint:exhaustive // default covers everything at or above error (DPanic, Panic, Fatal) the same way.
	case zapcore.DebugLevel:
		return "\x1b[90mDBG\x1b[0m"
	case zapcore.InfoLevel:
		return "\x1b[32mINF\x1b[0m"
	case zapcore.WarnLevel:
		return "\x1b[33mWRN\x1b[0m"
	case zapcore.ErrorLevel:
		return "\x1b[31mERR\x1b[0m"
	default:
		return "\x1b[31mFTL\x1b[0m"
	}
}

func plainLevelText(level zapcore.Level) string {
	switch level { //nolint:exhaustive // default covers everything at or above error (DPanic, Panic, Fatal) the same way.
	case zapcore.DebugLevel:
		return "DBG"
	case zapcore.InfoLevel:
		return "INF"
	case zapcore.WarnLevel:
		return "WRN"
	case zapcore.ErrorLevel:
		return "ERR"
	default:
		return "FTL"
	}
}
