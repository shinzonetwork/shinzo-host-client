package logger

import (
	"fmt"
	"sort"
	"strings"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var consoleBufferPool = buffer.NewPool() //nolint:gochecknoglobals

const timeFormat = "3:04PM"

func newConsoleEncoder() zapcore.Encoder {
	return &consoleEncoder{MapObjectEncoder: zapcore.NewMapObjectEncoder()}
}

type consoleEncoder struct {
	*zapcore.MapObjectEncoder
}

func (e *consoleEncoder) Clone() zapcore.Encoder {
	clone := zapcore.NewMapObjectEncoder()
	for k, v := range e.Fields {
		clone.Fields[k] = v
	}
	return &consoleEncoder{MapObjectEncoder: clone}
}

func (e *consoleEncoder) EncodeEntry(entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	line := consoleBufferPool.Get()

	line.AppendString(entry.Time.Format(timeFormat))
	line.AppendString(" ")
	line.AppendString(levelText(entry.Level))
	line.AppendString(" ")
	line.AppendString(entry.Message)

	merged := zapcore.NewMapObjectEncoder()
	for k, v := range e.Fields {
		merged.Fields[k] = v
	}
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

func levelText(level zapcore.Level) string {
	switch level {
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
