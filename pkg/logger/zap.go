package logger

import (
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Sugar is the package-level sugared logger. Call Init before first use; until
// then it is nil and call sites will panic on dereference.
var Sugar *zap.SugaredLogger

// Init builds a tee'd zap logger that writes to stdout and, when logsDir is
// writable, also to logsDir/logfile.log (all levels) and logsDir/errorfile.log
// (error and above). Falls back to stdout-only when the directory or files
// cannot be opened.
func Init(development bool, logsDir string) {
	var zapLevel zapcore.Level
	if development {
		zapLevel = zap.DebugLevel
	} else {
		zapLevel = zap.InfoLevel
	}

	encoderConfig := zap.NewDevelopmentEncoderConfig()
	encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder

	consoleWriter := zapcore.Lock(os.Stdout)

	var cores []zapcore.Core

	if err := os.MkdirAll(logsDir, 0755); err == nil {
		logFile := filepath.Join(logsDir, "logfile.log")
		errorFile := filepath.Join(logsDir, "errorfile.log")

		if logFileWriter, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err == nil {
			consoleCore := zapcore.NewCore(zapcore.NewConsoleEncoder(encoderConfig), consoleWriter, zapLevel)
			cores = append(cores, consoleCore)

			logFileCore := zapcore.NewCore(zapcore.NewConsoleEncoder(encoderConfig), zapcore.AddSync(logFileWriter), zapLevel)
			cores = append(cores, logFileCore)

			if errorFileWriter, err := os.OpenFile(errorFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err == nil {
				errorCore := zapcore.NewCore(
					zapcore.NewConsoleEncoder(encoderConfig),
					zapcore.AddSync(errorFileWriter),
					zapcore.ErrorLevel,
				)
				cores = append(cores, errorCore)
			}
		} else {
			consoleCore := zapcore.NewCore(zapcore.NewConsoleEncoder(encoderConfig), consoleWriter, zapLevel)
			cores = append(cores, consoleCore)
		}
	} else {
		consoleCore := zapcore.NewCore(zapcore.NewConsoleEncoder(encoderConfig), consoleWriter, zapLevel)
		cores = append(cores, consoleCore)
	}

	core := zapcore.NewTee(cores...)
	Sugar = zap.New(core).Sugar()
}
