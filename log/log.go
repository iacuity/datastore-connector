package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

var (
	logger   *zap.Logger
	logLevel = zap.NewAtomicLevel()
)

type Level int8

const (
	// DebugLevel logs are typically voluminous, and are usually disabled in  production.
	DebugLevel Level = iota
	// InfoLevel is the default logging priority.
	InfoLevel
	// WarnLevel logs are more important than Info, but don't need individual human review.
	WarnLevel
	// ErrorLevel logs are high-priority. If an application is running smoothly,
	// it shouldn't generate any error-level logs.
	ErrorLevel
	// PanicLevel logs a message, then panics.
	PanicLevel
	// FatalLevel logs a message, then calls os.Exit(1).
	FatalLevel
)

func Init(logFileName string) {
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.TimeEncoderOfLayout("2006/01/02 15:04:05.000")
	fileEncoder := zapcore.NewJSONEncoder(config)

	writer := zapcore.AddSync(
		zapcore.AddSync(&lumberjack.Logger{
			Filename:   logFileName,
			MaxSize:    1024, // megabytes
			MaxBackups: 5,
			MaxAge:     30, // days
			Compress:   true,
		}),
	)

	logLevel.SetLevel(zap.DebugLevel)

	options := []zap.Option{
		zap.AddCaller(),
		zap.AddStacktrace(zap.FatalLevel),
	}

	core := zapcore.NewTee(
		zapcore.NewCore(fileEncoder, writer, logLevel),
	)

	logger = zap.New(core, options...)
}

func Log() *zap.Logger {
	return logger
}

func SetLogLevel(l Level) {
	if l >= DebugLevel && l <= FatalLevel {
		logLevel.SetLevel((zapcore.Level)(l - 1))
	}
}

// Applications should take care to call Sync before exiting.
func Sync() {
	Log().Sync()
}
