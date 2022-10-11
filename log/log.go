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

func SetLogLevel(l int8) {
	if (zapcore.Level)(l) >= zapcore.DebugLevel && (zapcore.Level)(l) <= zapcore.FatalLevel {
		logLevel.SetLevel((zapcore.Level)(l))
	}
}

// Applications should take care to call Sync before exiting.
func Sync() {
	Log().Sync()
}
