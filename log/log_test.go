package log

import (
	"testing"
)

func TestBasic(t *testing.T) {
	Init("log.json")
	Log().Debug("This is debug message")
	Log().Info("This is info message")
	Log().Error("This is error message")
	Log().Sync()
}
