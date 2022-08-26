package cache

import (
	"os"

	"go.uber.org/zap"
)

var (
	Logger    *zap.Logger
	nopLogger = zap.NewNop()
)

func logger() *zap.Logger {
	if Logger == nil || os.Getenv("AIVA_CACHE_DEBUG") == "" {
		Logger = nopLogger
	}
	return Logger
}
