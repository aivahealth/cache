package cache

import "go.uber.org/zap"

var (
	Logger *zap.Logger
)

func logger() *zap.Logger {
	if Logger == nil {
		Logger = zap.NewNop()
	}
	return Logger
}
