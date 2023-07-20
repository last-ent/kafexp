package kafexp

import (
	"go.uber.org/zap"
	"log"
)

// GetLogger returns a zap logger based on the given environment.
func GetLogger(env string) (*zap.Logger, error) {
	switch env {
	case "live":
		return zap.NewProduction()
	case "staging":
		cfg := zap.NewProductionConfig()
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		return cfg.Build()
	case "testing":
		return zap.NewNop(), nil
	default:
		return zap.NewDevelopment()
	}
}

// getStdLogger returns a logger based on the given environment.
// Required interface for Sarama client.
func getStdLogger(env string) (*log.Logger, error) {
	lg, err := GetLogger(env)
	if err != nil {
		return nil, err
	}
	return zap.NewStdLog(lg), nil
}
