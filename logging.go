package kafexp

import (
	"go.uber.org/zap"
	"log"
)

// GetZapLogger returns a zap logger based on the given environment.
func GetZapLogger(env string) (*zap.Logger, error) {
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

// GetStdLogger returns a logger based on the given environment.
// Required interface for Sarama client.
func GetStdLogger(env string) (*log.Logger, error) {
	lg, err := GetZapLogger(env)
	if err != nil {
		return nil, err
	}
	return zap.NewStdLog(lg), nil
}
