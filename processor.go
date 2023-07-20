package kafexp

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"time"
)

// RawMessage is the raw format of consumed Kafka Message
type RawMessage struct {
	Topic     string
	Key       []byte
	Body      []byte
	Headers   map[string]string
	EventTime time.Time
	Partition int32
	Offset    int64
}

// Processor interface that any processor needs to satisfy
type Processor interface {
	Process(ctx context.Context, msg *RawMessage) error
}

func NewPrintProcessor(logger *zap.Logger) Processor {
	return &printProcessor{logger}
}

type printProcessor struct {
	logger *zap.Logger
}

func (p *printProcessor) Process(_ context.Context, msg *RawMessage) error {
	p.logger.Info(fmt.Sprintf("Consumed message: %#v", msg))
	return nil
}
