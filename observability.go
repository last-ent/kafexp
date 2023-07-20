package kafexp

import (
	"context"
	"fmt"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
)

var (
	partitionTag, _ = tag.NewKey("partition")
	topicTag, _     = tag.NewKey("topic")
	reasonTag, _    = tag.NewKey("reason")

	eventCounterM = stats.Int64(
		"kafka_consumer_events_total",
		"A counter for total number of events being consumed",
		stats.UnitDimensionless,
	)
	eventCounterView = &view.View{
		Name:        "kafka_consumer_events_total",
		Measure:     eventCounterM,
		Description: "A counter for total number of events being consumed",
		TagKeys:     []tag.Key{partitionTag, topicTag, reasonTag},
		Aggregation: view.Count(),
	}
)

// Instrument adds metrics to events
func Instrument(ctx context.Context, m *RawMessage, reason string, logger *zap.Logger) {
	ctx, err := tag.New(ctx,
		tag.Upsert(partitionTag, fmt.Sprintf("%d", m.Partition)),
		tag.Upsert(topicTag, m.Topic),
		tag.Upsert(reasonTag, reason),
	)
	if err != nil {
		logger.Error("unable to instrument consumed message", zap.Error(err))
	}
	stats.Record(ctx, eventCounterM.M(1))
	logger.Debug("instrumented message",
		zap.Int32("partition", m.Partition),
		zap.String("topic", m.Topic),
		zap.String("reason", reason),
	)
}

// registerMonitoringViews registers monitoring views
func registerMonitoringViews() error {
	return view.Register(eventCounterView)
}
