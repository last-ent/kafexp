package kafexp

import (
	"context"
	"database/sql"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// ConsumerGroup implements sarama.ConsumerGroupHandler interface
// sarama.ConsumerGroupHandler instances are used to handle individual topic/partition claims.
// It also provides hooks for your consumer group session life-cycle and allow you to
// trigger logic before or after the consume loop(s).
type ConsumerGroup struct {
	db        *sql.DB
	consumer  sarama.ConsumerGroup
	processor Processor
	topics    []string
	logger    *zap.Logger
}

// Setup (from Sarama docs) is run at the beginning of a new session, before ConsumeClaim,
// is likely to be called from several goroutines concurrently
func (c *ConsumerGroup) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup (from Sarama docs) is run at the end of a session, once all ConsumeClaim goroutines have exited
// but before the offsets are committed for the very last time,
// is likely to be called from several goroutines concurrently
func (c *ConsumerGroup) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Close (from Sarama docs) stops the ConsumerGroup and detaches any running sessions.
// It is required to call this function before the object passes out of scope, as it will otherwise leak memory.
func (c *ConsumerGroup) Close() error {
	c.logger.Info("Closing consumer group...")
	return c.consumer.Close()
}

// Start will start consuming messages from Kafka topic using sarama's interface
// consumer.Consume joins a cluster of consumers for a given list of topics and
// starts a blocking ConsumerGroupSession through the ConsumerGroupHandler.
func (c *ConsumerGroup) Start(ctx context.Context, errChan chan error) {
	// consumer.Consume is called inside an infinite loop, when a
	// server-side rebalance happens, the consumer session will need to be
	// recreated to get the new claims.
	for {
		c.logger.Info("Starting message consumption...")
		err := c.consumer.Consume(ctx, c.topics, c)
		if err != nil {
			c.logger.Error(errors.Wrap(err, "Failed to consume messages").Error())
			errChan <- err
			return
		}
		if ctx.Err() != nil {
			c.logger.Error(errors.Wrap(ctx.Err(), "context cancelled").Error())
			errChan <- ctx.Err()
			return
		}
	}
}

// NewConsumerGroup is the constructor function for ConsumerGroup
// The consumers join the group and is assigned their "fair share" of partitions, aka 'claims'
func NewConsumerGroup(cfg Config, pr Processor, logger *zap.Logger) (*ConsumerGroup, error) {
	stdLogger, err := GetStdLogger(cfg.Env)
	if err != nil {
		logger.Warn("failed to setup ConsumerGroup logger", zap.Error(err))
	}
	cfg = setupSarama(cfg, stdLogger)

	scg, err := sarama.NewConsumerGroup(cfg.Addresses, cfg.GroupID, cfg.Sarama)
	if err != nil {
		return nil, errors.Wrap(err, "error creating consumer")
	}

	return &ConsumerGroup{
		consumer:  scg,
		processor: pr,
		topics:    cfg.Topics,
		logger:    logger,
	}, nil
}

// ConsumeClaim is used to handle individual topic/partition claims
// is likely to be called from several goroutines concurrently
// handles the consumer loop
func (c *ConsumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// ConsumeClaim is already invoked by a goroutine
	for {
		select {
		case newMsg := <-claim.Messages():
			if newMsg != nil {
				msg := &RawMessage{
					Topic:     newMsg.Topic,
					Key:       newMsg.Key,
					Body:      newMsg.Value,
					Partition: newMsg.Partition,
					Offset:    newMsg.Offset,
					EventTime: newMsg.Timestamp,
					Headers:   convertSaramaHeaders(newMsg.Headers),
				}

				err := c.processor.Process(session.Context(), msg)
				if err != nil {
					c.logger.Error("error occurred while consuming legacy subscription message", zap.Error(err))

					err = c.processor.HandleError(msg, err)
					if err != nil {
						c.logger.Error("failed to handle error", zap.Error(err))
					}
				}

				session.MarkMessage(newMsg, "")
				session.Commit()
			}

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

func convertSaramaHeaders(saramaHeaders []*sarama.RecordHeader) map[string]string {
	headers := make(map[string]string)
	for _, header := range saramaHeaders {
		key := string(header.Key)
		value := string(header.Value)
		headers[key] = value
	}
	return headers
}
