package consumer

import (
	"log/slog"
	"time"

	"github.com/b0nano/module1/cofee/config"
	"github.com/b0nano/module1/cofee/models"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/jsonschema"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type consumerService struct {
	consumer               *kafka.Consumer
	cfg                    *config.ConsumerConfig
	deserializer           *jsonschema.Deserializer
	autoCommit             bool
	batchSize              int
	collectDeadliteTimeout time.Duration
	readTimeoutMs          int
	logger                 *slog.Logger
	stopCh                 chan struct{}
}

func NewService(consumer *kafka.Consumer, cfg *config.ConsumerConfig, batchSize int, collectDeadlineTimeout time.Duration, deserializer *jsonschema.Deserializer, logger *slog.Logger) *consumerService {
	autoCommit, _ := cfg.Config.Get("enable.auto.commit", false)

	return &consumerService{
		consumer:               consumer,
		cfg:                    cfg,
		deserializer:           deserializer,
		autoCommit:             autoCommit.(bool),
		batchSize:              batchSize,
		readTimeoutMs:          100,
		collectDeadliteTimeout: collectDeadlineTimeout,
		logger:                 logger.With("service", cfg.Name),
		stopCh:                 make(chan struct{}),
	}
}

func (svc *consumerService) Run() {
	go svc.runCollect()
}

func (svc *consumerService) runCollect() {
	for {
		select {
		case <-svc.stopCh:
			if err := svc.consumer.Close(); err != nil {
				svc.logger.Error("close consumer", slog.Any("err", err))
			}
		default:
			svc.collect(svc.handleMessages)
		}
	}
}

func (svc *consumerService) collect(cb func(msgs ...*kafka.Message)) {
	var messages []*kafka.Message
	var cFn func() []*kafka.Message
	if svc.batchSize > 1 {
		cFn = svc.collectBatch
	} else {
		cFn = svc.collectImmediate
	}

	messages = cFn()
	cb(messages...)

	if svc.autoCommit {
		return
	}

	// nothing to commit
	if len(messages) == 0 {
		return
	}

	if _, err := svc.consumer.Commit(); err != nil {
		svc.logger.Error("commit messages",
			slog.Any("err", err))
	}
}

func (svc *consumerService) handleMessages(msgs ...*kafka.Message) {
	svc.logger.Info("start handler messages", slog.Int("count", len(msgs)))

	for _, m := range msgs {
		if m == nil {
			svc.logger.Warn("empty message")
			continue
		}

		topic := getString(m.TopicPartition.Topic)

		var order models.Order
		err := svc.deserializer.DeserializeInto(topic, m.Value, &order)
		if err != nil {
			svc.logger.Error("failed to deserialize message", slog.Any("err", err))
			continue
		}

		svc.logger.Info("read message from kafka",
			slog.String("topic", *m.TopicPartition.Topic),
			slog.Int("partition", int(m.TopicPartition.Partition)),
			slog.Int("offset", int(m.TopicPartition.Offset)),
			slog.Time("timestamp", m.Timestamp),
			slog.Any("order", order),
		)
	}
}

func (svc *consumerService) collectBatch() []*kafka.Message {
	messageBatch := make([]*kafka.Message, 0, svc.batchSize)
	deadline := time.Now().Add(svc.collectDeadliteTimeout)

	for len(messageBatch) < svc.batchSize {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		e := svc.consumer.Poll(svc.readTimeoutMs)
		if e == nil {
			continue
		}

		switch m := e.(type) {
		case *kafka.Message:
			messageBatch = append(messageBatch, m)
		case *kafka.Error:
			svc.logger.Error("kafka send error",
				slog.Any("code", m.Code()),
				slog.Any("err", m.Error()),
			)
		}
	}

	return messageBatch
}

func (svc *consumerService) collectImmediate() []*kafka.Message {
	e := svc.consumer.Poll(svc.readTimeoutMs)
	if e == nil {
		return nil
	}

	messageBatch := make([]*kafka.Message, 1)

	switch m := e.(type) {
	case *kafka.Message:
		messageBatch[0] = m
	case *kafka.Error:
		svc.logger.Error("kafka send error",
			slog.Any("code", m.Code()),
			slog.Any("err", m.Error()),
		)
	}
	return messageBatch
}

func (svc *consumerService) Stop() {
	close(svc.stopCh)
}

// getString returns string from pointer
// or empty string if nil
func getString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
