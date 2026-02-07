package consumer

import (
	"log/slog"

	"github.com/b0nano/module1/cofee/config"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// NewConsumer create new kafka consumer and subscibes to topics
func NewConsumer(config *config.ConsumerConfig) (*kafka.Consumer, error) {
	slog.Info("create consumer")
	consumer, err := kafka.NewConsumer(&config.Config)
	if err != nil {
		return nil, err
	}

	if err = consumer.SubscribeTopics(config.Topics, nil); err != nil {
		return nil, err
	}

	return consumer, nil
}
