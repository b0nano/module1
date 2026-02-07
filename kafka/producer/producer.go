package producer

import (
	"github.com/b0nano/module1/cofee/config"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// func NewProducer(addrs string) (*kafka.Producer, error) {
func NewProducer(cfg *config.ProducerConfig) (*kafka.Producer, error) {

	producer, err := kafka.NewProducer(&cfg.Config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}
