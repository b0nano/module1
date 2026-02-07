package producer

import (
	"log/slog"
	"time"

	"github.com/b0nano/module1/cofee/models"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/jsonschema"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type producerService struct {
	producer       *kafka.Producer
	serializer     *jsonschema.Serializer
	topic          string
	eventCh        chan kafka.Event
	stopCh         chan struct{}
	produceTimeout time.Duration
	logger         *slog.Logger
}

func NewService(producer *kafka.Producer, topic string, produceTimeout time.Duration, serializer *jsonschema.Serializer, logger *slog.Logger) *producerService {
	return &producerService{
		producer:       producer,
		serializer:     serializer,
		topic:          topic,
		eventCh:        producer.Events(),
		stopCh:         make(chan struct{}),
		produceTimeout: produceTimeout,
		logger:         logger.With("service", "producer"),
	}
}

// Run rtun produce messages
func (svc *producerService) Run() {
	go svc.deliveryLogging()
	go svc.runProduceMessages()
}

func (svc *producerService) runProduceMessages() {
	timer := time.NewTicker(svc.produceTimeout)
	for {
		select {
		case <-timer.C:
			svc.producerMessage()
		case <-svc.stopCh:
			svc.producer.Close()
			close(svc.eventCh)
			return
		}
	}
}

func (svc *producerService) deliveryLogging() {
	for e := range svc.eventCh {
		switch m := e.(type) {
		case *kafka.Message:
			if m.TopicPartition.Error != nil {
				svc.logger.Error("delivery failed", "err", m.TopicPartition.Error.Error())
			} else {
				svc.logger.Info("delivery successed",
					"topic", *m.TopicPartition.Topic,
					"partition", m.TopicPartition.Partition,
					"offet", m.TopicPartition.Offset,
				)
			}
		}
	}
}

func (svc *producerService) producerMessage() {
	order := prepareOrder()

	data, err := svc.serializer.Serialize(svc.topic, order)
	if err != nil {
		svc.logger.Error("failed to serialize order")
		return
	}

	// data, _ := json.Marshal(order)
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &svc.topic,
			Partition: kafka.PartitionAny,
		},
		Value:     data,
		Timestamp: time.Now().UTC(),
	}

	if err := svc.producer.Produce(&msg, svc.eventCh); err != nil {
		svc.logger.Error("produce to kafka", "err", err.Error())
	}

}

func prepareOrder() *models.Order {
	return &models.Order{
		ID:       gofakeit.UUID(),
		ClientID: gofakeit.UUID(),
		Created:  gofakeit.Date(),
		Items: func() []models.Item {
			rowsCount := gofakeit.IntRange(1, 20)
			items := make([]models.Item, 0, rowsCount)
			for range rowsCount {
				items = append(items, models.Item{
					ProductID: gofakeit.UUID(),
					Quantity:  gofakeit.IntRange(1, 5),
					Price:     gofakeit.Float64Range(1., 50.),
				})
			}
			return items
		}(),
		Total: gofakeit.Float64Range(1., 50.),
	}
}

// Stop stop produce mesages
func (svc *producerService) Stop() {
	close(svc.stopCh)
}
