package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/b0nano/module1/cofee/config"
	"github.com/b0nano/module1/cofee/kafka/consumer"
	"github.com/b0nano/module1/cofee/kafka/producer"
	consumerSvc "github.com/b0nano/module1/cofee/services/consumer"
	svcProducerSvc "github.com/b0nano/module1/cofee/services/producer"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/jsonschema"
)

func main() {
	cfg := config.MustLoad()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{}))
	slog.SetDefault(logger)

	logger.Info("init kafka producer")
	producer, err := producer.NewProducer(&cfg.Producer)
	if err != nil {
		logger.Error("", slog.Any("err", err))
		return
	}

	logger.Info("init kafka immediate consumer")
	immediateConsumer, err := consumer.NewConsumer(&cfg.SingleMessageConsumer)
	if err != nil {
		logger.Error("", slog.Any("err", err), slog.Any("config", cfg.SingleMessageConsumer))
		return
	}

	logger.Info("init kafka batch consumer")
	batchConsumer, err := consumer.NewConsumer(&cfg.BatchMessageConsumer)
	if err != nil {
		logger.Error("", slog.Any("err", err), slog.Any("config", cfg.BatchMessageConsumer))
		return
	}

	logger.Info("init schemaregistry")
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(cfg.SchemaRegistry.URL))
	if err != nil {
		logger.Error("", slog.Any("err", err))
		return
	}

	logger.Info("init json serializer")
	serializer, err := jsonschema.NewSerializer(srClient, serde.ValueSerde, jsonschema.NewSerializerConfig())
	if err != nil {
		logger.Error("", slog.Any("err", err))
		return
	}

	logger.Info("init json deserializer")
	deserializer, err := jsonschema.NewDeserializer(srClient, serde.ValueSerde, jsonschema.NewDeserializerConfig())
	if err != nil {
		logger.Error("", slog.Any("err", err))
		return
	}

	logger.Info("init producer service")
	produceSvc := svcProducerSvc.NewService(producer, cfg.Producer.Topic, cfg.Producer.ProduceTimeout, serializer, logger)

	logger.Info("init consumer services")
	batchSvc := consumerSvc.NewService(batchConsumer, &cfg.BatchMessageConsumer, 10, time.Second, deserializer, logger)
	immediateSvc := consumerSvc.NewService(immediateConsumer, &cfg.SingleMessageConsumer, 1, time.Second, deserializer, logger)

	termCh := make(chan struct{})
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		logger.Info("terminate application", "sig", <-c)
		termCh <- struct{}{}
	}()

	produceSvc.Run()
	immediateSvc.Run()
	batchSvc.Run()

	defer batchSvc.Stop()
	defer immediateSvc.Stop()
	defer produceSvc.Stop()

	<-termCh

}
