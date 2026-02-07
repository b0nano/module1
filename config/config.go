package config

import (
	"flag"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/ilyakaznacheev/cleanenv"
)

var (
	c          *Config
	once       sync.Once
	configPath = flag.String("config", "config.yaml", "config file path")
)

type Config struct {
	Producer              ProducerConfig       `yaml:"producer"`
	SingleMessageConsumer ConsumerConfig       `yaml:"single_message_consumer"`
	BatchMessageConsumer  ConsumerConfig       `yaml:"batch_message_consumer"`
	SchemaRegistry        SchemaRegistryConfig `yaml:"schemaregistry"`
}

type ProducerConfig struct {
	Topic          string          `yaml:"topic"`
	ProduceTimeout time.Duration   `yaml:"produce_timeout"`
	Config         kafka.ConfigMap `yaml:"producer_config"`
}

type ConsumerConfig struct {
	Name      string          `yaml:"name"`
	Topics    []string        `yaml:"subscribe_topics"`
	BatchSize int             `yaml:"batch_size"`
	Config    kafka.ConfigMap `yaml:"consumer_config"`
}

type SchemaRegistryConfig struct {
	URL string `yaml:"url"`
}

func MustLoad() *Config {
	once.Do(func() {
		if !flag.Parsed() {
			flag.Parse()
		}
		c = new(Config)
		if err := cleanenv.ReadConfig(*configPath, c); err != nil {
			panic(err)
		}
	})

	return c
}
