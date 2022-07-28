package kafka

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

type KafkaReader struct {
	reader *kafka.Reader
}

type KafkaReaderConfig struct {
	Brokers       []string
	GroupId       string
	Topic         string
	QueueCapacity int
	MinBytes      int
	MaxBytes      int
}

func NewKafkaReader(config KafkaReaderConfig) (*KafkaReader, error) {
	readCfg := kafka.ReaderConfig{
		Brokers:       config.Brokers,
		GroupID:       config.GroupId,
		Topic:         config.Topic,
		QueueCapacity: config.QueueCapacity,
	}

	if config.MinBytes > 0 {
		readCfg.MinBytes = config.MinBytes
	}

	if config.MaxBytes > 0 {
		readCfg.MaxBytes = config.MaxBytes
	}

	r := kafka.NewReader(
		readCfg,
	)

	return &KafkaReader{reader: r}, nil
}

func (kr *KafkaReader) Write() ([]byte, error) {
	msg, err := kr.reader.ReadMessage(context.Background())
	if nil != err {
		log.Println("Kafka read message error: ", err.Error())
		return nil, err
	}

	return msg.Value, nil
}

func (kr *KafkaReader) Close() error {
	return kr.reader.Close()
}
