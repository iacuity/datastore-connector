package kafka

import (
	"bufio"
	"context"
	"errors"

	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	newLineByts = []byte("\n")
)

type Message []byte

type KafkaWritter struct {
	writter    *kafka.Writer
	logWritter *bufio.Writer
}

type KafkaWritterConfig struct {
	Brokers      []string
	Topic        string
	MaxAttempts  int
	BatchSize    int
	BatchTimeout time.Duration
	WriteTimeout time.Duration
	LogFileName  string
}

func NewKafkaWritter(config KafkaWritterConfig) (*KafkaWritter, error) {
	if len(config.Brokers) == 0 {
		return nil, errors.New("cannot create a kafka writer with an empty list of brokers")
	}

	w := &kafka.Writer{
		Addr:                   kafka.TCP(config.Brokers...),
		Topic:                  config.Topic,
		MaxAttempts:            config.MaxAttempts,
		Balancer:               &kafka.RoundRobin{},
		BatchSize:              config.BatchSize,
		BatchTimeout:           config.BatchTimeout,
		WriteTimeout:           config.WriteTimeout,
		Compression:            kafka.Lz4,
		AllowAutoTopicCreation: true,
	}

	kw := &KafkaWritter{writter: w, logWritter: nil}

	if "" != config.LogFileName {
		fh, err := os.OpenFile(config.LogFileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if nil != err {
			return nil, err
		}

		kw.logWritter = bufio.NewWriter(fh)
	}

	return kw, nil
}

func (kw *KafkaWritter) Write(msgs []Message) {
	// as key is not specified
	// message can go to any partition using a round-robin technique
	var kafkaMsgs []kafka.Message
	for _, msg := range msgs {
		kafkaMsgs = append(kafkaMsgs, kafka.Message{Value: msg})
	}

	err := kw.writter.WriteMessages(
		context.Background(),
		kafkaMsgs...,
	)

	if nil != err {
		if nil != kw.logWritter {
			for _, msg := range msgs {
				kw.logWritter.Write(msg)
				kw.logWritter.Write(newLineByts)
			}
		}
	}
}

func (kw *KafkaWritter) Close() error {
	if nil != kw.logWritter {
		kw.logWritter.Flush()
	}
	return kw.writter.Close()
}
