package SaramaMigrator

import (
	"github.com/Shopify/sarama"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

type TransitioningPartitionConsumer struct {
	partition   int32
	topic       string
	messages    chan *sarama.ConsumerMessage
	errors      chan *sarama.ConsumerError
	ckgConsumer *kafka.Consumer
	stopper     *Stopper
}

func newTransitioningPartitionConsumer(topic string, partition int32, offset int64, configMap *kafka.ConfigMap) (*TransitioningPartitionConsumer, error) {
	consumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, err
	}
	err = consumer.Assign([]kafka.TopicPartition{kafka.TopicPartition{
		Topic:     &topic,
		Partition: partition,
		Offset:    kafka.Offset(offset),
		Metadata:  nil,
		Error:     nil,
	}})
	if err != nil {
		return nil, err
	}
	obj := &TransitioningPartitionConsumer{
		topic:       topic,
		ckgConsumer: consumer,
		messages:    make(chan *sarama.ConsumerMessage),
		errors:      make(chan *sarama.ConsumerError),
		partition:   partition,
		stopper:     NewStopper(),
	}
	obj.run()
	return obj, nil
}

func (t *TransitioningPartitionConsumer) run() {
	t.stopper.Add(1)
	go func() {
		defer t.stopper.Done()
		for {
			if t.stopper.Stopped() {
				return
			}
			msg, err := t.ckgConsumer.ReadMessage(sarama.NewConfig().Consumer.MaxWaitTime)
			if err != nil {
				continue
			} else if msg != nil {
				t.messages <- t.kafkaMessageToSaramaMessage(msg)
			}
		}
	}()
}

func (t TransitioningPartitionConsumer) AsyncClose() {
	t.stopper.StopAndWait()
}

func (t TransitioningPartitionConsumer) Close() error {
	t.stopper.StopAndWait()
	return nil
}

func (t TransitioningPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return t.messages
}

func (t TransitioningPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return t.errors
}

func (t TransitioningPartitionConsumer) HighWaterMarkOffset() int64 {
	panic("implement me")
}

func (t *TransitioningPartitionConsumer) kafkaMessageToSaramaMessage(msg *kafka.Message) *sarama.ConsumerMessage {
	headers := make([]*sarama.RecordHeader, len(msg.Headers))
	for _, header := range msg.Headers {
		newHeader := &sarama.RecordHeader{
			Key:   []byte(header.Key),
			Value: header.Value,
		}
		headers = append(headers, newHeader)
	}
	return &sarama.ConsumerMessage{
		Headers:        headers,
		Timestamp:      msg.Timestamp,
		BlockTimestamp: time.Time{},
		Key:            msg.Key,
		Value:          msg.Value,
		Topic:          *msg.TopicPartition.Topic,
		Partition:      msg.TopicPartition.Partition,
		Offset:         int64(msg.TopicPartition.Offset),
	}
}
