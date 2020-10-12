package SaramaMigrator

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type TransitioningPartitionConsumer struct {
	partition int32
	topic     string
}

func newTransitioningPartitionConsumer(topic string, partition int32, offset int64, configMap *kafka.ConfigMap) (*TransitioningPartitionConsumer, error) {
	return nil, fmt.Errorf("implement me")
}

func (t TransitioningPartitionConsumer) AsyncClose() {
	panic("implement me")
}

func (t TransitioningPartitionConsumer) Close() error {
	return fmt.Errorf("implement me")
}

func (t TransitioningPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	panic("implement me")
}

func (t TransitioningPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	panic("implement me")
}

func (t TransitioningPartitionConsumer) HighWaterMarkOffset() int64 {
	panic("implement me")
}
