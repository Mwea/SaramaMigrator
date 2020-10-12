package SaramaMigrator

import (
	"fmt"
	"github.com/Shopify/sarama"
)

type TransitioningConsumer struct {
}

func NewTransitioningConsumer(strings []string, t interface{}) (sarama.Consumer, error) {
	return nil, fmt.Errorf("implement me")
}

func (t TransitioningConsumer) Topics() ([]string, error) {
	return nil, fmt.Errorf("implement me")
}

func (t TransitioningConsumer) Partitions(topic string) ([]int32, error) {
	return nil, fmt.Errorf("implement me")
}

func (t TransitioningConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	return nil, fmt.Errorf("implement me")
}

func (t TransitioningConsumer) HighWaterMarks() map[string]map[int32]int64 {
	panic("implement me")
}

func (t TransitioningConsumer) Close() error {
	return fmt.Errorf("implement me")
}
