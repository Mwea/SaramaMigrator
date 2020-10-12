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

// Topics returns the set of available topics as retrieved from the cluster
// metadata. This method is the same as Client.Topics(), and is provided for
// convenience.
func (t TransitioningConsumer) Topics() ([]string, error) {
	return nil, fmt.Errorf("implement me")
}

// Partitions returns the sorted list of all partition IDs for the given topic.
// This method is the same as Client.Partitions(), and is provided for convenience.
func (t TransitioningConsumer) Partitions(topic string) ([]int32, error) {
	return nil, fmt.Errorf("implement me")
}

// ConsumePartition creates a PartitionConsumer on the given topic/partition with
// the given offset. It will return an error if this Consumer is already consuming
// on the given topic/partition. Offset can be a literal offset, or OffsetNewest
// or OffsetOldest
func (t TransitioningConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	return nil, fmt.Errorf("implement me")
}

// HighWaterMarks returns the current high water marks for each topic and partition.
// Consistency between partitions is not guaranteed since high water marks are updated separately.
func (t TransitioningConsumer) HighWaterMarks() map[string]map[int32]int64 {
	panic("implement me")
}

// Close shuts down the consumer. It must be called after all child
// PartitionConsumers have already been closed.
func (t TransitioningConsumer) Close() error {
	return fmt.Errorf("implement me")
}
