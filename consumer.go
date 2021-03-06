package SaramaMigrator

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"strings"
	"sync"
)

type TransitioningConsumer struct {
	ckgConsumer  *kafka.Consumer
	configMap    *kafka.ConfigMap
	lock         sync.Mutex
	children     map[string]map[int32]*TransitioningPartitionConsumer
	saramaConfig *sarama.Config
}

func NewTransitioningConsumer(addrs []string, saramaConfig *sarama.Config) (sarama.Consumer, error) {
	if saramaConfig == nil {
		saramaConfig = sarama.NewConfig()
	}
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(addrs, ","),
		// TODO: support setting consumer groups from sarama config
		"group.id":                 "toto",
		"auto.offset.reset":        "error",
		"go.events.channel.enable": true,
	}

	err := configMap.SetKey("go.events.channel.size", saramaConfig.ChannelBufferSize)
	if err != nil {
		return nil, err
	}
	err = configMap.SetKey("broker.version.fallback", saramaConfig.Version.String())
	if err != nil {
		return nil, err
	}
	err = configMap.SetKey("session.timeout.ms", int(saramaConfig.Net.ReadTimeout.Milliseconds()))
	if err != nil {
		return nil, err
	}
	consumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, err
	}
	return &TransitioningConsumer{ckgConsumer: consumer, configMap: configMap, saramaConfig: saramaConfig}, nil
}

// Topics returns the set of available topics as retrieved from the cluster
// metadata. This method is the same as Client.Topics(), and is provided for
// convenience.
func (t *TransitioningConsumer) Topics() ([]string, error) {
	metadata, err := t.ckgConsumer.GetMetadata(nil, true, 1000)
	// todo cache metadata to avoid spamming the cluster
	if err != nil {
		return nil, err
	}
	topics := make([]string, 0, len(metadata.Topics))
	for topic, _ := range metadata.Topics {
		topics = append(topics, topic)
	}
	return topics, nil
}

// Partitions returns the sorted list of all partition IDs for the given topic.
// This method is the same as Client.Partitions(), and is provided for convenience.
func (t *TransitioningConsumer) Partitions(topic string) ([]int32, error) {
	metadata, err := t.ckgConsumer.GetMetadata(&topic, false, 1000)
	// todo cache metadata to avoid spamming the cluster
	if err != nil {
		return nil, err
	}
	if topicMetadata, ok := metadata.Topics[topic]; ok {
		ids := make([]int32, 0, len(topicMetadata.Partitions))
		for _, partitionMetadata := range topicMetadata.Partitions {
			ids = append(ids, partitionMetadata.ID)
		}
		return ids, nil
	}
	return nil, fmt.Errorf("could not find metadata for topic %s", topic)
}

// ConsumePartition creates a PartitionConsumer on the given topic/partition with
// the given offset. It will return an error if this Consumer is already consuming
// on the given topic/partition. Offset can be a literal offset, or OffsetNewest
// or OffsetOldest
func (t *TransitioningConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	child, err := newTransitioningPartitionConsumer(topic, partition, offset, t.configMap, t.saramaConfig)
	if err != nil {
		return nil, err
	}
	if err := t.addChild(child); err != nil {
		return nil, err
	}
	return child, nil
}

// HighWaterMarks returns the current high water marks for each topic and partition.
// Consistency between partitions is not guaranteed since high water marks are updated separately.
func (t *TransitioningConsumer) HighWaterMarks() map[string]map[int32]int64 {
	metadata, err := t.ckgConsumer.GetMetadata(nil, true, 1000)
	if err != nil {
		return nil
	}
	highWatermarks := make(map[string]map[int32]int64, len(metadata.Topics))
	for topic, data := range metadata.Topics {
		topicWatermarks := make(map[int32]int64, len(data.Partitions))
		for _, partition := range data.Partitions {
			_, high, err := t.ckgConsumer.GetWatermarkOffsets(topic, partition.ID)
			if err != nil {
				return nil
			}
			topicWatermarks[partition.ID] = high
		}
		highWatermarks[topic] = topicWatermarks
	}

	return highWatermarks
}

// Close shuts down the consumer. It must be called after all child
// PartitionConsumers have already been closed.
func (t *TransitioningConsumer) Close() error {
	if err := t.ckgConsumer.Unassign(); err != nil {
		return err
	}
	return t.ckgConsumer.Close()
}

func (t *TransitioningConsumer) addChild(child *TransitioningPartitionConsumer) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.children == nil {
		t.children = make(map[string]map[int32]*TransitioningPartitionConsumer)
	}
	topicChildren := t.children[child.topic]
	if topicChildren == nil {
		topicChildren = make(map[int32]*TransitioningPartitionConsumer)
		t.children[child.topic] = topicChildren
	}

	if topicChildren[child.partition] != nil {
		return sarama.ConfigurationError("That topic/partition is already being consumed")
	}

	topicChildren[child.partition] = child
	child.parent = t
	return nil
}

func (t *TransitioningConsumer) removeChild(child *TransitioningPartitionConsumer) {
	t.lock.Lock()
	defer t.lock.Unlock()

	delete(t.children[child.topic], child.partition)
}
