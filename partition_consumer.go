package SaramaMigrator

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

type TransitioningPartitionConsumer struct {
	partition    int32
	topic        string
	messages     chan *sarama.ConsumerMessage
	errors       chan *sarama.ConsumerError
	ckgConsumer  *kafka.Consumer
	stopper      *Stopper
	parent       *TransitioningConsumer
	saramaConfig *sarama.Config
	term         chan bool
}

func newTransitioningPartitionConsumer(topic string, partition int32, offset int64, configMap *kafka.ConfigMap, saramaConfig *sarama.Config) (*TransitioningPartitionConsumer, error) {
	consumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, err
	}
	meta, err := consumer.GetMetadata(&topic, false, 1000)
	if err != nil {
		return nil, err
	} else if _, ok := meta.Topics[topic]; !ok {
		return nil, sarama.ErrUnknownTopicOrPartition
	}
	offset, err = pickOffset(consumer, topic, partition, offset)
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
		topic:        topic,
		ckgConsumer:  consumer,
		messages:     make(chan *sarama.ConsumerMessage, saramaConfig.ChannelBufferSize),
		errors:       make(chan *sarama.ConsumerError),
		partition:    partition,
		stopper:      NewStopper(),
		saramaConfig: saramaConfig,
		term:         make(chan bool),
	}
	obj.run()
	return obj, nil
}

func pickOffset(consumer *kafka.Consumer, topic string, partition int32, offset int64) (int64, error) {
	low, high, err := consumer.QueryWatermarkOffsets(topic, partition, 1000)
	if err != nil {
		return 0, err
	}
	switch {
	case offset == sarama.OffsetNewest:
		return high, nil
	case offset == sarama.OffsetOldest:
		return low, nil
	case offset >= low && offset <= high:
		return offset, nil
	default:
		return 0, sarama.ErrOffsetOutOfRange
	}
}

func (t *TransitioningPartitionConsumer) run() {
	go func() {
		for {
			switch {
			case <-t.term:
				if err := t.ckgConsumer.Unassign(); err != nil {
					return
				}
				t.ckgConsumer.Close()
				close(t.messages)
				close(t.errors)
			}
		}
	}()

	t.stopper.Add(1)
	go func() {
		defer t.stopper.Done()
	msgLoop:
		for {
			if t.stopper.Stopped() {
				break
			}
			ev := <-t.ckgConsumer.Events()
			switch e := ev.(type) {
			case *kafka.Message:
				if e.TopicPartition.Error != nil {
					fmt.Println("WESH JE SUIS LA")
					continue
				}
				t.messages <- t.kafkaMessageToSaramaMessage(e)
			case kafka.Error:
				switch sarama.KError(e.Code()) {
				case sarama.ErrOffsetOutOfRange:
					break msgLoop
				default:
					if t.saramaConfig.Consumer.Return.Errors {
						t.errors <- t.kafkaErrorToSaramaError(e)
					}
				}
			default:
				fmt.Println(e)
				// Ignore other event types
			}
		}
		t.term <- true
	}()
}

func (t *TransitioningPartitionConsumer) AsyncClose() {
	t.parent.removeChild(t)
	t.stopper.Stop()
}

func (t *TransitioningPartitionConsumer) Close() error {
	t.AsyncClose()
	return nil
}

func (t *TransitioningPartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return t.messages
}

func (t *TransitioningPartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return t.errors
}

func (t *TransitioningPartitionConsumer) HighWaterMarkOffset() int64 {
	_, high, err := t.ckgConsumer.GetWatermarkOffsets(t.topic, t.partition)
	if err != nil {
		return 0
	}
	return high
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
	if msg.TimestampType == kafka.TimestampNotAvailable {
		msg.Timestamp = time.Time{}
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

func (t *TransitioningPartitionConsumer) kafkaErrorToSaramaError(e kafka.Error) *sarama.ConsumerError {
	return &sarama.ConsumerError{
		Topic:     t.topic,
		Partition: t.partition,
		Err:       e,
	}
}
