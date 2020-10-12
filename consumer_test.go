package SaramaMigrator

import (
	"io"
	"log"
	"os"
	"testing"

	"github.com/Shopify/sarama"
)

var testMsg = StringEncoder("Foo")

func init() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
}

// If a particular offset is provided then messages are consumed starting from
// that offset.
func TestConsumerOffsetManual(t *testing.T) {
	// Given
	broker0 := sarama.NewMockBroker(t, 0)

	mockFetchResponse := sarama.NewMockFetchResponse(t, 1)
	for i := 0; i < 10; i++ {
		mockFetchResponse.SetMessage("my_topic", 0, int64(i+1234), testMsg).SetVersion(4).
			SetHighWaterMark("my_topic", 0, 2345)
	}

	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("my_topic", 0, broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 2345),
		"FetchRequest":       mockFetchResponse,
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})

	// When
	master, err := NewTransitioningConsumer([]string{broker0.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	consumer, err := master.ConsumePartition("my_topic", 0, 1234)
	if err != nil {
		t.Fatal(err)
	}

	// Then: messages starting from offset 1234 are consumed.
	for i := 0; i < 10; i++ {
		select {
		case message := <-consumer.Messages():
			assertMessageOffset(t, message, int64(i+1234))
		case err := <-consumer.Errors():
			t.Error(err)
		}
	}

	safeClose(t, consumer)
	safeClose(t, master)
	broker0.Close()
}

// If `sarama.OffsetNewest` is passed as the initial offset then the first consumed
// message is indeed corresponds to the offset that broker claims to be the
// newest in its metadata response.
func TestConsumersaramaOffsetNewest(t *testing.T) {
	// Given
	broker0 := sarama.NewMockBroker(t, 0)
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("my_topic", 0, broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 10).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 7),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).
			SetMessage("my_topic", 0, 9, testMsg).
			SetMessage("my_topic", 0, 10, testMsg).
			SetMessage("my_topic", 0, 11, testMsg).
			SetHighWaterMark("my_topic", 0, 14).SetVersion(4),
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})

	master, err := NewTransitioningConsumer([]string{broker0.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// When
	consumer, err := master.ConsumePartition("my_topic", 0, sarama.OffsetNewest)
	if err != nil {
		t.Fatal(err)
	}

	// Then
	assertMessageOffset(t, <-consumer.Messages(), 10)
	if hwmo := consumer.HighWaterMarkOffset(); hwmo != 14 {
		t.Errorf("Expected high water mark offset 14, found %d", hwmo)
	}

	safeClose(t, consumer)
	safeClose(t, master)
	broker0.Close()
}

// It is possible to close a partition consumer and create the same anew.
func TestConsumerRecreate(t *testing.T) {
	// Given
	broker0 := sarama.NewMockBroker(t, 0)
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("my_topic", 0, broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1000),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).
			SetMessage("my_topic", 0, 10, testMsg).SetVersion(4),
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})

	c, err := NewTransitioningConsumer([]string{broker0.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	pc, err := c.ConsumePartition("my_topic", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	assertMessageOffset(t, <-pc.Messages(), 10)

	// When
	safeClose(t, pc)
	pc, err = c.ConsumePartition("my_topic", 0, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Then
	assertMessageOffset(t, <-pc.Messages(), 10)

	safeClose(t, pc)
	safeClose(t, c)
	broker0.Close()
}

// An attempt to consume the same partition twice should fail.
func TestConsumerDuplicate(t *testing.T) {
	// Given
	broker0 := sarama.NewMockBroker(t, 0)
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("my_topic", 0, broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1000),
		"FetchRequest":       sarama.NewMockFetchResponse(t, 1),
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})

	c, err := NewTransitioningConsumer([]string{broker0.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}

	pc1, err := c.ConsumePartition("my_topic", 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	// When
	pc2, err := c.ConsumePartition("my_topic", 0, 0)

	// Then
	if pc2 != nil || err != sarama.ConfigurationError("That topic/partition is already being consumed") {
		t.Fatal("A partition cannot be consumed twice at the same time")
	}

	safeClose(t, pc1)
	safeClose(t, c)
	broker0.Close()
}

func assertMessageOffset(t *testing.T, msg *sarama.ConsumerMessage, expectedOffset int64) {
	if msg.Offset != expectedOffset {
		t.Errorf("Incorrect message offset: expected=%d, actual=%d", expectedOffset, msg.Offset)
	}
}

func safeClose(t testing.TB, c io.Closer) {
	err := c.Close()
	if err != nil {
		t.Error(err)
	}
}
