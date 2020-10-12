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
		return
	}

	safeClose(t, consumer)
	safeClose(t, master)
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
