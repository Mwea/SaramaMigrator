package SaramaMigrator

import (
	"io"
	"log"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

var testMsg = StringEncoder("Foo")

func init() {
	sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
}

// If a particular offset is provided then messages are consumed starting from
// that offset.
func TestConsumerOffsetManual(t *testing.T) {
	go FailOnTimeout(t, 5*time.Second)

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
	go FailOnTimeout(t, 5*time.Second)

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
	hwo := consumer.HighWaterMarkOffset()
	assert.EqualValues(t, hwo, 14, "Expected high water mark offset 14, found %d", hwo)

	safeClose(t, consumer)
	safeClose(t, master)
	broker0.Close()
}

// It is possible to close a partition consumer and create the same anew.
func TestConsumerRecreate(t *testing.T) {
	go FailOnTimeout(t, 5*time.Second)

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
	go FailOnTimeout(t, 5*time.Second)

	// Given
	broker0 := sarama.NewMockBroker(t, 0)
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("my_topic", 0, broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1000),
		"FetchRequest":       sarama.NewMockFetchResponse(t, 1).SetVersion(4),
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
	assert.Equal(t, pc2, nil)
	assert.Equal(t, err, sarama.ConfigurationError("That topic/partition is already being consumed"))

	safeClose(t, pc1)
	safeClose(t, c)
	broker0.Close()
}

// StdLogger is used to log error messages.
type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

func runConsumerLeaderRefreshErrorTestWithConfig(t *testing.T, config *sarama.Config) {
	var Logger StdLogger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
	// Given
	broker0 := sarama.NewMockBroker(t, 100)

	// Stage 1: my_topic/0 served by broker0
	Logger.Printf("    STAGE 1")

	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("my_topic", 0, broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 123).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1000),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).
			SetMessage("my_topic", 0, 123, testMsg).SetVersion(4),
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})

	c, err := NewTransitioningConsumer([]string{broker0.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	pc, err := c.ConsumePartition("my_topic", 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}

	assertMessageOffset(t, <-pc.Messages(), 123)

	// Stage 2: broker0 says that it is no longer the leader for my_topic/0,
	// but the requests to retrieve metadata fail with network timeout.
	Logger.Printf("    STAGE 2")

	fetchResponse2 := sarama.NewMockFetchResponse(t, 1)
	fetchResponse2.SetError("my_topic", 0, sarama.ErrNotLeaderForPartition).SetVersion(4)

	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"FetchRequest":       fetchResponse2,
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})

	if consErr := <-pc.Errors(); consErr.Err != sarama.ErrOutOfBrokers {
		t.Errorf("Unexpected error: %v", consErr.Err)
	}

	// Stage 3: finally the metadata returned by broker0 tells that broker1 is
	// a new leader for my_topic/0. Consumption resumes.

	Logger.Printf("    STAGE 3")

	broker1 := sarama.NewMockBroker(t, 101)

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).
			SetMessage("my_topic", 0, 124, testMsg).SetVersion(4),
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetBroker(broker1.Addr(), broker1.BrokerID()).
			SetLeader("my_topic", 0, broker1.BrokerID()),
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})

	assertMessageOffset(t, <-pc.Messages(), 124)

	safeClose(t, pc)
	safeClose(t, c)
	broker1.Close()
	broker0.Close()
}

func TestConsumerInvalidTopic(t *testing.T) {
	go FailOnTimeout(t, 5*time.Second)

	// Given
	broker0 := sarama.NewMockBroker(t, 100)
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()),
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})

	c, err := NewTransitioningConsumer([]string{broker0.Addr()}, NewTestConfig())
	if err != nil {
		t.Fatal(err)
	}

	// When
	pc, err := c.ConsumePartition("my_topic", 0, sarama.OffsetOldest)

	// Then
	assert.Nil(t, pc)
	assert.Equalf(t, err, sarama.ErrUnknownTopicOrPartition, "Should fail with, err=%v", err)

	safeClose(t, c)
	broker0.Close()
}

// If the initial offset passed on partition consumer creation is out of the
// actual offset range for the partition, then the partition consumer stops
// immediately closing its output channels.
func TestConsumerShutsDownOutOfRange(t *testing.T) {
	go FailOnTimeout(t, 5*time.Second)
	// Given
	broker0 := sarama.NewMockBroker(t, 0)
	fetchResponse := sarama.NewMockFetchResponse(t, 1)
	fetchResponse.SetError("my_topic", 0, sarama.ErrOffsetOutOfRange).SetVersion(4)
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("my_topic", 0, broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1234).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 7),
		"FetchRequest":       fetchResponse,
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})

	master, err := NewTransitioningConsumer([]string{broker0.Addr()}, NewTestConfig())
	if err != nil {
		t.Fatal(err)
	}

	// When
	consumer, err := master.ConsumePartition("my_topic", 0, 101)
	if err != nil {
		t.Fatal(err)
	}

	// Then: consumer should shut down closing its messages and errors channels.
	if _, ok := <-consumer.Messages(); ok {
		t.Error("Expected the consumer to shut down")
	}
	safeClose(t, consumer)

	safeClose(t, master)
	broker0.Close()
}

// If a fetch response contains messages with offsets that are smaller then
// requested, then such messages are ignored.
func TestConsumerExtraOffsets(t *testing.T) {
	go FailOnTimeout(t, 5*time.Second)
	// Given
	legacyFetchResponse := &sarama.FetchResponse{Version: 4}
	legacyFetchResponse.AddMessage("my_topic", 0, nil, testMsg, 1)
	legacyFetchResponse.AddMessage("my_topic", 0, nil, testMsg, 2)
	legacyFetchResponse.AddMessage("my_topic", 0, nil, testMsg, 3)
	legacyFetchResponse.AddMessage("my_topic", 0, nil, testMsg, 4)
	newFetchResponse := &sarama.FetchResponse{Version: 4}
	newFetchResponse.AddRecord("my_topic", 0, nil, testMsg, 1)
	newFetchResponse.AddRecord("my_topic", 0, nil, testMsg, 2)
	newFetchResponse.AddRecord("my_topic", 0, nil, testMsg, 3)
	newFetchResponse.AddRecord("my_topic", 0, nil, testMsg, 4)
	newFetchResponse.SetLastOffsetDelta("my_topic", 0, 4)
	newFetchResponse.SetLastStableOffset("my_topic", 0, 4)
	for _, fetchResponse1 := range []*sarama.FetchResponse{legacyFetchResponse, newFetchResponse} {

		cfg := NewTestConfig()
		cfg.Consumer.Return.Errors = true
		if fetchResponse1.Version >= 4 {
			cfg.Version = sarama.V0_11_0_0
		}

		broker0 := sarama.NewMockBroker(t, 0)
		fetchResponse2 := &sarama.FetchResponse{}
		fetchResponse2.Version = fetchResponse1.Version
		fetchResponse2.AddError("my_topic", 0, sarama.ErrNoError)
		broker0.SetHandlerByMap(map[string]sarama.MockResponse{
			"MetadataRequest": sarama.NewMockMetadataResponse(t).
				SetBroker(broker0.Addr(), broker0.BrokerID()).
				SetLeader("my_topic", 0, broker0.BrokerID()),
			"OffsetRequest": sarama.NewMockOffsetResponse(t).
				//SetVersion(offsetResponseVersion).
				SetOffset("my_topic", 0, sarama.OffsetNewest, 1234).
				SetOffset("my_topic", 0, sarama.OffsetOldest, 0),
			"FetchRequest":       sarama.NewMockSequence(fetchResponse1, fetchResponse2),
			"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
		})

		master, err := NewTransitioningConsumer([]string{broker0.Addr()}, cfg)
		if err != nil {
			t.Fatal(err)
		}

		// When
		consumer, err := master.ConsumePartition("my_topic", 0, 3)
		if err != nil {
			t.Fatal(err)
		}

		// Then: messages with offsets 1 and 2 are not returned even though they
		// are present in the response.
		select {
		case msg := <-consumer.Messages():
			assertMessageOffset(t, msg, 3)
		case err := <-consumer.Errors():
			t.Fatal(err)
		}

		select {
		case msg := <-consumer.Messages():
			assertMessageOffset(t, msg, 4)
		case err := <-consumer.Errors():
			t.Fatal(err)
		}

		safeClose(t, consumer)
		safeClose(t, master)
		broker0.Close()
	}
}

// In some situations broker may return a block containing only
// messages older then requested, even though there would be
// more messages if higher offset was requested.
func TestConsumerReceivingFetchResponseWithTooOldRecords(t *testing.T) {
	// Given
	fetchResponse1 := sarama.NewMockFetchResponse(t, 1).SetMessage("my_topic", 0, 1, testMsg).
		SetVersion(4)
		//SetHighWaterMark("my_topic", 0, 1)
	fetchResponse2 := sarama.NewMockFetchResponse(t, 1).SetMessage("my_topic", 0, 1000000, testMsg).
		SetVersion(4)
		//SetHighWaterMark("my_topic", 0, 10000)

	cfg := NewTestConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Version = sarama.V0_11_0_0

	broker0 := sarama.NewMockBroker(t, 0)

	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("my_topic", 0, broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1234).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0),
		"FetchRequest":       sarama.NewMockSequence(fetchResponse1, fetchResponse2),
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})

	master, err := NewTransitioningConsumer([]string{broker0.Addr()}, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// When
	consumer, err := master.ConsumePartition("my_topic", 0, 2)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-consumer.Messages():
		assertMessageOffset(t, msg, 1000000)
	case err := <-consumer.Errors():
		t.Fatal(err)
	}

	safeClose(t, consumer)
	safeClose(t, master)
	broker0.Close()
}

func TestConsumerNonSequentialOffsets(t *testing.T) {
	// Given
	legacyFetchResponse := &sarama.FetchResponse{Version: 4}
	legacyFetchResponse.AddMessage("my_topic", 0, nil, testMsg, 5)
	legacyFetchResponse.AddMessage("my_topic", 0, nil, testMsg, 7)
	legacyFetchResponse.AddMessage("my_topic", 0, nil, testMsg, 11)
	newFetchResponse := &sarama.FetchResponse{Version: 4}
	newFetchResponse.AddRecord("my_topic", 0, nil, testMsg, 5)
	newFetchResponse.AddRecord("my_topic", 0, nil, testMsg, 7)
	newFetchResponse.AddRecord("my_topic", 0, nil, testMsg, 11)
	newFetchResponse.SetLastOffsetDelta("my_topic", 0, 11)
	newFetchResponse.SetLastStableOffset("my_topic", 0, 11)
	for _, fetchResponse1 := range []*sarama.FetchResponse{legacyFetchResponse, newFetchResponse} {
		cfg := NewTestConfig()

		broker0 := sarama.NewMockBroker(t, 0)
		fetchResponse2 := &sarama.FetchResponse{Version: fetchResponse1.Version}
		fetchResponse2.AddError("my_topic", 0, sarama.ErrNoError)
		broker0.SetHandlerByMap(map[string]sarama.MockResponse{
			"MetadataRequest": sarama.NewMockMetadataResponse(t).
				SetBroker(broker0.Addr(), broker0.BrokerID()).
				SetLeader("my_topic", 0, broker0.BrokerID()),
			"OffsetRequest": sarama.NewMockOffsetResponse(t).
				SetOffset("my_topic", 0, sarama.OffsetNewest, 1234).
				SetOffset("my_topic", 0, sarama.OffsetOldest, 0),
			"FetchRequest":       sarama.NewMockSequence(fetchResponse1, fetchResponse2),
			"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
		})

		master, err := NewTransitioningConsumer([]string{broker0.Addr()}, cfg)
		if err != nil {
			t.Fatal(err)
		}

		// When
		consumer, err := master.ConsumePartition("my_topic", 0, 3)
		if err != nil {
			t.Fatal(err)
		}

		// Then: messages with offsets 1 and 2 are not returned even though they
		// are present in the response.
		assertMessageOffset(t, <-consumer.Messages(), 5)
		assertMessageOffset(t, <-consumer.Messages(), 7)
		assertMessageOffset(t, <-consumer.Messages(), 11)

		safeClose(t, consumer)
		safeClose(t, master)
		broker0.Close()
	}
}

// TODO
func TestConsumerOffsetOutOfRange(t *testing.T) {
	// Given
	broker0 := sarama.NewMockBroker(t, 2)
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("my_topic", 0, broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1234).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 2345),
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})

	master, err := NewTransitioningConsumer([]string{broker0.Addr()}, NewTestConfig())
	if err != nil {
		t.Fatal(err)
	}

	// When/Then
	if _, err := master.ConsumePartition("my_topic", 0, 0); err != sarama.ErrOffsetOutOfRange {
		t.Fatal("Should return sarama.ErrOffsetOutOfRange, got:", err)
	}
	if _, err := master.ConsumePartition("my_topic", 0, 3456); err != sarama.ErrOffsetOutOfRange {
		t.Fatal("Should return sarama.ErrOffsetOutOfRange, got:", err)
	}
	if _, err := master.ConsumePartition("my_topic", 0, -3); err != sarama.ErrOffsetOutOfRange {
		t.Fatal("Should return sarama.ErrOffsetOutOfRange, got:", err)
	}

	safeClose(t, master)
	broker0.Close()
}

// TODO
func TestConsumerTimestamps(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	type testMessage struct {
		key       Encoder
		offset    int64
		timestamp time.Time
	}
	for _, d := range []struct {
		kversion          sarama.KafkaVersion
		logAppendTime     bool
		messages          []testMessage
		expectedTimestamp []time.Time
	}{
		{sarama.MinVersion, false, []testMessage{
			{testMsg, 1, now},
			{testMsg, 2, now},
		}, []time.Time{time.Time{}, time.Time{}}},
		{sarama.V0_9_0_0, false, []testMessage{
			{testMsg, 1, now},
			{testMsg, 2, now},
		}, []time.Time{time.Time{}, time.Time{}}},
		{sarama.V0_10_0_0, false, []testMessage{
			{testMsg, 1, now},
			{testMsg, 2, now},
		}, []time.Time{time.Time{}, time.Time{}}},
		{sarama.V0_10_2_1, false, []testMessage{
			{testMsg, 1, now.Add(time.Second)},
			{testMsg, 2, now.Add(2 * time.Second)},
		}, []time.Time{now.Add(time.Second), now.Add(2 * time.Second)}},
		{sarama.V0_10_2_1, true, []testMessage{
			{testMsg, 1, now.Add(time.Second)},
			{testMsg, 2, now.Add(2 * time.Second)},
		}, []time.Time{now, now}},
		{sarama.V0_11_0_0, false, []testMessage{
			{testMsg, 1, now.Add(time.Second)},
			{testMsg, 2, now.Add(2 * time.Second)},
		}, []time.Time{now.Add(time.Second), now.Add(2 * time.Second)}},
		{sarama.V0_11_0_0, true, []testMessage{
			{testMsg, 1, now.Add(time.Second)},
			{testMsg, 2, now.Add(2 * time.Second)},
		}, []time.Time{now, now}},
	} {
		var fr *sarama.FetchResponse
		cfg := NewTestConfig()
		cfg.Version = d.kversion
		switch {
		case d.kversion.IsAtLeast(sarama.V0_11_0_0):
			fr = &sarama.FetchResponse{Version: 4, LogAppendTime: d.logAppendTime, Timestamp: now}
			for _, m := range d.messages {
				fr.AddRecordWithTimestamp("my_topic", 0, m.key, testMsg, m.offset, m.timestamp)
			}
			fr.SetLastOffsetDelta("my_topic", 0, 2)
			fr.SetLastStableOffset("my_topic", 0, 2)
		case d.kversion.IsAtLeast(sarama.V0_10_1_0):
			fr = &sarama.FetchResponse{Version: 3, LogAppendTime: d.logAppendTime, Timestamp: now}
			for _, m := range d.messages {
				fr.AddMessageWithTimestamp("my_topic", 0, m.key, testMsg, m.offset, m.timestamp, 1)
			}
		default:
			var version int16
			switch {
			case d.kversion.IsAtLeast(sarama.V0_10_0_0):
				version = 2
			case d.kversion.IsAtLeast(sarama.V0_9_0_0):
				version = 1
			}
			fr = &sarama.FetchResponse{Version: version}
			for _, m := range d.messages {
				fr.AddMessageWithTimestamp("my_topic", 0, m.key, testMsg, m.offset, m.timestamp, 0)
			}
		}

		broker0 := sarama.NewMockBroker(t, 0)
		broker0.SetHandlerByMap(map[string]sarama.MockResponse{
			"MetadataRequest": sarama.NewMockMetadataResponse(t).
				SetBroker(broker0.Addr(), broker0.BrokerID()).
				SetLeader("my_topic", 0, broker0.BrokerID()),
			"OffsetRequest": sarama.NewMockOffsetResponse(t).
				SetOffset("my_topic", 0, sarama.OffsetNewest, 1234).
				SetOffset("my_topic", 0, sarama.OffsetOldest, 0),
			"FetchRequest":       sarama.NewMockSequence(fr),
			"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t).AddApiVersionsResponseBlock(1, 0, fr.Version),
		})

		master, err := NewTransitioningConsumer([]string{broker0.Addr()}, cfg)
		if err != nil {
			t.Fatal(err)
		}

		consumer, err := master.ConsumePartition("my_topic", 0, 1)
		if err != nil {
			t.Fatal(err)
		}

		for i, ts := range d.expectedTimestamp {
			select {
			case msg := <-consumer.Messages():
				assertMessageOffset(t, msg, int64(i)+1)
				if msg.Timestamp != ts {
					t.Errorf("Wrong timestamp (kversion:%v, logAppendTime:%v): got: %v, want: %v",
						d.kversion, d.logAppendTime, msg.Timestamp, ts)
				}
			case err := <-consumer.Errors():
				t.Fatal(err)
			}
		}

		safeClose(t, consumer)
		safeClose(t, master)
		broker0.Close()
	}
}

// When set to sarama.ReadCommitted, no uncommitted message should be available in messages channel
// TODO
func TestExcludeUncommitted(t *testing.T) {
	// Given
	broker0 := sarama.NewMockBroker(t, 0)

	fetchResponse := &sarama.FetchResponse{
		Version: 4,
		Blocks: map[string]map[int32]*sarama.FetchResponseBlock{"my_topic": {0: {
			AbortedTransactions: []*sarama.AbortedTransaction{{ProducerID: 7, FirstOffset: 1235}},
		}}},
	}
	fetchResponse.AddRecordBatch("my_topic", 0, nil, testMsg, 1234, 7, true)          // committed msg
	fetchResponse.AddRecordBatch("my_topic", 0, nil, testMsg, 1235, 7, true)          // uncommitted msg
	fetchResponse.AddRecordBatch("my_topic", 0, nil, testMsg, 1236, 7, true)          // uncommitted msg
	fetchResponse.AddControlRecord("my_topic", 0, 1237, 7, sarama.ControlRecordAbort) // abort control record
	fetchResponse.AddRecordBatch("my_topic", 0, nil, testMsg, 1238, 7, true)          // committed msg

	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(broker0.Addr(), broker0.BrokerID()).
			SetLeader("my_topic", 0, broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetVersion(1).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1237),
		"FetchRequest":       sarama.NewMockWrapper(fetchResponse),
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
	})

	cfg := NewTestConfig()
	cfg.Consumer.Return.Errors = true
	cfg.Version = sarama.V0_11_0_0
	cfg.Consumer.IsolationLevel = sarama.ReadCommitted

	// When
	master, err := NewTransitioningConsumer([]string{broker0.Addr()}, cfg)
	if err != nil {
		t.Fatal(err)
	}

	consumer, err := master.ConsumePartition("my_topic", 0, 1234)
	if err != nil {
		t.Fatal(err)
	}

	// Then: only the 2 committed messages are returned
	select {
	case message := <-consumer.Messages():
		assertMessageOffset(t, message, int64(1234))
	case err := <-consumer.Errors():
		t.Error(err)
	}
	select {
	case message := <-consumer.Messages():
		assertMessageOffset(t, message, int64(1238))
	case err := <-consumer.Errors():
		t.Error(err)
	}

	safeClose(t, consumer)
	safeClose(t, master)
	broker0.Close()
}

// NewTestConfig returns a config meant to be used by tests.
// Due to inconsistencies with the request versions the clients send using the default Kafka version
// and the response versions our mocks use, we default to the minimum Kafka version in most tests
func NewTestConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.MinVersion
	return config
}

// If consumer fails to refresh metadata it keeps retrying with frequency
// specified by `Config.Consumer.Retry.Backoff`.
func TestConsumerLeaderRefreshError(t *testing.T) {
	go FailOnTimeout(t, 5*time.Second)

	config := NewTestConfig()
	config.Net.ReadTimeout = 100 * time.Millisecond
	config.Consumer.Retry.Backoff = 200 * time.Millisecond
	config.Consumer.Return.Errors = true
	config.Metadata.Retry.Max = 0
	runConsumerLeaderRefreshErrorTestWithConfig(t, config)
}

func TestConsumerLeaderRefreshErrorWithBackoffFunc(t *testing.T) {
	go FailOnTimeout(t, 5*time.Second)

	var calls int32 = 0
	config := NewTestConfig()
	config.Net.ReadTimeout = 100 * time.Millisecond
	config.Consumer.Retry.BackoffFunc = func(retries int) time.Duration {
		atomic.AddInt32(&calls, 1)
		return 200 * time.Millisecond
	}
	config.Consumer.Return.Errors = true
	config.Metadata.Retry.Max = 0

	runConsumerLeaderRefreshErrorTestWithConfig(t, config)

	// we expect at least one call to our backoff function
	if calls == 0 {
		t.Fail()
	}
}

func assertMessageOffset(t *testing.T, msg *sarama.ConsumerMessage, expectedOffset int64) {
	assert.Equalf(t, msg.Offset, expectedOffset, "Incorrect message offset: expected=%d, actual=%d", expectedOffset, msg.Offset)
}

func safeClose(t testing.TB, c io.Closer) {
	err := c.Close()
	if err != nil {
		t.Error(err)
	}
}

func FailOnTimeout(t *testing.T, d time.Duration) {
	<-time.After(d)
	//	t.Errorf("Failed because of timeout")
	// panic("Failed because of timeout")
}
