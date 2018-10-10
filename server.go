package main

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/segmentio/kafka-go"
)

var kafkaSubsWriter *kafka.Writer

func startServer() {
	kafkaSubsWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Topic:   subsEvents,
	})
	subscribe(subsEvents, ReactToSubscriptionEvents)
}

func ReactToSubscriptionEvents(key string, value []byte, offset int64) {
	subsEvent := &SubscriptionEvent{}
	proto.Unmarshal(value, subsEvent)
	kafkaSubsWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key: []byte(subsEvent.GetSession()),
			Value: []byte(fmt.Sprintf("Received a subscription event. SessionID=%s, Topic=%s, Subscribe=%t",
				subsEvent.GetSession(),
				subsEvent.GetTopic(),
				subsEvent.GetSubscribe())),
		})
}
