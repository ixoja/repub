package main

import (
	"context"
	"fmt"
	"log"

	"github.com/golang/protobuf/proto"
	"github.com/segmentio/kafka-go"
)

var redisApi RedisApi = RedisApi{}

const topicsIndex = "topics"

func StartServer() {
	if err := redisApi.Connect(); err != nil {
		log.Fatal("Could not successfully connect to Redis. ", err)
	}
	TryToRestoreServer()

	log.Println("Subscribing to kafka topic:", subsEvents)
	go kafkaSubscriber.Subscribe(subsEvents, ReactToSubscriptionEvents)

	log.Println("Server successfully started.")

	wg.Wait()
}

func TryToRestoreServer() {
	topics := redisApi.GetIndex(topicsIndex)
	for _, topic := range topics {
		sessions := redisApi.Read(topic)
		for _, sess := range sessions {
			go SubscribeToKafka(sess, topic)
		}
	}
}

func ReactToSubscriptionEvents(key string, value []byte, offset int64) {
	subsEvent := &SubscriptionEvent{}
	proto.Unmarshal(value, subsEvent)

	sess := subsEvent.GetSession()
	topic := subsEvent.GetTopic()
	shouldSubscribe := subsEvent.GetSubscribe()

	kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Topic:   sess,
	})
	message := fmt.Sprintf("Received a subscription event. SessionID=%s, Topic=%s, Subscribe=%t",
		sess,
		topic,
		shouldSubscribe)
	log.Println(message)

	kafkaWriter.WriteMessages(context.Background(),
		kafka.Message{Value: []byte(message)})

	if shouldSubscribe {
		SubscribeToChannel(sess, topic)
	} else {
		UnsubscribeFromChannel(sess, topic)
	}
}

func SubscribeToChannel(sess string, topic string) {
	if !redisApi.IsSubscribed(sess, topic) {
		log.Println("Initializing subscription for session", sess, "on topic", topic)
		redisApi.Save(sess, topic)
		redisApi.Save(topic, sess)
		redisApi.AddToIndex(topicsIndex, topic)
		redisApi.AddToIndex(sessionsIndex, sess)

		SubscribeToKafka(sess, topic)
	}
}

func UnsubscribeFromChannel(sess string, topic string) {
	redisApi.Remove(sess, topic)
	redisApi.Remove(topic, sess)
}

func SubscribeToKafka(sess string, topic string) {
	callback := func(topic string, value []byte, offset int64) {
		sessionsToUpdate := redisApi.Read(topic)
		if len(sessionsToUpdate) == 0 {
			log.Println("Received update for 0 sessions. Unsubscribing from topic:", topic)
			kafkaSubscriber.Unsubscribe(topic)
		}
		for _, sess := range sessionsToUpdate {
			kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
				Brokers: brokers,
				Topic:   sess,
			})

			kafkaWriter.WriteMessages(context.Background(), kafka.Message{Value: value})
		}
	}

	go kafkaSubscriber.Subscribe(topic, callback)
}
