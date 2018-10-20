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

func startServer() {
	if err := redisApi.connect(); err != nil {
		log.Fatal("Could not successfully connect to Redis. ", err)
	}
	tryToRestoreServer()

	log.Println("Subscribing to kafka topic:", subsEvents)
	go kafkaSubscriber.subscribe(subsEvents, ReactToSubscriptionEvents)

	log.Println("Server successfully started.")

	wg.Wait()
}

func tryToRestoreServer() {
	topics := redisApi.getIndex(topicsIndex)
	for _, topic := range topics {
		sessions := redisApi.read(topic)
		for _, session := range sessions {
			go subscribeToKafka(session, topic)
		}
	}
}

func ReactToSubscriptionEvents(key string, value []byte, offset int64) {
	subsEvent := &SubscriptionEvent{}
	proto.Unmarshal(value, subsEvent)

	session := subsEvent.GetSession()
	topic := subsEvent.GetTopic()
	shouldSubscribe := subsEvent.GetSubscribe()

	kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Topic:   session,
	})
	message := fmt.Sprintf("Received a subscription event. SessionID=%s, Topic=%s, Subscribe=%t",
		session,
		topic,
		shouldSubscribe)
	log.Println(message)

	kafkaWriter.WriteMessages(context.Background(),
		kafka.Message{Value: []byte(message)})

	if shouldSubscribe {
		subscribeToChannel(session, topic)
	} else {
		unsubscribeFromChannel(session, topic)
	}
}

func subscribeToChannel(session string, topic string) {
	if !redisApi.isSubscribed(session, topic) {
		log.Println("Initializing subscription for session", session, "on topic", topic)
		redisApi.save(session, topic)
		redisApi.save(topic, session)
		redisApi.addToIndex(topicsIndex, topic)
		redisApi.addToIndex(sessionsIndex, session)

		subscribeToKafka(session, topic)
	}
}

func unsubscribeFromChannel(session string, topic string) {
	redisApi.remove(session, topic)
	redisApi.remove(topic, session)
}

func subscribeToKafka(session string, topic string) {
	callback := func(topic string, value []byte, offset int64) {
		sessionsToUpdate := redisApi.read(topic)
		if len(sessionsToUpdate) == 0 {
			log.Println("Received update for 0 sessions. Unsubscribing from topic:", topic)
			kafkaSubscriber.unsubscribe(topic)
		}
		for _, session := range sessionsToUpdate {
			kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
				Brokers: brokers,
				Topic:   session,
			})

			kafkaWriter.WriteMessages(context.Background(), kafka.Message{Value: value})
		}
	}

	go kafkaSubscriber.subscribe(topic, callback)
}
