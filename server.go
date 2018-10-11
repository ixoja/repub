package main

import (
	"context"
	"fmt"
	"log"

	"github.com/golang/protobuf/proto"
	"github.com/gomodule/redigo/redis"
	"github.com/segmentio/kafka-go"
)

var pool *redis.Pool

func startServer() {
	log.Println("Subscribing to kafka topic:", subsEvents)
	go subscribe(subsEvents, ReactToSubscriptionEvents)

	log.Println("Initializing Redis pool.")
	if err := connectToRedis(); err != nil {
		log.Fatal("Could not successfully connect to Redis. ", err)
	}
	log.Println("Server successfully started.")
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

	kafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Value: []byte(fmt.Sprintf("Received a subscription event. SessionID=%s, Topic=%s, Subscribe=%t",
				session,
				topic,
				shouldSubscribe)),
		})

	if shouldSubscribe {
		subscribeToChannel(session, topic)
	} else {
		unsubscribeFromChannel(session, topic)
	}
}

func connectToRedis() error {
	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "127.0.0.1:6379")
			if err != nil {
				return nil, err
			}

			return c, err
		},
		MaxIdle: 100,
	}

	return pool.Get().Err()
}

func subscribeToChannel(session string, topic string) {
	saveToRedis(session, topic)
	saveToRedis(topic, session)

	callback := func(topic string, value []byte, offset int64) {
		sessionsToUpdate := readFromRedis(topic)

		for _, session := range sessionsToUpdate {
			kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
				Brokers: brokers,
				Topic:   session,
			})

			kafkaWriter.WriteMessages(context.Background(), kafka.Message{Value: value})
		}
	}
	//TODO: check if we already subscribed on topic
	go subscribe(topic, callback)
}

func unsubscribeFromChannel(session string, topic string) {
	deleteFromRedis(session, topic)
	deleteFromRedis(topic, session)
}

func saveToRedis(key string, value string) {

}

func readFromRedis(key string) []string {

}

func deleteFromRedis(key string, value string) {

}
