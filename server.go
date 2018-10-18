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

	go kafkaSubscriber.subscribe(subsEvents, ReactToSubscriptionEvents)

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

func connectToRedis() error {
	pool = &redis.Pool{
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

func subscribeToKafka(session string, topic string) {
	callback := func(topic string, value []byte, offset int64) {
		sessionsToUpdate := readFromRedis(topic)
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

func subscribeToChannel(session string, topic string) {
	if !isSubscribed(session, topic) {
		log.Println("Initializing subscription for session", session, "on topic", topic)
		saveToRedis(session, topic)
		saveToRedis(topic, session)

		subscribeToKafka(session, topic)
	}
}

func unsubscribeFromChannel(session string, topic string) {
	deleteFromRedis(session, topic)
	deleteFromRedis(topic, session)
}

func saveToRedis(key string, value string) {
	conn := pool.Get()
	defer conn.Close()
	_, err := conn.Do("HSET", key, value, "")
	if err != nil {
		log.Println(err)
	}
}

func readFromRedis(key string) []string {
	result := []string{}
	conn := pool.Get()
	defer conn.Close()

	values, err := redis.Values(conn.Do("HKEYS", key))
	if err != nil {
		log.Println(err)
	}
	redis.ScanSlice(values, &result)
	return result
}

func deleteFromRedis(key string, value string) {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("HDEL", key, value)
	if err != nil {
		log.Println(err)
	}
}

func isSubscribed(session string, topic string) bool {
	conn := pool.Get()
	defer conn.Close()

	res, err := redis.Int(conn.Do("HEXISTS", session, topic))
	if err != nil {
		log.Println(err)
	}

	if res == 1 {
		return true
	} else {
		return false
	}
}
