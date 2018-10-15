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
	//resubscribe()
	log.Println("Subscribing to kafka topic:", subsEvents)
	wg.Add(1)
	go subscribe(subsEvents, ReactToSubscriptionEvents)

	log.Println("Initializing Redis pool.")
	if err := connectToRedis(); err != nil {
		log.Fatal("Could not successfully connect to Redis. ", err)
	}
	log.Println("Server successfully started.")
}

func ReactToSubscriptionEvents(key string, value []byte, offset int64) bool {
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

	return true
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
	callback := func(topic string, value []byte, offset int64) bool {
		sessionsToUpdate := readFromRedis(topic)
		if len(sessionsToUpdate) == 0 {
			return false
		}
		for _, session := range sessionsToUpdate {
			kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
				Brokers: brokers,
				Topic:   session,
			})

			kafkaWriter.WriteMessages(context.Background(), kafka.Message{Value: value})
		}
		return true
	}

	wg.Add(1)
	go subscribe(topic, callback)
}

func subscribeToChannel(session string, topic string) {
	if !isSubscribed(session, topic) {
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
	conn.Do("HSET", key, value, "")
}

func readFromRedis(key string) []string {
	result := []string{}
	conn := pool.Get()
	defer conn.Close()

	values, _ := redis.Values(conn.Do("HKEYS", key))
	redis.ScanSlice(values, &result)
	return result
}

func deleteFromRedis(key string, value string) {
	conn := pool.Get()
	defer conn.Close()
	conn.Do("HDEL", key, value)
}

func isSubscribed(session string, topic string) bool {
	conn := pool.Get()
	defer conn.Close()

	res, _ := redis.Int(conn.Do("HEXISTS", session, topic))
	if res == 1 {
		return true
	} else {
		return false
	}
}
