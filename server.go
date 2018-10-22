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
	err := TryToRestoreServer()
	if err != nil {
		log.Println("WARN: error during server restore.", err)
	}

	log.Println("Subscribing to kafka topic:", subsEvents)
	go kafkaSubscriber.Subscribe(subsEvents, ReactToSubscriptionEvents)

	log.Println("Server successfully started.")

	wg.Wait()
}

func TryToRestoreServer() error {
	topics, err := redisApi.GetIndex(topicsIndex)
	if err != nil {
		return err
	}
	for _, topic := range topics {
		sessions, err := redisApi.Read(topic)
		if err != nil {
			return err
		}
		for _, sess := range sessions {
			go SubscribeToKafka(sess, topic)
		}
	}
	return nil
}

func ReactToSubscriptionEvents(key string, value []byte, offset int64) bool {
	subsEvent := &SubscriptionEvent{}
	err := proto.Unmarshal(value, subsEvent)
	if err != nil {
		log.Println("WARN: could not unmarshal a SubscriptionEvent. Skipping.")
		return true
	}

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

	err = kafkaWriter.WriteMessages(context.Background(), kafka.Message{Value: []byte(message)})
	if err != nil {
		log.Println("Error when writing to kafka.", err)
		return false
	}

	if shouldSubscribe {
		err = SubscribeToChannel(sess, topic)
		if err != nil {
			log.Println("Error when subscribing to message channel.", err)
			return false
		}
	} else {
		err = UnsubscribeFromChannel(sess, topic)
		if err != nil {
			log.Println("Error when unsubscribing from message channel.", err)
			return false
		}
		return false
	}

	return true
}

func SubscribeToChannel(sess string, topic string) error {
	isSub, err := redisApi.IsSubscribed(sess, topic)
	if err != nil {
		return err
	}

	if !isSub {
		log.Println("Initializing subscription for session", sess, "on topic", topic)
		err := redisApi.Save(sess, topic)
		if err != nil {
			return err
		}
		err = redisApi.Save(topic, sess)
		if err != nil {
			return err
		}
		err = redisApi.AddToIndex(topicsIndex, topic)
		if err != nil {
			return err
		}
		err = redisApi.AddToIndex(sessionsIndex, sess)
		if err != nil {
			return err
		}

		SubscribeToKafka(sess, topic)
	}
	return nil
}

func UnsubscribeFromChannel(sess string, topic string) error {
	log.Println("Unsubscribing session", sess, "from topic", topic)
	isSub, err := redisApi.IsSubscribed(sess, topic)
	if err != nil {
		return err
	}
	if !isSub {
		log.Println("Session", sess, "wasn't subscribed on topic", topic, " - skipping.")
		return nil
	}

	err = redisApi.Remove(sess, topic)
	if err != nil {
		return err
	}
	err = redisApi.Remove(topic, sess)
	if err != nil {
		return err
	}
	return nil
}

func SubscribeToKafka(sess string, topic string) {
	callback := func(topic string, value []byte, offset int64) bool {
		sessionsToUpdate, err := redisApi.Read(topic)
		if err != nil {
			log.Println("WARN: could not read a topic from redis.", err)
			return false
		}
		if len(sessionsToUpdate) == 0 {
			log.Println("Received update for 0 sessions. Unsubscribing from topic:", topic)
			kafkaSubscriber.Unsubscribe(topic)
		}
		for _, sess := range sessionsToUpdate {
			kafkaWriter := kafka.NewWriter(kafka.WriterConfig{
				Brokers: brokers,
				Topic:   sess,
			})

			err := kafkaWriter.WriteMessages(context.Background(), kafka.Message{Value: value})
			if err != nil {
				log.Println("WARN: could not write a message to kafka.", err)
				return false
			}
		}

		return true
	}

	go kafkaSubscriber.Subscribe(topic, callback)
}
