package main

import (
	"context"
	"log"
	"sync"

	kafka "github.com/segmentio/kafka-go"
)

type Subscriber interface {
	Subscribe(topic string, callback func(topic string, message []byte, offset int64))
	Unsubscribe(topic string)
}

type KafkaSubscriber struct {
	activeSubscriptions map[string](chan bool)
	mutex               sync.RWMutex
}

func NewSubscriber() *KafkaSubscriber {
	kafkaSubscriber := &KafkaSubscriber{}
	kafkaSubscriber.activeSubscriptions = make(map[string](chan bool))
	return kafkaSubscriber
}

func (ks *KafkaSubscriber) Subscribe(topic string, callback func(string, []byte, int64) bool) {
	wg.Add(1)
	defer wg.Done()

	log.Println("Subscribe method called for topic:", topic)
	ks.mutex.RLock()
	if _, ok := ks.activeSubscriptions[topic]; ok {
		ks.mutex.RUnlock()
		log.Println("Already subscribed on the topic, skipping.", topic)
		return
	}
	ks.mutex.RUnlock()

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  "group",
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	quit := make(chan bool)

	ks.mutex.Lock()
	ks.activeSubscriptions[topic] = quit
	ks.mutex.Unlock()

	defer kafkaReader.Close()

	ks.ReaderLoop(kafkaReader, quit, topic, callback)
}

func (ks *KafkaSubscriber) ReaderLoop(kafkaReader *kafka.Reader,
	quit chan bool,
	topic string,
	callback func(string, []byte, int64) bool) {

	ctx := context.Background()
	for {
		m, err := kafkaReader.FetchMessage(ctx)
		if err != nil {
			log.Println("Error reading Kafka message: ", err)
			break
		}

		select {
		case <-quit:
			ks.mutex.Lock()
			err = kafkaReader.Close()
			if err != nil {
				log.Println("Error closing Kafka reader: ", err)
			}
			delete(ks.activeSubscriptions, topic)
			ks.mutex.Unlock()
			break
		default:
			log.Println("Received an update from Kafka on topic:", topic)
			success := callback(topic, m.Value, m.Offset)
			if success {
				err = kafkaReader.CommitMessages(ctx, m)
				if err != nil {
					log.Println("Error committing Kafka message: ", err)
					break
				}
			} else {
				log.Println("Kafka subscription callback didn't secceed. Closing reader.")
				err = kafkaReader.Close()
				if err != nil {
					log.Println("Error closing Kafka reader: ", err)
					break
				}
			}
		}
	}
}

func (ks *KafkaSubscriber) Unsubscribe(topic string) {
	log.Println("Unsubscribing from topic:", topic)
	ks.mutex.RLock()
	quit, ok := ks.activeSubscriptions[topic]
	ks.mutex.RUnlock()
	if ok {
		quit <- true
	}
}
