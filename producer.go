package main

import (
	"context"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const (
	topicOne = "Topic-1"
	topicTwo = "Topic-2"
)

func StartProducer() {
	log.Println("Starting the producer")
	wg.Add(1)
	go PublishTopic(topicOne)
	wg.Add(1)
	go PublishTopic(topicTwo)
}

func PublishTopic(topic string) {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Topic:   topic,
	})
	defer writer.Close()
	defer wg.Done()

	for {
		err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Value: []byte(fmt.Sprint(topic, time.Now())),
			},
		)

		if err != nil {
			log.Println("Could not write to kafka, error:", err)
			break
		}

		log.Println("message produced", topic)
		time.Sleep(2 * time.Second)
	}
}
