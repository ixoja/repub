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

func startProducer() {
	log.Println("Starting the producer")
	wg.Add(1)
	go publishTopic(topicOne)
	wg.Add(1)
	go publishTopic(topicTwo)
}

func publishTopic(topic string) {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Topic:   topic,
	})
	defer writer.Close()
	defer wg.Done()

	for {
		writer.WriteMessages(context.Background(),
			kafka.Message{
				Value: []byte(fmt.Sprint(topic, time.Now())),
			},
		)

		log.Println("message produced", topic)
		time.Sleep(2 * time.Second)
	}
}
