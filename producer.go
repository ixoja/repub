package main

import (
	"context"
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
	for {
		writer.WriteMessages(context.Background(),
			kafka.Message{
				Value: []byte(time.Now().String()),
			},
		)

		log.Println("message produced")
		time.Sleep(2 * time.Second)
	}

}
