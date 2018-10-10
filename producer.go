package main

import (
	"context"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const (
	topicOne = "Topic-1"
	key      = "Key-A"
)

func startProducer() {
	log.Println("Starting the producer")
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Topic:   topicOne,
	})
	defer writer.Close()
	for {
		writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(key),
				Value: []byte(time.Now().String()),
			},
		)

		log.Println("message produced")
		time.Sleep(2 * time.Second)
	}

}
