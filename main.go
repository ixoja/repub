package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

var (
	addr     = flag.String("addr", "localhost:8080", "http service address")
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	brokers     = []string{"localhost:9092"}
	connections = struct {
		sync.RWMutex
		m map[string]*websocket.Conn
	}{m: make(map[string]*websocket.Conn)}
)

const (
	subscriptions = "subscriptions"
	webapi        = "webapi"
	server        = "server"
	producer      = "producer"
	subsEvents    = "SubscriptionEvents"
)

func main() {
	log.SetFlags(0)
	log.Println("Starting...")

	modePtr := flag.String("mode", webapi, "string value: webapi, server or producer")
	flag.Parse()
	mode := *modePtr

	switch mode {
	case webapi:
		http.HandleFunc("/login", login)
		http.HandleFunc("/getTweets", getTweets)
		log.Fatal(http.ListenAndServe(*addr, nil))
	case server:
		startServer()
	case producer:
		startProducer()
	default:
		log.Fatal("Not recognized mode. Please set mode to webapi, server or producer.")
	}
}

func subscribe(topic string, callback func(string, []byte, int64)) {
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	defer kafkaReader.Close()

	for {
		m, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			log.Println("Error reading Kafka message: ", err)
			break
		}

		callback(topic, m.Value, m.Offset)
	}
}
