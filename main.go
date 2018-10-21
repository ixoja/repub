package main

import (
	"flag"
	"log"
	"sync"
)

var (
	brokers         = []string{"localhost:9092"}
	wg              sync.WaitGroup
	kafkaSubscriber = NewSubscriber()
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
		StartWebServer()
	case server:
		StartServer()
	case producer:
		StartProducer()
	default:
		log.Fatal("Not recognized mode. Please set mode to webapi, server or producer.")
	}

	wg.Wait()
}
