package main

import (
	"flag"
	"log"
	"os"
	"sync"
)

var (
	KAFKA_HOST      string
	REDIS_HOST      string
	brokers         []string
	wg              sync.WaitGroup
	kafkaSubscriber = NewSubscriber()
	redisApi        = RedisApi{}
)

const (
	subscriptions = "subscriptions"
	webapi        = "webapi"
	server        = "server"
	producer      = "producer"
	subsEvents    = "SubscriptionEvents"
	redisPort     = "6379"
)

func main() {
	log.SetFlags(0)
	log.Println("Starting...")
	if KAFKA_HOST = os.Getenv("KAFKA_HOST"); KAFKA_HOST == "" {
		KAFKA_HOST = "localhost"
	}
	if REDIS_HOST = os.Getenv("REDIS_HOST"); REDIS_HOST == "" {
		REDIS_HOST = "localhost"
	}

	brokers = []string{KAFKA_HOST + ":9092"}

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
