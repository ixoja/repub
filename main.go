package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/gorilla/websocket"
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
	brokers = []string{"localhost:9092"}
)

const (
	consumer = "consumer"
	producer = "producer"
	topic    = "Topic-1"
)

func startProducer() {
	log.Println("Starting the producer")
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Topic:   topic,
	})
	defer writer.Close()
	for {
		writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte("Key-A"),
				Value: []byte(time.Now().String()),
			},
		)

		log.Println("message produced")
		time.Sleep(2 * time.Second)
	}

}

func main() {
	log.SetFlags(0)
	log.Println("Starting...")

	modePtr := flag.String("mode", consumer, "string value: consumer or producer")
	flag.Parse()
	mode := *modePtr
	switch mode {
	case consumer:
		http.HandleFunc("/getTweets", getTweets)
		log.Fatal(http.ListenAndServe(*addr, nil))
	case producer:
		startProducer()
	default:
		log.Fatal("Not recognized mode. Please set mode to consumer or producer.")
	}
}

func getTweets(w http.ResponseWriter, request *http.Request) {
	connection, err := upgrader.Upgrade(w, request, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	defer connection.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	reader.SetOffset(42)

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			break
		}
		messageString := fmt.Sprintf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))

		err = connection.WriteMessage(websocket.TextMessage, []byte(messageString))
		if err != nil {
			log.Println("write:", err)
		}
	}

	reader.Close()
}

/* func readFile(filename string) []byte {
	log.Println("Opening the file ", filename)
	// Open our jsonFile
	jsonFile, err := os.Open(filename)
	// if we os.Open returns an error then handle it
	if err != nil {
		fmt.Println(err)
	}
	log.Println("Successfully Opened ", filename)
	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)
	return byteValue
} */
