package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/astaxie/beego/session"
	_ "github.com/astaxie/beego/session/redis"
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
	brokers        = []string{"localhost:9092"}
	globalSessions *session.Manager
)

const (
	subscriptions = "subscriptions"
	consumer      = "consumer"
	producer      = "producer"
	topicOne      = "Topic-1"
	key           = "Key-A"
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

func main() {
	log.SetFlags(0)
	log.Println("Starting...")

	modePtr := flag.String("mode", consumer, "string value: consumer or producer")
	flag.Parse()
	mode := *modePtr

	switch mode {
	case consumer:
		sessionConfig := &session.ManagerConfig{
			CookieName:     "gosessionid",
			Gclifetime:     3600,
			ProviderConfig: "127.0.0.1:6379,100",
		}
		globalSessions, _ = session.NewManager("redis", sessionConfig)
		go globalSessions.GC()

		http.HandleFunc("/getTweets", getTweets)
		log.Fatal(http.ListenAndServe(*addr, nil))
	case producer:
		startProducer()
	default:
		log.Fatal("Not recognized mode. Please set mode to consumer or producer.")
	}
}

func getTweets(w http.ResponseWriter, request *http.Request) {
	sess, _ := globalSessions.SessionStart(w, request)
	sess.Set(subscriptions, make(map[string]interface{}))
	defer sess.SessionRelease(w)

	connection, err := upgrader.Upgrade(w, request, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	defer connection.Close()

	callback := func(msg string) {
		err = connection.WriteMessage(websocket.TextMessage, []byte(msg))
		if err != nil {
			log.Println("write:", err)
		}
	}

	for {
		_, message, err := connection.ReadMessage()
		if err != nil {
			log.Println("read:", err)
			break
		}

		var dat map[string]interface{}
		if err := json.Unmarshal(message, &dat); err != nil {
			panic(err)
		}

		if shouldSubscribe, ok := dat["subscribe"].(bool); ok {
			topic := dat["topic"].(string)
			subsMap := sess.Get(subscriptions).(map[string]interface{})
			if shouldSubscribe {
				if _, alreadySubscribed := subsMap[topic]; !alreadySubscribed {
					go subscribe(topic, sess, callback)
				}
			} else {
				delete(subsMap, topic)
			}
		}
	}
}

func subscribe(topic string, session session.Store, callback func(string)) {
	subsMap := session.Get(subscriptions).(map[string]interface{})
	subsMap[topic] = nil

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
			break
		}

		if _, stillSubscribed := subsMap[topic]; stillSubscribed {
			messageString := fmt.Sprintf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))

			if err != nil {
				log.Println("write:", err)
				break
			}

			callback(messageString)
		}
	}
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
