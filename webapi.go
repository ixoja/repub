package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	fmt "fmt"
	"log"
	"net/http"
	"sync"
	"time"

	proto "github.com/golang/protobuf/proto"
	"github.com/gorilla/websocket"
	kafka "github.com/segmentio/kafka-go"
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
	connections = struct {
		sync.RWMutex
		m map[string]*websocket.Conn
	}{m: make(map[string]*websocket.Conn)}
)

const sessionString = "session"

func startWebServer() {
	http.Handle("/", http.FileServer(http.Dir("./html")))
	http.HandleFunc("/login", login)
	http.HandleFunc("/getTweets", getTweets)
	log.Println("Registering web server on:", addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

func ExtractSession(request *http.Request) (string, bool) {
	if sessionCookie, err := request.Cookie(sessionString); err == nil {
		session := sessionCookie.Value
		_, knownSession := GetWS(session)
		if knownSession {
			return session, true
		}
	}

	return "", false
}

func login(respWriter http.ResponseWriter, request *http.Request) {
	if sess, ok := ExtractSession(request); ok {
		log.Println("Logging in a known user. Session id:", sess)
		return
	}

	log.Println("Starting a new session")
	sess, _ := sessionID()

	cookie := http.Cookie{Name: sessionString,
		Value:   sess,
		Expires: time.Now().Add(time.Duration(time.Hour * 24 * 30))}
	http.SetCookie(respWriter, &cookie)

	connections.Lock()
	connections.m[sess] = nil
	connections.Unlock()
	log.Println("Session ID created: ", sess)
	fmt.Fprint(respWriter, "{OK}")
}

func getTweets(respWriter http.ResponseWriter, request *http.Request) {
	sess, ok := ExtractSession(request)
	if !ok {
		log.Print("Unknown session")
	} else {
		connection, err := upgrader.Upgrade(respWriter, request, nil)
		if err != nil {
			log.Print("WebSocket opening error:", err)
			return
		}

		connections.Lock()
		connections.m[sess] = connection
		connections.Unlock()

		defer CloseConnection(sess)

		processMessages(respWriter, connection, sess)
	}
}

func sessionID() (string, error) {
	b := make([]byte, 16)
	n, err := rand.Read(b)
	if n != len(b) || err != nil {
		return "", fmt.Errorf("Could not successfully read generate a random id")
	}
	return hex.EncodeToString(b), nil
}

func CloseConnection(session string) {
	connections.Lock()
	connections.m[session].Close()
	connections.m[session] = nil
	connections.Unlock()
	log.Println("Connection closed for session ID:", session)
}

func processMessages(respWriter http.ResponseWriter, connection *websocket.Conn, sess string) {
	writer := kafka.NewWriter(kafka.WriterConfig{Brokers: brokers,
		Topic: subsEvents})

	defer writer.Close()
	for {
		_, message, err := connection.ReadMessage()
		if err != nil {
			log.Println("Error reading from Kafka:", err)
			break
		}

		var pb SubscriptionEvent
		if err := json.Unmarshal(message, &pb); err != nil {
			log.Println(err)
			break
		}
		log.Println("Received a subscription event. Session:", pb.GetSession(),
			"Topic:", pb.GetTopic(),
			"Subscribe:", pb.GetSubscribe())

		pb.Session = sess
		if bytes, err := proto.Marshal(&pb); err != nil {
			log.Println(err)
			break
		} else {
			writer.WriteMessages(context.Background(), kafka.Message{Value: bytes})
		}

		callback := func(sess string, value []byte, offset int64) {
			msg := fmt.Sprintf("message at offset %d: %s\n", offset, string(value))
			if conn, ok := GetWS(sess); ok {
				if conn != nil {
					err = conn.WriteMessage(websocket.TextMessage, []byte(msg))
					if err != nil {
						log.Println("write:", err)
					}
				} else { /*TODO: add buffering logic for disconnected sessions*/
				}
			}
		}

		go kafkaSubscriber.subscribe(sess, callback)
	}
}

func GetWS(sessionID string) (*websocket.Conn, bool) {
	connections.RLock()
	conn, ok := connections.m[sessionID]
	connections.RUnlock()
	return conn, ok
}
