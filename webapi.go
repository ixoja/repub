package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	fmt "fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	proto "github.com/golang/protobuf/proto"
	"github.com/gorilla/websocket"
	kafka "github.com/segmentio/kafka-go"
)

var (
	PORT     = "8080"
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

const (
	unknownSession = "Unknown session."
	sessionString  = "session"
	sessionsIndex  = "sessions"
)

func StartWebServer() {
	var HTML string
	if HTML = os.Getenv("HTML"); HTML == "" {
		HTML = "./html"
	}

	err := redisApi.Connect(REDIS_HOST + ":" + redisPort)
	if err != nil {
		log.Println("Could not connect to Redis. Exiting.")
		return
	}

	defer redisApi.Disconnect()

	http.Handle("/", http.FileServer(http.Dir(HTML)))
	http.HandleFunc("/login", Login)
	http.HandleFunc("/getTweets", GetTweets)
	log.Println("Registering web server on port:", PORT)
	log.Fatal(http.ListenAndServe(":"+PORT, nil))
}

func IsKnownSession(sess string) (bool, error) {
	knownSession, err := redisApi.IsInIndex(sessionsIndex, sess)
	if err != nil {
		log.Println("Could not successfully search for a session in Redis.", err)
		return false, err
	}

	return knownSession, nil
}

func Login(w http.ResponseWriter, r *http.Request) {
	sessionCookie, err := r.Cookie(sessionString)

	if err == nil {
		sess := sessionCookie.Value
		knownSession, err := IsKnownSession(sess)
		if err != nil {
			log.Println("Failed to log in a user.")
			http.Error(w, err.Error(), 500)
			return
		}
		if knownSession {
			if _, ok := GetWS(sess); ok {
				warn := "WARN: Trying to log in a user with already session."
				log.Println(warn, "Session id:", sess)
				http.Error(w, warn, 403)
				return
			} else {
				log.Println("Logging in a known user. Session id:", sess)
				return
			}
		}
	}

	log.Println("Starting a new session")
	sess, _ := SessionID()

	cookie := http.Cookie{Name: sessionString,
		Value:   sess,
		Expires: time.Now().Add(time.Duration(time.Hour * 24 * 30))}
	http.SetCookie(w, &cookie)

	err = redisApi.AddToIndex(sessionsIndex, sess)
	if err != nil {
		http.Error(w, err.Error(), 500)
	}
	log.Println("Session ID created: ", sess)
	fmt.Fprint(w, "{OK}")
}

func GetTweets(w http.ResponseWriter, r *http.Request) {
	sessionCookie, err := r.Cookie(sessionString)

	if err != nil {
		log.Println(unknownSession, "You need to log in first.")
		http.Error(w, err.Error(), 403)
		return
	}

	sess := sessionCookie.Value
	knownSession, err := IsKnownSession(sess)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if !knownSession {
		log.Println(unknownSession, sess)
		http.Error(w, unknownSession, 403)
		return
	}

	if _, ok := GetWS(sess); ok {
		warn := "WARN: Call from a user with already session. Session id:"
		log.Println(warn, "Session id:", sess)
		http.Error(w, warn, 403)
		return
	}

	connection, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("WebSocket opening error:", err)
		return
	}

	if c, ok := GetWS(sess); ok && c != nil {
		log.Print("WARN: trying to establish connection for session, which is already connected. Session ID:", sess)
		return
	}
	AddConnection(sess, connection)
	defer CloseConnection(sess)
	kafkaSubscriber.Subscribe(sess, SubscriberCallback)

	ProcessMessages(w, connection, sess)

}

func SessionID() (string, error) {
	b := make([]byte, 16)
	n, err := rand.Read(b)
	if n != len(b) || err != nil {
		return "", fmt.Errorf("Could not successfully read generate a random id")
	}
	return hex.EncodeToString(b), nil
}

func CloseConnection(sess string) {
	connections.Lock()
	connections.m[sess].Close()
	delete(connections.m, sess)
	connections.Unlock()
	log.Println("Connection closed for session ID:", sess)
}

func AddConnection(sess string, connection *websocket.Conn) {
	connections.Lock()
	connections.m[sess] = connection
	connections.Unlock()
}

func ProcessMessages(w http.ResponseWriter, connection *websocket.Conn, sess string) {
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
		pb.Session = sess
		log.Println("Received a subscription event from UI. Session:", pb.GetSession(),
			"Topic:", pb.GetTopic(),
			"Subscribe:", pb.GetSubscribe())

		if bytes, err := proto.Marshal(&pb); err != nil {
			log.Println(err)
			break
		} else {
			err := writer.WriteMessages(context.Background(), kafka.Message{Value: bytes})
			if err != nil {
				log.Println("Could not write to kafka, error:", err)
				break
			}

			log.Println("Subscription event sent to kafka.")
		}
	}
}

func GetWS(sessionID string) (*websocket.Conn, bool) {
	connections.RLock()
	conn, ok := connections.m[sessionID]
	connections.RUnlock()
	return conn, ok
}

func SubscriberCallback(sess string, value []byte, offset int64) bool {
	msg := fmt.Sprintf("message at offset %d: %s\n", offset, string(value))

	conn, ok := GetWS(sess)
	if !ok || conn == nil {
		log.Println("SubscriberCallback: could not find connection for session", sess)
		return false
	}

	err := conn.WriteMessage(websocket.TextMessage, []byte(msg))
	if err != nil {
		log.Println("write:", err)
		return false
	}
	return true
}
