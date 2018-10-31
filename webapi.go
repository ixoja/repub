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

const sessionString = "session"
const sessionsIndex = "sessions"

func StartWebServer() {
	var HTML string
	if HTML = os.Getenv("HTML"); HTML == "" {
		HTML = "./html"
	}

	TryToRestoreWebapi()
	http.Handle("/", http.FileServer(http.Dir(HTML)))
	http.HandleFunc("/login", Login)
	http.HandleFunc("/getTweets", GetTweets)
	log.Println("Registering web server on port:", PORT)
	log.Fatal(http.ListenAndServe(":"+PORT, nil))
}

func ExtractSession(request *http.Request) (string, bool) {
	if sessionCookie, err := request.Cookie(sessionString); err == nil {
		sess := sessionCookie.Value
		_, knownSession := GetWS(sess)
		if knownSession {
			return sess, true
		}
	}

	return "", false
}

func Login(respWriter http.ResponseWriter, request *http.Request) {
	if sess, ok := ExtractSession(request); ok {
		log.Println("Logging in a known user. Session id:", sess)
		return
	}

	log.Println("Starting a new session")
	sess, _ := SessionID()

	cookie := http.Cookie{Name: sessionString,
		Value:   sess,
		Expires: time.Now().Add(time.Duration(time.Hour * 24 * 30))}
	http.SetCookie(respWriter, &cookie)

	AddConnection(sess, nil)
	log.Println("Session ID created: ", sess)
	fmt.Fprint(respWriter, "{OK}")
}

func GetTweets(respWriter http.ResponseWriter, request *http.Request) {
	sess, ok := ExtractSession(request)
	if !ok {
		log.Print("Unknown session")
		return
	} else {
		connection, err := upgrader.Upgrade(respWriter, request, nil)
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
		go kafkaSubscriber.Subscribe(sess, SubscriberCallback)

		ProcessMessages(respWriter, connection, sess)
	}
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
	connections.m[sess] = nil
	connections.Unlock()
	log.Println("Connection closed for session ID:", sess)
}

func AddConnection(sess string, connection *websocket.Conn) {
	connections.Lock()
	connections.m[sess] = connection
	connections.Unlock()
}

func ProcessMessages(respWriter http.ResponseWriter, connection *websocket.Conn, sess string) {
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
			writer.WriteMessages(context.Background(), kafka.Message{Value: bytes})
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

func TryToRestoreWebapi() error {
	redisApi := RedisApi{}
	err := redisApi.Connect(REDIS_HOST + ":" + redisPort)
	if err != nil {
		return err
	}

	defer redisApi.Disconnect()

	sessions, err := redisApi.GetIndex(sessionsIndex)
	if err != nil {
		return err
	}
	for _, sess := range sessions {
		AddConnection(sess, nil)
	}

	return nil
}
