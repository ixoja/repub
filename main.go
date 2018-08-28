package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
)

var addr = flag.String("addr", "localhost:8080", "http service address")

var upgrader = websocket.Upgrader{} // use default options

func main() {
	//client := login()
	log.SetFlags(0)
	log.Println("Starting...")
	http.HandleFunc("/getTweets", getTweets)
	log.Fatal(http.ListenAndServe(*addr, nil))
	//tweetsListener(client)
}

func getTweets(w http.ResponseWriter, r *http.Request) {
	log.Println("got request:", r.Body)
	message := readFile("C:\\Go\\json.json")
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	defer c.Close()
	for {
		err = c.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			log.Println("write:", err)
			break
		}
	}
}

func readFile(filename string) []byte {
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
}

func login() *twitter.Client {
	consumerKey := os.Getenv("CONSUMER_KEY")
	consumerSecret := os.Getenv("CONSUMER_SECRET")
	accessToken := os.Getenv("ACCESS_TOKEN")
	accessSecret := os.Getenv("ACCESS_SECRET")

	// boilerplate for go-twitter
	config := oauth1.NewConfig(consumerKey, consumerSecret)
	token := oauth1.NewToken(accessToken, accessSecret)

	httpClient := config.Client(oauth1.NoContext, token)
	client := twitter.NewClient(httpClient)

	verifyParams := &twitter.AccountVerifyParams{
		SkipStatus:   twitter.Bool(true),
		IncludeEmail: twitter.Bool(true),
	}
	_, _, err := client.Accounts.VerifyCredentials(verifyParams)
	if err != nil {
		fmt.Println("WOAH ERROR WITH CREDENTIALS")
		panic(err)
	}
	return client
}

func tweetsListener(client *twitter.Client) {
	params := &twitter.SearchTweetParams{
		Query:      "#golang",
		Count:      5,
		ResultType: "recent",
		Lang:       "en",
	}

	for {
		searchResult, _, _ := client.Search.Tweets(params)
		tweets := searchResult.Statuses
		fmt.Printf("Found %v Tweets\n", len(tweets))
		sinceIDs := make([]int64, 0)
		for _, tweet := range tweets {
			sinceIDs = append(sinceIDs, tweet.ID)
		}
		if len(tweets) > 0 {
			finalSinceID := sinceIDs[0]
			for _, sinceID := range sinceIDs {
				if finalSinceID < sinceID {
					finalSinceID = sinceID
				}
			}
			params.SinceID = finalSinceID
		}
		time.Sleep(60 * time.Second)
		fmt.Printf("Begin again with SinceID %v \n", params.SinceID)
	}
}
