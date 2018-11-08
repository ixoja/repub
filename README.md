# repub
repub stands for "re-publisher". It is a pet project written by Ihor Khodzhaniiazov to try such languages, tools and technologies as:
- Go
- Kafka 
- Redis
- Docker
- Protobuf
- Websocket
- Pub/Sub approach in general

The idea was to create a feed aggregator which would allow user to asynchronously subscribe on topics (like, in Twitter, Instagram, etc.) and receive updates in real time. The project contains of such components:
- Web UI, with two functions: subscribe on a topic, and unsubscribe. Both actions result in sending JSON messages to API.
- Web API, which receives sub/unsub messages from UI, transforms JSON to Protobuf and sends to Kafka. Each session is stored in cookies and in Redis. 
- Server, where the main logic is implemented. It receives Subscription Events as Protobuf messages from kafka and according to them, either subscribes a session on a topic, or unsubscribes it. When a topic is updated, the server sends the update to kafka, for all the sessions subscribed.
- Redis, as a storage to index sessions and topics and to store correspondence sessions->topics and topics->sessions.
- Kafka, as a medium for Subscription Events running API->Server and updates on subscribd topics running Server->API. 
- Also a mock producer, which sends updates to kafka, to imitate a media activity. The server subscribes to such updates to imitate subscription to a media API.

# Docker
To run the project inside Docker I build it as a single executable, using the parameters: _CGO_ENABLED=0 GOOS=linux_
The build command looks like this: `go build -a -installsuffix cgo -o main .`
After the executable is ready I just call `docker-compose up` and it puts all repub components into docker containers according to yml and creates a network for interaction.


# TODOs
- implement session expiration
- add scalability by supporting multi-node api and server (rewrite session management)
- add support of different data sources