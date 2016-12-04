package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
)

const (
	TOPIC = "twitter"
)

var producer *kafka.Producer

func openProducer() {
	var err error
	producer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		fmt.Println("Could not connect to producer")
		os.Exit(1)
	}
	fmt.Println("Created producer")
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}

			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}

	}()
}

func closeProducer() {
	producer.Close()
}

func openTwitterStream(jsonConfig map[string]string) *twitter.Stream {
	config := oauth1.NewConfig(jsonConfig["cKey"], jsonConfig["cTok"])
	token := oauth1.NewToken(jsonConfig["aKey"], jsonConfig["aTok"])
	httpClient := config.Client(oauth1.NoContext, token)

	client := twitter.NewClient(httpClient)
	fmt.Println("Created client")

	// create stream
	params := &twitter.StreamFilterParams{
		Track:         []string{"climate"},
		StallWarnings: twitter.Bool(true),
	}

	stream, err := client.Streams.Filter(params)
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
	fmt.Println("Created stream")

	// create demultiplexer
	demux := twitter.NewSwitchDemux()
	demux.Tweet = func(tweet *twitter.Tweet) {
		fmt.Println("Tweet", tweet.Text)
		topic := string(TOPIC)
		producer.ProduceChannel() <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(tweet.Text),
		}
	}
	demux.DM = func(dm *twitter.DirectMessage) {
		fmt.Println("DM", dm.SenderID)
	}
	go demux.HandleChan(stream.Messages)
	fmt.Println("Setup demux")
	return stream
}

func main() {
	var jsonConfig map[string]string
	if data, err := ioutil.ReadFile("config.json"); err != nil {
		fmt.Println("Error opening file", err)
	} else {
		json.Unmarshal(data, &jsonConfig)
		fmt.Println(jsonConfig)
	}

	openProducer()
	defer closeProducer()

	stream := openTwitterStream(jsonConfig)
	defer stream.Stop()

	// Wait for SIGINT and SIGTERM (HIT CTRL-C)
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println(<-ch)
	fmt.Println("Quitting")
}
