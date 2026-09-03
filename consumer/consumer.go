package main

import (
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const MIN_COMMIT_COUNT = 2

func processMsgMock(msg *kafka.Message) {
	fmt.Println("Processing message...")
	time.Sleep(3 * time.Second)
	fmt.Printf("Message processed. Value: %s\n", msg.Value)
}

func main() {
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
		"group.id":          "testGroup",
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer: %v", err))
	}

	err = consumer.SubscribeTopics([]string{"async-topic", "sync-topic"}, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to subscribe to topic: %v", err))
	}

	fmt.Println("Consumer initialized")

	for run := true; run == true; {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			_, err = consumer.CommitMessage(e) // Фиксация смещения до обработки сообщения.
			if err == nil {
				processMsgMock(e)
			}
		case kafka.PartitionEOF:
			fmt.Printf("%% Reached %v\n", e)
		case kafka.OffsetsCommitted:
			fmt.Printf("%% Offsets Commited %v\n", e)
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			run = false
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}

	consumer.Close()
}
