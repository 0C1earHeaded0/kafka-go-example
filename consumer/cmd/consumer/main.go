package main

import (
	"context"
	"fmt"
	"kafka-go-example-consumer/storage/postgresql"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	env "github.com/joho/godotenv"
)

func main() {
	err := env.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
		"group.id":          "testGroup",
		"auto.offset.reset": "earliest",
	}

	storage, err := postgresql.NewStorage()
	if err != nil {
		panic(fmt.Sprintf("Failed to create storage: %v", err))
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		panic(fmt.Sprintf("Failed to create consumer: %v", err))
	}

	err = consumer.SubscribeTopics([]string{"metrics"}, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to subscribe to topic: %v", err))
	}

	fmt.Println("Consumer initialized")

	ctx := context.Background()

	for run := true; run == true; {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			id := fmt.Sprintf("%s-%d-%s", *e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset.String())
			res, err := storage.Save(ctx, id, string(e.Value))
			if err != nil {
				panic(fmt.Sprintf("Failed to save metric: %v", err))
			}
			fmt.Printf("Metric saved: %s\n", res)

			// panic("Netw problems.")

			_, err = consumer.CommitMessage(e)
			if err != nil {
				fmt.Printf("Commit message failed with error: %v\n", err)
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
