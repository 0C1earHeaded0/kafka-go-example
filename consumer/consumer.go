package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	env "github.com/joho/godotenv"
	"github.com/lib/pq"
)

func processMsgMock(msg *kafka.Message) {
	fmt.Println("Processing message...")
	time.Sleep(3 * time.Second)
	fmt.Printf("Message processed. Value: %s\n", msg.Value)
}

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

	// TODO: Обработать отсутствие переменных окружения.
	dbConfig := pq.Config{
		Host: os.Getenv("TRANSACTION_DB_HOST"),
		Port: 5400, // Сделать нормальную загрузку с переменных окружения.
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
			processMsgMock(e) // Обработка сообщения до фиксации смещения

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
