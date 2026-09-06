package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func sendMessage(producer *kafka.Producer, topic string, msg string) {
	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(msg),
	}, nil)

	if err != nil {
		fmt.Printf("Failed to send message: %s\n", err)
		return
	}
}

func sendSyncMessage(producer *kafka.Producer, topic string, msg string) *kafka.Message {
	delivery := make(chan kafka.Event)
	defer close(delivery)

	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(msg),
	}, delivery)

	if err != nil {
		fmt.Printf("Failed to send sync message: %s\n", err)
		return nil
	}

	event := <-delivery

	return event.(*kafka.Message)
}

func main() {
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
		"acks":              "all",
		"client.id":         "exampleProducer",
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	defer producer.Close()
	fmt.Println("Producer initialized")

	// sendMessage(producer, "async-topic", "test async message")
	// producer.Flush(5000) // Продюсер не успевает отправить брокеру сообщение, если приложение не является сервером.

	// go func() {
	// 	for e := range producer.Events() {
	// 		switch ev := e.(type) {
	// 		case *kafka.Message:
	// 			if ev.TopicPartition.Error != nil {
	// 				fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition.Error)
	// 			} else {
	// 				fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
	// 			}
	// 		}
	// 	}
	// }()

	m := sendSyncMessage(producer, "metrics", "{\"name\":\"cpu\",\"value\":15}")

	if m.TopicPartition.Error != nil {
		fmt.Printf("Failed to deliver sync message: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
}
