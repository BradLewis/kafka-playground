package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func launchWorker(id string) {
	topic := "my-topic"
	var reader *kafka.Reader
	if id == "3" {
		reader = kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:  []string{"localhost:50321"},
				Topic:    topic,
				MaxBytes: 10e6,
				GroupID:  "my-group-2",
				MaxWait:  1 * time.Second,
			})
	} else {
		reader = kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:  []string{"localhost:50321"},
				Topic:    topic,
				MaxBytes: 10e6,
				GroupID:  "my-group",
				MaxWait:  1 * time.Second,
			})
	}
	defer reader.Close()

	fmt.Printf("Worker %s is listening\n", id)
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("error reading message", err)
		}
		log.Printf(
			" - %s - message at topic/partition/offset %v/%v/%v: %s = %s\n",
			id,
			m.Topic,
			m.Partition,
			m.Offset,
			string(m.Key),
			string(m.Value),
		)
	}
}

func main() {
	go launchWorker("1")
	go launchWorker("2")
	launchWorker("3")
}
