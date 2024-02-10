package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

type Data struct {
	Timestamp  time.Time
	Value      float64
	MetricName string
}

func main() {
	w := &kafka.Writer{
		Addr:            kafka.TCP("localhost:51451"),
		Topic:           "my-topic",
		Balancer:        &kafka.Hash{},
		WriteBackoffMax: 100 * time.Millisecond,
		Async:           true,
	}
	ticker := time.NewTicker(2000 * time.Millisecond)

	for {
		select {

		case <-ticker.C:
			log.Println("writing messages")
			data := Data{
				Timestamp:  time.Now(),
				Value:      rand.Float64(),
				MetricName: "my-metric",
			}
			dataBytes, err := json.Marshal(data)
			if err != nil {
				log.Printf("failed to marshal data: %v", err)
				continue
			}
			err = w.WriteMessages(context.Background(),
				kafka.Message{
					Key:   []byte("Key-A"),
					Value: []byte("Hello World!"),
				},
				kafka.Message{
					Key:   []byte("Key-B"),
					Value: []byte("One!"),
				},
				kafka.Message{
					Key:   []byte("Key-C"),
					Value: []byte("Two!"),
				},
				kafka.Message{
					Key:   []byte("Key-D"),
					Value: []byte("abcdefg"),
				},
				kafka.Message{
					Key:   []byte("Key-E"),
					Value: dataBytes,
				},
			)
			if err != nil {
				log.Fatal("failed to write messages:", err)
			}
			log.Println("wrote messages")

		}
	}
	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
