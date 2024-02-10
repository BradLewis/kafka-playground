package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
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
	defer fmt.Printf("Worker %s is done\n", id)
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		reader.Close()
		fmt.Printf("Worker %s is done\n", id)

		os.Exit(1)
	}()

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
	// launchWorker(os.Args[1])
	startGoroutineWorkers()

}

func startGoroutineWorkers() {
	topic := "my-topic"
	reader := kafka.NewReader(
		kafka.ReaderConfig{
			Brokers:  []string{"localhost:51451"},
			Topic:    topic,
			MaxBytes: 10e6,
			GroupID:  "my-group-2",
			MaxWait:  1 * time.Second,
		})
	defer reader.Close()
	chanMap := make(map[int]chan *kafka.Message)
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("error reading message", err)
		}
		if _, ok := chanMap[m.Partition]; !ok {
			log.Printf("Creating new channel for partition %d\n", m.Partition)
			chanMap[m.Partition] = make(chan *kafka.Message, 256)
			go partitionConsumer(fmt.Sprintf("Partition %d", m.Partition), chanMap[m.Partition])
		}
		chanMap[m.Partition] <- &m
	}
}

func partitionConsumer(name string, messageChan chan *kafka.Message) {
	for message := range messageChan {
		log.Printf(
			"name: %s - message at topic/partition/offset %v/%v/%v: %s = %s\n",
			name,
			message.Topic,
			message.Partition,
			message.Offset,
			string(message.Key),
			string(message.Value),
		)
	}
}
