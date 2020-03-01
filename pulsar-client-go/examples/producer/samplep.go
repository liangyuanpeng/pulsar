package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
)

func main() {
	topic := "persistent://public/default/some_partition_topic"

	// Instantiate a Pulsar client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	if err != nil {
		log.Fatal(err)
	}

	// Use the client to instantiate a producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})

	if err != nil {
		log.Fatal(err)
	}

	// Use the client object to instantiate a consumer
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "subName",
		Type:             pulsar.Shared,
		AckTimeout:       60 * time.Second,
	})

	ctx := context.Background()
	// Send 10 messages synchronously and 10 messages asynchronously
	for i := 0; i < 5; i++ {
		// Create a message
		msg := pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("message-%d", i)),
		}
		// Attempt to send the message
		if err := producer.Send(ctx, msg); err != nil {
			log.Fatal(err)
		}
	}

	// Listen indefinitely on the topic
	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			log.Fatal(err)
		}
		// Do something with the message
		fmt.Println("" + time.Now().Format(time.RFC850) + " consume " + string(msg.Payload()))
		//consumer.Ack(msg)
	}
}
