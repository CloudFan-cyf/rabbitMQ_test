package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnErr(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func bodyFrom(args []string) string {
	var s string
	if (len(args) < 3) || os.Args[2] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[2:], " ")
	}
	return s
}

func severityFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "anonyous.info"
	} else {
		s = os.Args[1]
	}
	return s
}

func main() {
	// Create a connection to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnErr(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// Create a channel, which is where most of the API for getting things done resides
	ch, err := conn.Channel()
	failOnErr(err, "Failed to open a channel")
	defer ch.Close()

	// Declare a Exchange, fanout exchange broadcasts all the messages it receives to all the queues it knows
	// Direct exchange delivers messages to queues based on the message routing key
	// Topic exchange delivers messages to queues based on the message routing key and a pattern that the queue defines
	// routing key is a message attribute the exchange looks at when deciding how to route the message to queues, it is like an address for the message.Such as "stock.usd.nyse" or "nyse.vmw"
	// * (star) can substitute for exactly one word.
	// # (hash) can substitute for zero or more words.
	err = ch.ExchangeDeclare(
		"logs_topic", // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnErr(err, "Failed to declare an exchange")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	body := bodyFrom(os.Args)
	err = ch.PublishWithContext(
		ctx,                   // context
		"logs_topic",          // exchange
		severityFrom(os.Args), // routing key
		false,                 // mandatory
		false,                 // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnErr(err, "Failed to publish a message")

	log.Printf(" [x] Published %s", body)

}
