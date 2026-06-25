package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	const connectionString = "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Error creating connection to rabbitMQ. Error:  %s", err)
	}
	defer connection.Close()
	fmt.Println("rabbitMQ connection successful")
	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
 	<-signalChan
	fmt.Println("\nStop signal recieved. RabbitMQ shutting down.")

}
