package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	

)

func main() {
	fmt.Println("Starting Peril client...")
	const connectionString = "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Error creating connection to rabbitMQ. Error:  %s", err)
	}
	defer connection.Close()
	fmt.Println("rabbitMQ connection successful")

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Error creating username. Error: %s", err)
	}
	// Channel here is ignored for now!!
	_, queue, err := pubsub.DeclareAndBind(connection, routing.ExchangePerilDirect, routing.PauseKey+ "." + userName, routing.PauseKey, pubsub.Transient)
	if err != nil {
		log.Fatalf("Error binding queue to exchange. Error: %s", err)
	}
	fmt.Println(queue.Name)



	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
 	<-signalChan

	fmt.Println("\nStop signal recieved. RabbitMQ shutting down.")
}
