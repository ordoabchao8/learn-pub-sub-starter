package main

import (
	"fmt"
	"log"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	
)

func main() {
	fmt.Println("Starting Peril server...")
	const connectionString = "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Error creating connection to rabbitMQ. Error:  %s", err)
	}
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("Error creating channel from connection. Error :%s", err)
	}
	fmt.Println("rabbitMQ connection successful")


	fmt.Println("\nStop signal recieved. RabbitMQ shutting down.")

	gamelogic.PrintServerHelp()
	for {
		userInput := gamelogic.GetInput()
		if userInput == nil {
			continue
		}
		switch userInput[0] {
		case "pause":
			log.Println("Sending pause message")
			pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})

		case "resume":
			log.Println("Sending resume message")
			pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
		case "quit":
			log.Println("Exiting")
			return
		default:
			log.Println("Command not available. Please try again.")
		}
	}
}
