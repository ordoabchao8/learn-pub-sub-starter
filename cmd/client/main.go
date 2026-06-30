package main

import (
	"fmt"
	"log"
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
	_, pauseQueue, err := pubsub.DeclareAndBind(connection, routing.ExchangePerilDirect, routing.PauseKey+ "." + userName, routing.PauseKey, pubsub.Transient)
	if err != nil {
		log.Fatalf("Error binding queue to exchange. Error: %s", err)
	}
	fmt.Println(pauseQueue.Name)
	gameState := gamelogic.NewGameState(userName)
	for {
		userInput := gamelogic.GetInput()
		if userInput == nil {
			continue
		}
		switch userInput[0] {
		case "spawn":
			err = gameState.CommandSpawn(userInput)
			if err != nil {
				fmt.Println(err)
			}
		case "move":
			currentMove, err := gameState.CommandMove(userInput)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Printf("Successfully moved unit! ArmyMove:  %v", currentMove)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command try again")
			continue
		}
	}
	// wait for ctrl+c
	//signalChan := make(chan os.Signal, 1)
	//signal.Notify(signalChan, os.Interrupt)
 	//<-signalChan

	//fmt.Println("\nStop signal recieved. RabbitMQ shutting down.")
}
