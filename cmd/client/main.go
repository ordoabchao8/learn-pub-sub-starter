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
	ch, err := connection.Channel()
	if err != nil {
		log.Fatalf("Error creating channel from connection. Error: %v", err)
	}
	defer ch.Close()
	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Error creating username. Error: %s", err)
	}
	
	gameState := gamelogic.NewGameState(userName)
	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilDirect, routing.PauseKey+"."+userName, routing.PauseKey, pubsub.Transient, handlerPause(gameState))
	if err != nil {
		log.Fatalf("Error pausing game. Error: %v", err)
	}
	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+userName, routing.ArmyMovesPrefix+".*", pubsub.Transient, handlerMove(gameState))
	if err != nil {
		log.Fatalf("Error subscribing to move queue. Error: %v", err)
	}
	for {
		userInput := gamelogic.GetInput()
		if len(userInput) == 0 {
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
				continue 
			}
			err = pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+userName, currentMove)
			if err != nil {
				fmt.Println(err)
				return
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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	return func(am gamelogic.ArmyMove) {
		defer fmt.Print("> ")
		gs.HandleMove(am)
	}
}