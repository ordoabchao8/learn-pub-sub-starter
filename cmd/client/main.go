package main

import (
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	//Create a connection to rabbitMQ
	const connectionString = "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Error creating connection to rabbitMQ. Error:  %s", err)
	}
	defer connection.Close()
	fmt.Println("rabbitMQ connection successful")
	// Open a channel from the connection 
	ch, err := connection.Channel()
	if err != nil {
		log.Fatalf("Error creating channel from connection. Error: %v", err)
	}
	defer ch.Close()
	// Get a username for player
	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Error creating username. Error: %s", err)
	}
	//Create a new game state
	gameState := gamelogic.NewGameState(userName)
	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilDirect, routing.PauseKey+"."+userName, routing.PauseKey, pubsub.Transient, handlerPause(gameState))
	if err != nil {
		log.Fatalf("Error pausing or resuming game. Error: %v", err)
	}
	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+userName, routing.ArmyMovesPrefix+".*", pubsub.Transient, handlerMove(gameState, ch))
	if err != nil {
		log.Fatalf("Error subscribing to move queue. Error: %v", err)
	}

	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix, routing.WarRecognitionsPrefix+".*", pubsub.Durable, handlerWar(gameState, ch))
	if err != nil {
		log.Fatalf("Error subscribing to war queue. Error: %v", err)
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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(ps routing.PlayingState) pubsub.Acktype{
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(move gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		moveOutcome := gs.HandleMove(move)
		switch moveOutcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix + "." + gs.Player.Username, gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackDiscard
		}	
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(war gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(war)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			message := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := handlerPublishGameLog(gs, ch, message)
			if err != nil {
				fmt.Printf("unable to publish gob data in handlerPublishGameLogs. Error: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			message := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := handlerPublishGameLog(gs, ch, message)
			if err != nil {
				fmt.Printf("unable to publish gob data in handlerPublishGameLogs. Error: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			message := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			err := handlerPublishGameLog(gs, ch, message)
			if err != nil {
				fmt.Printf("unable to publish gob data in handlerPublishGameLogs. Error: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Printf("Unable to declare war, recognition of war failed during outcome processing. %v", outcome)
			return pubsub.NackDiscard
		}
	}
}

func handlerPublishGameLog(gs *gamelogic.GameState, ch *amqp.Channel, msg string) error {
	username := gs.GetUsername()
	exchange := routing.ExchangePerilTopic
	routing_key := routing.GameLogSlug+"."+username
	err := pubsub.PublishGob(ch, exchange, routing_key, routing.GameLog{
		CurrentTime: time.Now(),
		Message: msg,
		Username: username,
	})
	if err != nil {
		return err
	}
	return nil
}