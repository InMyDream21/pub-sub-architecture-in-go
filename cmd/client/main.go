package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	const RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
	conn, err := amqp091.Dial(RABBITMQ_URL)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Welcome,", username)
	state := gamelogic.NewGameState(username)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, pubsub.TransientQueueType, handlerPause(state))
	if err != nil {
		fmt.Println(err)
		return
	}
	for {
		words := gamelogic.GetInput()
		if words == nil {
			continue
		}
		if words[0] == "spawn" {
			err := state.CommandSpawn(words)
			if err != nil {
				fmt.Println(err)
			}
			continue
		}
		if words[0] == "move" {
			move, err := state.CommandMove(words)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(move)
			continue
		}
		if words[0] == "status" {
			state.CommandStatus()
			continue
		}
		if words[0] == "help" {
			gamelogic.PrintClientHelp()
			continue
		}
		if words[0] == "spam" {
			fmt.Println("Spamming not allowed yet!")
			continue
		}
		if words[0] == "quit" {
			gamelogic.PrintQuit()
			return
		}
		fmt.Println("Unknown command:", words[0])
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
	}
}
