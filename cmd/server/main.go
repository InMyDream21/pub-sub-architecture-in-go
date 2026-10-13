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

	channel, err := conn.Channel()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer channel.Close()

	ch, _, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, "game_logs", "game_logs.*", pubsub.DurableQueueType)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer ch.Close()

	err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Connection Established")
	gamelogic.PrintServerHelp()
	for {
		words := gamelogic.GetInput()
		if words == nil {
			continue
		}
		if words[0] == "pause" {
			fmt.Println("Pausing...")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				fmt.Println(err)
				return
			}
			continue
		}
		if words[0] == "resume" {
			fmt.Println("Resuming...")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				fmt.Println(err)
				return
			}
			continue
		}
		if words[0] == "quit" {
			fmt.Println("Quitting...")
			return
		}
		fmt.Println("Unknown command:", words[0])
	}
}
