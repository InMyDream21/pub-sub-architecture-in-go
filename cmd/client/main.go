package main

import (
	"fmt"
	"strconv"
	"time"

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
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, routing.ArmyMovesPrefix+".*", pubsub.TransientQueueType, handlerMove(state, channel))
	if err != nil {
		fmt.Println(err)
		return
	}
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix, routing.WarRecognitionsPrefix+".*", pubsub.DurableQueueType, handlerWar(state, channel))
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
			err = pubsub.PublishJSON(channel, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, move)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("Move Published")
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
			if len(words) < 2 {
				fmt.Println("Usage: spam <message>")
				continue
			}
			count, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Println(err)
				continue
			}
			for range count {
				message := gamelogic.GetMaliciousLog()
				pubsub.PublishGob(channel, routing.ExchangePerilTopic, routing.GameLogSlug+"."+username, routing.GameLog{
					CurrentTime: time.Now(),
					Message:     message,
					Username:    username,
				})
			}
			continue
		}
		if words[0] == "quit" {
			gamelogic.PrintQuit()
			return
		}
		fmt.Println("Unknown command:", words[0])
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.AckTypeAck
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp091.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(m gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(m)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.AckTypeAck
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix+"."+m.Player.Username, gamelogic.RecognitionOfWar{
				Attacker: m.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				return pubsub.AckTypeNackRequeue
			}
			return pubsub.AckTypeAck
		}
		return pubsub.AckTypeNackDiscard
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp091.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(w gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(w)
		var message string
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.AckTypeNackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.AckTypeNackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			message = fmt.Sprintf("%s won a war against %s\n", winner, loser)
			if err := publishWarResult(ch, message, routing.GameLog{
				CurrentTime: time.Now(),
				Message:     message,
				Username:    w.Attacker.Username,
			}); err != nil {
				return pubsub.AckTypeNackRequeue
			}
			return pubsub.AckTypeAck
		case gamelogic.WarOutcomeYouWon:
			message = fmt.Sprintf("%s won a war against %s\n", winner, loser)
			if err := publishWarResult(ch, message, routing.GameLog{
				CurrentTime: time.Now(),
				Message:     message,
				Username:    w.Defender.Username,
			}); err != nil {
				return pubsub.AckTypeNackRequeue
			}
			return pubsub.AckTypeAck
		case gamelogic.WarOutcomeDraw:
			message = fmt.Sprintf("A war between %s and %s resulted in a draw\n", winner, loser)
			if err := publishWarResult(ch, message, routing.GameLog{
				CurrentTime: time.Now(),
				Message:     message,
				Username:    w.Attacker.Username,
			}); err != nil {
				return pubsub.AckTypeNackRequeue
			}
			return pubsub.AckTypeAck
		}
		fmt.Println("error")
		return pubsub.AckTypeNackDiscard
	}
}

func publishWarResult(ch *amqp091.Channel, username string, data routing.GameLog) error {
	err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, routing.GameLogSlug+"."+username, data)
	return err
}
