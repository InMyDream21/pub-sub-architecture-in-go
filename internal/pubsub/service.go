package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	DurableQueueType   SimpleQueueType = "durable"
	TransientQueueType SimpleQueueType = "transient"
)

type AckType string

const (
	AckTypeAck         AckType = "ack"
	AckTypeNackRequeue AckType = "nack"
	AckTypeNackDiscard AckType = "nack_discard"
)

func PublishJSON[T any](ch *amqp091.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}
	if err := ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp091.Publishing{ContentType: "application/json", Body: data}); err != nil {
		return err
	}
	return nil
}

func DeclareAndBind(
	conn *amqp091.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp091.Channel, amqp091.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp091.Queue{}, err
	}
	durable := queueType == DurableQueueType
	queue, err := ch.QueueDeclare(queueName, durable, !durable, !durable, false, amqp091.Table{"x-dead-letter-exchange": routing.ExchangeDeadLetter})
	if err != nil {
		return nil, amqp091.Queue{}, err
	}
	if err := ch.QueueBind(queueName, key, exchange, false, nil); err != nil {
		return nil, amqp091.Queue{}, err
	}
	return ch, queue, nil
}

func subscribe[T any](
	conn *amqp091.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	if err := ch.Qos(10, 0, false); err != nil {
		return err
	}
	msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for msg := range msgs {
			var val T
			if val, err = unmarshaller(msg.Body); err != nil {
				fmt.Println("could not unmarshal message: ", err)
				continue
			}
			ackType := handler(val)
			if ackType == AckTypeNackDiscard {
				fmt.Println("nack discard")
				if err := msg.Nack(false, false); err != nil {
					fmt.Println("could not ack message: ", err)
				}
				continue
			} else if ackType == AckTypeNackRequeue {
				fmt.Println("nack requeue")
				if err := msg.Nack(false, true); err != nil {
					fmt.Println("could not ack message: ", err)
				}
				continue
			}
			fmt.Println("ack")
			if err := msg.Ack(false); err != nil {
				fmt.Println("could not ack message: ", err)
			}
		}
	}()
	return nil
}

func SubscribeJSON[T any](
	conn *amqp091.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe[T](conn, exchange, queueName, key, queueType, handler, func(b []byte) (T, error) {
		var val T
		if err := json.Unmarshal(b, &val); err != nil {
			return val, err
		}
		return val, nil
	})
}

func SubscribeGob[T any](
	conn *amqp091.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	return subscribe[T](conn, exchange, queueName, key, queueType, handler, func(b []byte) (T, error) {
		var val T
		if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&val); err != nil {
			return val, err
		}
		return val, nil
	})
}

func PublishGob[T any](
	ch *amqp091.Channel, exchange, key string, val T,
) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(val); err != nil {
		return err
	}
	return ch.Publish(exchange, key, false, false, amqp091.Publishing{
		ContentType: "application/gob",
		Body:        buf.Bytes(),
	})
}
