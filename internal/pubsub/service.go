package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	DurableQueueType   SimpleQueueType = "durable"
	TransientQueueType SimpleQueueType = "transient"
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
	queue, err := ch.QueueDeclare(queueName, durable, !durable, !durable, false, nil)
	if err != nil {
		return nil, amqp091.Queue{}, err
	}
	if err := ch.QueueBind(queueName, key, exchange, false, nil); err != nil {
		return nil, amqp091.Queue{}, err
	}
	return ch, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp091.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}
	msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		for msg := range msgs {
			var val T
			if err := json.Unmarshal(msg.Body, &val); err != nil {
				fmt.Println("could not unmarshal message: ", err)
				continue
			}
			handler(val)
			if err := msg.Ack(false); err != nil {
				fmt.Println("could not ack message: ", err)
			}
		}
	}()
	return nil
}
