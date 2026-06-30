package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func PublishJSON[T any](ch * amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body: data,
	})
	if err != nil {
		return err
	}
	return nil
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
		d := false
		t := false
		if queueType == Durable {
			d = true
		}
		if queueType ==  Transient {
			t = true
		}
		channel, err := conn.Channel()
		if err != nil {
			return nil, amqp.Queue{}, err
		}
		
		newQueue, err := channel.QueueDeclare(queueName, d, t, t, false, nil)
		if err != nil {
			return nil, newQueue, err
		}

		err = channel.QueueBind(queueName, key, exchange, false, nil)
		if err != nil {
			return nil, newQueue, err
		}
		return channel, newQueue, nil
}