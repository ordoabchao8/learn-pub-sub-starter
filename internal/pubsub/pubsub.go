package pubsub

import (
	"context"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

//----------------------------------------------------------------------------------------------------
type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)
//----------------------------------------------------------------------------------------------------
type Acktype int 

const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
)
//----------------------------------------------------------------------------------------------------

//----------------------------------------------------------------------------------------------------
func PublishJSON[T any](ch * amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("unable to marshal data to json. Function PublishJSON. Error: %s\n", err)
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body: data,
	})
	if err != nil {
		return fmt.Errorf("unable to publish data to channel. Function PublishJSON. Error: %s\n", err)
	}
	return nil
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) Acktype) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveryChannel, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func(){
		for msg := range deliveryChannel {
			var data T
			err = json.Unmarshal(msg.Body, &data)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue
			}
			acktype := handler(data)
			switch acktype {
			case Ack:
				msg.Ack(false)
				log.Println("Ack")
			case NackRequeue:
				msg.Nack(false, true)
				log.Println("NackRequeue")
			case NackDiscard:
				msg.Nack(false, false)
				log.Println("NackDiscard")
			default:
				fmt.Printf("Incorrect acktype: %v. Unable to process message\n", acktype)
			}	
		}
	}()
	return nil
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
		d := false
		t := false
		args := amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		}
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
		
		newQueue, err := channel.QueueDeclare(queueName, d, t, t, false, args)
		if err != nil {
			return nil, newQueue, err
		}

		err = channel.QueueBind(queueName, key, exchange, false, nil)
		if err != nil {
			return nil, newQueue, err
		}
		return channel, newQueue, nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(val)
	if err != nil {
		return fmt.Errorf("error encoding data to gob. Error: %v", err)
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body: buffer.Bytes(),
	})
	if err != nil {
		return fmt.Errorf("error publishing data to channel in PublishGob. Error: %v", err)
	}
	return nil
}