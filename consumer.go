package main

import (
    "fmt"
    "github.com/edebernis/sizematch-protobuf/go/items"
    "github.com/golang/protobuf/proto"
    "github.com/streadway/amqp"
    "time"
)

type consumer struct {
    host       string
    port       string
    username   string
    password   string
    vhost      string
    appID      string
    connection *amqp.Connection
    channel    *amqp.Channel
}

func (c *consumer) buildURL() string {
    return fmt.Sprintf("amqp://%s:%s@%s:%s/%s", c.username, c.password, c.host, c.port, c.vhost)
}

func (c *consumer) connect(connectionAttempts int) error {
    var err error
    url := c.buildURL()

    c.connection, err = amqp.Dial(url)
    if err != nil {
        if connectionAttempts < 1 {
            return err
        }
        time.Sleep(5 * time.Second)
        return c.connect(connectionAttempts - 1)
    }

    c.channel, err = c.connection.Channel()
    if err != nil {
        return err
    }

    return nil
}

func (c *consumer) setup(queueName string, prefetchCount int) error {
    _, err := c.channel.QueueDeclare(queueName, false, false, false, false, nil)
    if err != nil {
        return err
    }

    err = c.channel.Qos(prefetchCount, 0, false)
    if err != nil {
        return err
    }

    return nil
}

func (c *consumer) consumeItem(queueName string, callback func(item *items.NormalizedItem) error) error {
    msgs, err := c.channel.Consume(queueName, "", false, false, false, false, nil)
    if err != nil {
        return err
    }

    go func() {
        for msg := range msgs {
            item := items.NormalizedItem{}
            err := proto.Unmarshal(msg.Body, &item)
            if err != nil {
                fmt.Println("could not decode protobuf item: " + err.Error())
                msg.Nack(false, false)
                continue
            }

            err = callback(&item)
            if err != nil {
                msg.Nack(false, false)
                fmt.Println(err)
                continue
            }

            msg.Ack(false)
        }
    }()

    return nil
}

func (c *consumer) close() {
    c.connection.Close()
}
