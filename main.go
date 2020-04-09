package main

import (
    "fmt"
    "github.com/edebernis/sizematch-protobuf/build/go/items"
    "os"
    "strconv"
)

func getEnv(key, fallback string) string {
    if value, ok := os.LookupEnv(key); ok {
        return value
    }
    return fallback
}

func run(m *messenger) error {
    err := m.consumeItem(os.Getenv("CONSUMER_QUEUE_NAME"), func(item *items.NormalizedItem) error {
        fmt.Println(item)
        return nil
    })
    if err != nil {
        return err
    }

    return nil
}

func main() {
    m := messenger{
        host:     getEnv("RABBITMQ_HOST", "localhost"),
        port:     getEnv("RABBITMQ_PORT", "5672"),
        username: getEnv("RABBITMQ_USERNAME", ""),
        password: getEnv("RABBITMQ_PASSWORD", ""),
        vhost:    getEnv("RABBITMQ_VHOST", ""),
        appID:    getEnv("RABBITMQ_APP_ID", ""),
    }

    connectionAttempts, err := strconv.Atoi(getEnv("RABBITMQ_CONNECTION_ATTEMPTS", "5"))
    if err != nil {
        panic("could not convert RABBITMQ_CONNECTION_ATTEMPTS env variable to int: " + err.Error())
    }

    err = m.connect(connectionAttempts)
    if err != nil {
        panic("could not connect to RabbitMQ: " + err.Error())
    }
    defer m.close()

    prefetchCount, err := strconv.Atoi(getEnv("PREFETCH_COUNT", "1"))
    if err != nil {
        panic("could not convert PREFETCH_COUNT env variable to int: " + err.Error())
    }

    err = m.setupConsumer(
        os.Getenv("CONSUMER_QUEUE_NAME"),
        prefetchCount,
    )
    if err != nil {
        panic("could not setup consumer: " + err.Error())
    }

    forever := make(chan bool)

    err = run(&m)
    if err != nil {
        panic("could not setup run: " + err.Error())
    }

    <-forever
}
