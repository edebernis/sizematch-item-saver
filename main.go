package main

import (
    "github.com/edebernis/sizematch-protobuf/build/go/items"
    "os"
    "strconv"
    "strings"
)

func getEnv(key, fallback string) string {
    if value, ok := os.LookupEnv(key); ok {
        return value
    }
    return fallback
}

func run(c *consumer, s *saver) error {
    err := c.consumeItem(os.Getenv("CONSUMER_QUEUE_NAME"), func(item *items.NormalizedItem) error {
        return s.save(item)
    })
    if err != nil {
        return err
    }

    return nil
}

func setupConsumer() (*consumer, error) {
    c := consumer{
        host:     getEnv("RABBITMQ_HOST", "localhost"),
        port:     getEnv("RABBITMQ_PORT", "5672"),
        username: getEnv("RABBITMQ_USERNAME", ""),
        password: getEnv("RABBITMQ_PASSWORD", ""),
        vhost:    getEnv("RABBITMQ_VHOST", ""),
        appID:    getEnv("RABBITMQ_APP_ID", ""),
    }

    connectionAttempts, err := strconv.Atoi(getEnv("RABBITMQ_CONNECTION_ATTEMPTS", "5"))
    if err != nil {
        return nil, err
    }

    err = c.connect(connectionAttempts)
    if err != nil {
        return nil, err
    }

    prefetchCount, err := strconv.Atoi(getEnv("PREFETCH_COUNT", "1"))
    if err != nil {
        return nil, err
    }

    err = c.setup(
        os.Getenv("CONSUMER_QUEUE_NAME"),
        prefetchCount,
    )
    if err != nil {
        return nil, err
    }

    return &c, nil
}

func setupSaver() (*saver, error) {
    connectionAttempts, err := strconv.Atoi(getEnv("ELASTICSEARCH_CONNECTION_ATTEMPTS", "5"))
    if err != nil {
        return nil, err
    }
    maxRetries, err := strconv.Atoi(getEnv("ELASTICSEARCH_MAX_RETRIES", "3"))
    if err != nil {
        return nil, err
    }
    indexerWorkers, err := strconv.Atoi(getEnv("INDEXER_WORKERS", "1"))
    if err != nil {
        return nil, err
    }
    indexerFlushBytes, err := strconv.Atoi(getEnv("INDEXER_FLUSH_BYTES", "5000000"))
    if err != nil {
        return nil, err
    }
    indexerFlushInterval, err := strconv.Atoi(getEnv("INDEXER_FLUSH_INTERVAL", "3"))
    if err != nil {
        return nil, err
    }

    s := saver{
        urls:                 strings.Split(getEnv("ELASTICSEARCH_URLS", "http://localhost:9200"), ","),
        username:             getEnv("ELASTICSEARCH_USERNAME", ""),
        password:             getEnv("ELASTICSEARCH_PASSWORD", ""),
        maxRetries:           maxRetries,
        indexName:            os.Getenv("INDEX_NAME"),
        indexerWorkers:       indexerWorkers,
        indexerFlushBytes:    indexerFlushBytes,
        indexerFlushInterval: indexerFlushInterval,
    }

    err = s.connect(connectionAttempts)
    if err != nil {
        return nil, err
    }

    err = s.createIndex()
    if err != nil {
        return nil, err
    }

    err = s.createIndexer()
    if err != nil {
        return nil, err
    }

    return &s, nil
}

func main() {
    saver, err := setupSaver()
    if err != nil {
        panic(err.Error())
    }
    defer saver.close()

    consumer, err := setupConsumer()
    if err != nil {
        panic(err.Error())
    }
    defer consumer.close()

    forever := make(chan bool)

    err = run(consumer, saver)
    if err != nil {
        panic("could not setup run: " + err.Error())
    }

    <-forever
}
