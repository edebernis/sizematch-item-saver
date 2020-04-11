package main

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "github.com/edebernis/sizematch-protobuf/build/go/items"
    elasticsearch "github.com/elastic/go-elasticsearch/v8"
    "github.com/elastic/go-elasticsearch/v8/esutil"
    "strings"
    "time"
)

const indexConfig = `{
    "settings": {
        "index.number_of_shards": 1,
        "index.number_of_replicas": 1
    },
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "timestamp": { "type": "long" },
            "source":    { "type": "keyword" },
            "urls": {
                "properties": {
                    "en": { "type": "text" },
                    "fr": { "type": "text" }
                }
            }
            "name":  {
                "properties": {
                    "en": {
                        "type": "text",
                        "analyzer": "english",
                        "fields": {
                            "keyword": { 
                                "type": "keyword"
                            }
                        }
                    },
                    "fr": {
                        "type": "text",
                        "analyzer": "french",
                        "fields": {
                            "keyword": { 
                                "type": "keyword"
                            }
                        }
                    }
                }
            },
            "description": {
                "properties": {
                    "en": { 
                        "type":     "text",
                        "analyzer": "english"
                    },
                    "fr": { 
                        "type":     "text",
                        "analyzer": "french"
                    }
                }
            },
            "categories": {
                "properties": {
                    "en": {
                        "type": "text",
                        "analyzer": "english",
                        "fields": {
                            "keyword": { 
                                "type": "keyword"
                            }
                        }
                    },
                    "fr": {
                        "type": "text",
                        "analyzer": "french",
                        "fields": {
                            "keyword": { 
                                "type": "keyword"
                            }
                        }
                    }
                }
            },
            "image_urls": { "type": "text" },
            "dimensions": {
                "properties": {
                    "height":    { "type": "double" },
                    "width":     { "type": "double" },
                    "depth":     { "type": "double" },
                    "weight":    { "type": "double" },
                    "length":    { "type": "double" },
                    "diameter":  { "type": "double" },
                    "volume":    { "type": "double" },
                    "thickness": { "type": "double" }
                }
            },
            "price": {
                "properties": {
                    "en": {
                        "properties": {
                            "amount":    { "type": "double" },
                            "currency": { "type": "keyword" }
                        }
                    },
                    "fr": {
                        "properties": {
                            "amount":    { "type": "double" },
                            "currency": { "type": "keyword" }
                        }
                    }
                }
            }
        }
    }
}`

type savedItem struct {
    Timestamp   int64          `json:"timestamp"`
    Source      string         `json:"source"`
    Urls        multiLangValue `json:"urls"`
    Name        multiLangValue `json:"name"`
    Description multiLangValue `json:"description"`
    Categories  multiLangValue `json:"categories"`
    ImageUrls   []string       `json:"image_urls"`
    Dimensions  dimensions     `json:"dimensions"`
    Price       multiLangValue `json:"price"`
}

type multiLangValue struct {
    EN interface{} `json:"en,omitempty"`
    FR interface{} `json:"fr,omitempty"`
}

type dimensions struct {
    Height    float64 `json:"height,omitempty"`
    Width     float64 `json:"width,omitempty"`
    Depth     float64 `json:"depth,omitempty"`
    Weight    float64 `json:"weight,omitempty"`
    Length    float64 `json:"length,omitempty"`
    Diameter  float64 `json:"diameter,omitempty"`
    Volume    float64 `json:"volume,omitempty"`
    Thickness float64 `json:"thickness,omitempty"`
}

type price struct {
    Amount   float64 `json:"amount"`
    Currency string  `json:"currency"`
}

type saver struct {
    urls                 []string
    username             string
    password             string
    maxRetries           int
    indexName            string
    indexerWorkers       int
    indexerFlushBytes    int
    indexerFlushInterval int
    es                   *elasticsearch.Client
    indexer              esutil.BulkIndexer
}

func (s *saver) connect(connectionAttempts int) error {
    var err error

    cfg := elasticsearch.Config{
        Addresses:  s.urls,
        Username:   s.username,
        Password:   s.password,
        MaxRetries: s.maxRetries,
    }

    s.es, err = elasticsearch.NewClient(cfg)
    if err != nil {
        return fmt.Errorf("Error creating the client: %s", err)
    }

    _, err = s.es.Info()
    if err != nil {
        if connectionAttempts < 1 {
            return err
        }
        time.Sleep(5 * time.Second)
        return s.connect(connectionAttempts - 1)
    }

    return nil
}

func (s *saver) createIndex() error {
    res, err := s.es.Indices.Exists([]string{s.indexName})
    if err != nil {
        return err
    }

    res.Body.Close()
    if res.StatusCode == 404 {
        res, err := s.es.Indices.Create(
            s.indexName,
            s.es.Indices.Create.WithBody(strings.NewReader(indexConfig)),
        )
        if err != nil {
            return err
        }
        if res.IsError() {
            return fmt.Errorf("Error: Indices.Create: %s", res)
        }
    }
    return nil
}

func (s *saver) createIndexer() error {
    var err error
    s.indexer, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
        Index:         s.indexName,
        Client:        s.es,
        NumWorkers:    s.indexerWorkers,
        FlushBytes:    s.indexerFlushBytes,
        FlushInterval: time.Duration(s.indexerFlushInterval) * time.Second,
    })
    return err
}

func (s *saver) save(item *items.NormalizedItem) error {
    i, err := s.getSavedItem(item)
    if err != nil {
        return err
    }

    body, err := json.Marshal(map[string]interface{}{
        "doc":           i,
        "doc_as_upsert": true,
    })
    if err != nil {
        return err
    }

    return s.indexer.Add(
        context.Background(),
        esutil.BulkIndexerItem{
            Action:     "update",
            DocumentID: fmt.Sprintf("%s-%s", item.Source, item.Id),
            Body:       bytes.NewReader(body),
        },
    )
}

func (s *saver) getSavedItem(item *items.NormalizedItem) (*savedItem, error) {
    i := savedItem{
        Timestamp: time.Now().Unix(),
        Source:    item.Source,
        ImageUrls: item.ImageUrls,
    }

    err := s.serializeItemMultiLangAttributes(&i, item)
    if err != nil {
        return nil, err
    }

    err = s.serializeItemDimensions(&i, item)
    if err != nil {
        return nil, err
    }

    return &i, err
}

func (s *saver) serializeItemMultiLangAttributes(i *savedItem, item *items.NormalizedItem) error {
    switch item.Lang {
    case items.Lang_EN:
        i.Name = multiLangValue{EN: item.Name}
        i.Description = multiLangValue{EN: item.Description}
        i.Urls = multiLangValue{EN: item.Urls}
        i.Categories = multiLangValue{EN: item.Categories}
        i.Price = multiLangValue{EN: price{
            Amount:   item.Price.Amount,
            Currency: items.Price_Currency_name[int32(item.Price.Currency)],
        },
        }
    case items.Lang_FR:
        i.Name = multiLangValue{FR: item.Name}
        i.Description = multiLangValue{FR: item.Description}
        i.Urls = multiLangValue{FR: item.Urls}
        i.Categories = multiLangValue{FR: item.Categories}
        i.Price = multiLangValue{FR: price{
            Amount:   item.Price.Amount,
            Currency: items.Price_Currency_name[int32(item.Price.Currency)],
        },
        }
    }
    return nil
}

func (s *saver) serializeItemDimensions(i *savedItem, item *items.NormalizedItem) error {
    for _, d := range item.Dimensions {
        value, err := s.convertDimensionValue(d.Value, d.Unit)
        if err != nil {
            return err
        }
        switch d.Name {
        case items.Dimension_HEIGHT:
            i.Dimensions.Height = value
        case items.Dimension_WIDTH:
            i.Dimensions.Width = value
        case items.Dimension_DEPTH:
            i.Dimensions.Depth = value
        case items.Dimension_WEIGHT:
            i.Dimensions.Weight = value
        case items.Dimension_LENGTH:
            i.Dimensions.Length = value
        case items.Dimension_DIAMETER:
            i.Dimensions.Diameter = value
        case items.Dimension_VOLUME:
            i.Dimensions.Volume = value
        case items.Dimension_THICKNESS:
            i.Dimensions.Thickness = value
        default:
            return fmt.Errorf("Unknown dimension: %s", d)
        }
    }
    return nil
}

func (s *saver) convertDimensionValue(value float64, unit items.Dimension_Unit) (float64, error) {
    switch unit {
    case items.Dimension_CM, items.Dimension_KG, items.Dimension_CM2, items.Dimension_L:
        return value, nil
    case items.Dimension_M2:
        return 0.0001 * value, nil
    case items.Dimension_G, items.Dimension_M3:
        return 0.001 * value, nil
    case items.Dimension_MM2:
        return 0.01 * value, nil
    case items.Dimension_MM:
        return 0.1 * value, nil
    case items.Dimension_M:
        return 100 * value, nil
    case items.Dimension_CM3:
        return 1000 * value, nil
    case items.Dimension_MM3:
        return 1000000 * value, nil
    default:
        return 0, fmt.Errorf("Unknown unit: %s", unit)
    }
}

func (s *saver) close() {
    s.indexer.Close(context.Background())
}
