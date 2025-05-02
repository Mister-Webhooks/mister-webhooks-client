package client

import (
	"encoding/json"
	"fmt"

	"github.com/fxamacker/cbor/v2"
	"github.com/hamba/avro/v2"
	"github.com/jessekempf/mister-kafka/core"
	"github.com/segmentio/kafka-go"
)

// A WebhookTopic names a Kafka topic that contains a log of webhook events.
type WebhookTopic[T any] struct {
	name string
}

// DeclareWebhookTopic takes the name of a Kafka topic and wraps it into
// a WebhooksTopic.
func DeclareWebhookTopic[T any](topicName string) WebhookTopic[T] {
	return WebhookTopic[T]{
		name: topicName,
	}
}

func (t *WebhookTopic[T]) asConsumerTopic() core.ConsumerTopic[misterWebhooksEvent[T]] {
	decoder := func(key []byte, value []byte, headers []kafka.Header) (*misterWebhooksEvent[T], error) {
		envelopeType := byte(0)
		ok := false

		for _, header := range headers {
			if header.Key == "envelope" {
				envelopeType = header.Value[0]
				ok = true
				break
			}
		}

		if !ok {
			return nil, fmt.Errorf("malformed Kafka message, 'envelope' header is missing")
		}

		// Ignore all system messages
		if envelopeType < 0x80 {
			return &misterWebhooksEvent[T]{
				systemEvent: &struct{}{},
			}, nil
		}

		// Decode envelope and pass it back up
		switch envelopeType {
		case 0x80:
			t := new(T)
			kme := &KafkaMessageEnvelopeV1{}
			err := avro.Unmarshal(KafkaMessageEnvelopeV1Schema, value, kme)

			if err != nil {
				return nil, err
			}

			switch kme.Encoding {
			case "JSON":
				if err = json.Unmarshal(kme.Payload, &t); err != nil {
					return nil, err
				}
			case "CBOR":
				if err = cbor.Unmarshal(kme.Payload, &t); err != nil {
					return nil, err
				}
			}

			return &misterWebhooksEvent[T]{
				webhookEvent: &struct {
					Method  HTTPMethod
					Headers map[string][]string
					Payload *T
				}{
					Method:  kme.Method,
					Headers: kme.Headers,
					Payload: t,
				},
			}, nil
		default:
			return nil, fmt.Errorf("envelope type 0x%2x is unsupported, please upgrade mister-webhooks-client", envelopeType)
		}
	}

	return core.DeclareConsumerTopic(t.name, decoder)
}
