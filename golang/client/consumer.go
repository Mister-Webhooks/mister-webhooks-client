package client

import (
	"context"
	"syscall"

	"github.com/jessekempf/mister-kafka/consumer"
	"github.com/jessekempf/mister-kafka/core"
	"github.com/segmentio/kafka-go"
)

// A Consumer reads incoming webhook events and passes them to a supplied handler function.
type Consumer[T any] struct {
	consumer *consumer.Consumer[misterWebhooksEvent[T]]
}

// NewConsumer creates a new consumer from a provided ConnectionProfile and WebhookTopic.
func NewConsumer[T any](profile *ConnectionProfile, topic WebhookTopic[T]) (*Consumer[T], error) {
	c := consumer.NewKafkaConsumer(
		profile.brokers[0],
		profile.consumerName,
		topic.asConsumerTopic(),
		consumer.WithSASL[misterWebhooksEvent[T]](profile.auth),
		consumer.WithControlChannel[misterWebhooksEvent[T]](
			consumer.StopOnSignals([]syscall.Signal{syscall.SIGINT}),
		),
	)

	return &Consumer[T]{
		consumer: c,
	}, nil
}

// NewScriptedConsumer creates a new consumer from a topic and batches of raw Kafka messages.
//
// This is only useful for testing, and requires knowlege about Mister Webhooks's message format.
func NewScriptedConsumer[T any](topic WebhookTopic[T], script [][]*kafka.Message) (*Consumer[T], error) {
	c := consumer.NewScriptedConsumer(script, topic.asConsumerTopic())

	return &Consumer[T]{
		consumer: c,
	}, nil
}

// Consume runs the provided handler against each webhook event.
// It blocks until either the context expires or the handler returns an error.
func (c *Consumer[T]) Consume(ctx context.Context, handler func(ctx context.Context, event *Webhook[T]) error) error {
	return c.consumer.Consume(ctx, func(ctx context.Context, message *core.InboundMessage[misterWebhooksEvent[T]]) error {
		if message.Body.systemEvent != nil {
			// No system events are supported yet
			return nil
		}

		if message.Body.webhookEvent == nil {
			panic("programming error: input must be either a systemEvent or a webhookEvent")
		}

		return handler(ctx, &Webhook[T]{
			Timestamp: message.Time,
			Method:    message.Body.webhookEvent.Method,
			Headers:   message.Body.webhookEvent.Headers,
			Payload:   message.Body.webhookEvent.Payload,
		})
	})
}
