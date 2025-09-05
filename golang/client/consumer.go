package client

import (
	"context"
	"syscall"

	"github.com/jessekempf/kafka-go"
	mrkConsumer "github.com/jessekempf/mister-kafka/consumer"
	mrkCore "github.com/jessekempf/mister-kafka/core"
)

// A Consumer reads incoming webhook events and passes them to a supplied handler function.
type Consumer[T any] struct {
	kafkaConsumer *mrkConsumer.Consumer[misterWebhooksEvent[T]]
}

// A Handler is a user-supplied function that processes an event.
type Handler[T any] func(ctx context.Context, event *Webhook[T]) error

// NewConsumer creates a new consumer from a provided ConnectionProfile and WebhookTopic.
func NewConsumer[T any](profile *ConnectionProfile, topic WebhookTopic[T]) (*Consumer[T], error) {
	c := mrkConsumer.NewKafkaConsumer(
		profile.broker,
		profile.consumerName,
		topic.asConsumerTopic(),
		mrkConsumer.WithSecurity[misterWebhooksEvent[T]](
			profile.auth,
			profile.tls,
		),
		mrkConsumer.WithControlChannel[misterWebhooksEvent[T]](
			mrkConsumer.StopOnSignals([]syscall.Signal{syscall.SIGINT}),
		),
		mrkConsumer.WithAutoOffsetResetEarliest[misterWebhooksEvent[T]](),
	)

	return &Consumer[T]{
		kafkaConsumer: c,
	}, nil
}

// NewScriptedConsumer creates a new consumer from a topic and batches of raw Kafka messages.
//
// This is only useful for testing, and requires knowlege about Mister Webhooks's message format.
func NewScriptedConsumer[T any](topic WebhookTopic[T], script [][]*kafka.Message) (*Consumer[T], error) {
	c := mrkConsumer.NewScriptedConsumer(script, topic.asConsumerTopic())

	return &Consumer[T]{
		kafkaConsumer: c,
	}, nil
}

// Consume runs the provided handler against each webhook event.
// It blocks until either the context expires or the handler returns an error.
func (c *Consumer[T]) Consume(ctx context.Context, handler Handler[T]) error {
	return c.kafkaConsumer.Consume(ctx, func(ctx context.Context, message *mrkCore.InboundMessage[misterWebhooksEvent[T]]) error {
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
