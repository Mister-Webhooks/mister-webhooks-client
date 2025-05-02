// Package client provides the client for Mister Webhooks.
//
// # Getting Started
//
// The general flow is LoadConnectionProfile[T]() -> NewConsumer[T]() -> Consume[T]().
//
// To get started with the client, you can use the following basic code that reads a connection profile
// file, creates a new consumer using it, and then starts the event loop and logs to console each event
// as it arrives.
//
//	package main
//
//	import (
//		"context"
//		"log"
//
//		"github.com/mister-webhooks/mister-webhooks-client/client"
//	)
//
//	func main() {
//		profile, err := client.LoadConnectionProfile[map[string]any]("/tmp/mr-w-example.profile")
//
//		if err != nil {
//			log.Fatal(err)
//		}
//
//		consumer, err := client.NewConsumer(profile)
//
//		if err != nil {
//			log.Fatal(err)
//		}
//
//		err = consumer.Consume(context.Background(), func(ctx context.Context, event *client.Webhook[map[string]any]) error {
//			log.Printf("%#v", event)
//			return nil
//		})
//
//		if err != nil {
//			log.Fatalf("error: %s", err)
//		}
//	}
//
// # Struct Tags
//
// Just like the [encoding/json] package, the Mister Webhooks client supports using struct tags to decode semi-structured
// webhook data into a custom struct. In fact, it's the json struct tags that are supported. If you have a webhook payload
// that looks like:
//
//	{
//	  "greeting": "hello",
//		"to":       "world",
//		"count":    3,
//	}
//
// you can decode it like so:
//
//	type HelloWorld struct {
//	  Greeting string `json:"greeting"`
//	  To       string `json:"to"`
//	  Count    int    `json:"count"`
//	}
package client
