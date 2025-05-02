package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/davecgh/go-spew/spew"
	"github.com/mister-webhooks/mister-webhooks-client/client"
)

func main() {
	flag.Parse()

	topicName := flag.Arg(0)
	profilePath := flag.Arg(1)

	if topicName == "" || profilePath == "" {
		flag.Usage()
		fmt.Fprintln(os.Stderr, "main: [options] <topic> <filename>")
		fmt.Fprintln(os.Stderr, "\truns a console consumer on the given topic using the provided connection profile")
		os.Exit(64) // EX_USAGE
	}

	profile, err := client.LoadConnectionProfile(profilePath)

	if err != nil {
		log.Fatal(err)
	}

	consumer, err := client.NewConsumer(
		profile,
		client.DeclareWebhookTopic[map[string]any](topicName),
	)

	if err != nil {
		log.Fatal(err)
	}

	err = consumer.Consume(context.Background(), func(ctx context.Context, event *client.Webhook[map[string]any]) error {
		log.Println(spew.Sdump(event))
		return nil
	})

	if err != nil {
		log.Fatalf("error: %s", err)
	}
}
