package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/go-playground/validator/v10"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
)

var validate = validator.New(validator.WithRequiredStructEnabled())

// A ConnectionProfile contains everything needed to start a Consumer for webhook events.
type ConnectionProfile struct {
	consumerName string
	auth         sasl.Mechanism
	brokers      []net.Addr
}

// LoadConnectionProfile reads a ConnectionProfile from path.
//
// The caller must provide a type hint when calling LoadConnectionProfile. For example:
//
//	client.LoadConnectionProfile[map[string]any](profilePath)
//
// or
//
//	client.LoadConnectionProfile[HelloWorld](profilePath)
//
// If a type other than map[string]any is used, decoding will procede according to the
// [encoding/json] rules.
func LoadConnectionProfile(path string) (*ConnectionProfile, error) {
	p := struct {
		ConsumerName string `json:"consumer_name"`
		Auth         struct {
			Mechanism string `json:"mechanism"`
			Secret    string `json:"secret"`
		} `json:"auth"`
		Kafka struct {
			Servers []string `json:"servers"`
		}
	}{}

	f, err := os.Open(path)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	data, err := io.ReadAll(f)

	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("json decoding error: %w", err)
	}

	var mechanism sasl.Mechanism

	switch p.Auth.Mechanism {
	case "plain":
		mechanism = plain.Mechanism{
			Username: p.ConsumerName,
			Password: p.Auth.Secret,
		}
	default:
		return nil, fmt.Errorf("'%s' is not a supported auth mechanism", p.Auth.Mechanism)
	}

	brokers := make([]net.Addr, 0, len(p.Kafka.Servers))

	for _, addr := range p.Kafka.Servers {
		broker, err := net.ResolveTCPAddr("tcp", addr)

		if err != nil {
			return nil, err
		}

		brokers = append(brokers, broker)
	}

	candidate := ConnectionProfile{
		consumerName: p.ConsumerName,
		auth:         mechanism,
		brokers:      brokers,
	}

	if err = validate.Struct(candidate); err != nil {
		return nil, err
	}

	return &candidate, nil
}
