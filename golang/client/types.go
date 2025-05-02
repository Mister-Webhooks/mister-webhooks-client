package client

import (
	"fmt"
	"time"

	"github.com/hamba/avro/v2"
)

// KafkaMessageEnvelopeV1Schema contains the Mister Webhooks V1 Avro Schema
var KafkaMessageEnvelopeV1Schema avro.Schema = avro.MustParse(`{
  "type": "record",
  "name": "KafkaMessageEnvelopeV1",
  "namespace": "com.mister_webhooks.data",
  "fields": [
    {
      "name": "method",
      "type": {
        "type": "enum",
        "name": "Methods",
        "symbols": ["GET", "HEAD", "POST", "PUT", "DELETE", "PATCH"]
      }
    },
    {
      "name": "headers",
      "type": {
        "type": "map",
        "values": {
          "type": "array",
          "items": "string"
        }
      }
    },
    {
      "name": "payload",
      "type": "bytes"
    },
    {
      "name": "encoding",
      "type": {
        "type": "enum",
        "name": "Encodings",
        "symbols": ["JSON", "CBOR"]
      }
    }
  ]
}`)

// A KafkaMessageEnvelopeV1 corresponds to the KafkaMessageEnvelopeV1Schema.
type KafkaMessageEnvelopeV1 struct {
	Method   HTTPMethod          `avro:"method"`
	Headers  map[string][]string `avro:"headers"`
	Payload  []byte              `avro:"payload"`
	Encoding string              `avro:"encoding"`
}

// An HTTPMethod represents an HTTP method. Only the ones specified in the
// KafkaMessageEnvelopeV1Schema have been defined.
type HTTPMethod int

const (
	httpMethodInvalid HTTPMethod = iota
	HTTPMethodGET                // HTTP GET
	HTTPMethodHEAD               // HTTP HEAD
	HTTPMethodPOST               // HTTP POST
	HTTPMethodPUT                // HTTP PUT
	HTTPMethodDELETE             // HTTP DELETE
	HTTPMethodPATCH              // HTTP PATCH
)

func (m HTTPMethod) String() string {
	bytes, err := m.MarshalText()

	if err != nil {
		if m == httpMethodInvalid {
			return "<http_method_invalid>"
		}

		return fmt.Sprintf("<http_method_unknown(0x%2x)>", int(m))
	}

	return fmt.Sprintf("HTTP_Method_%s", bytes)
}

func (m *HTTPMethod) UnmarshalText(text []byte) error {
	switch string(text) {
	case "GET":
		*m = HTTPMethodGET
	case "HEAD":
		*m = HTTPMethodHEAD
	case "POST":
		*m = HTTPMethodPOST
	case "PUT":
		*m = HTTPMethodPUT
	case "DELETE":
		*m = HTTPMethodDELETE
	case "PATCH":
		*m = HTTPMethodPATCH
	default:
		*m = httpMethodInvalid
		return fmt.Errorf("unknown HTTPMethod '%s'", text)
	}

	return nil
}

func (m HTTPMethod) MarshalText() ([]byte, error) {
	switch m {
	case HTTPMethodGET:
		return []byte("GET"), nil
	case HTTPMethodHEAD:
		return []byte("HEAD"), nil
	case HTTPMethodPOST:
		return []byte("POST"), nil
	case HTTPMethodPUT:
		return []byte("PUT"), nil
	case HTTPMethodDELETE:
		return []byte("DELETE"), nil
	case HTTPMethodPATCH:
		return []byte("PATCH"), nil
	default:
		return nil, fmt.Errorf("unknown HTTP method enum value %d", m)
	}
}

// A Webhook represents a received webhook event.
type Webhook[T any] struct {
	Method    HTTPMethod          // The HTTP method the webhook used.
	Timestamp time.Time           // The time at which the webhook was sent.
	Headers   map[string][]string // The request's HTTP headers.
	Payload   *T                  // The request's payload.
}

type misterWebhooksEvent[T any] struct {
	systemEvent  *struct{}
	webhookEvent *struct {
		Method  HTTPMethod
		Headers map[string][]string
		Payload *T
	}
}
