# The Mister Webhooks Data Plane Protocol

We built Mister Webhooks out of standard components we chose for interoperability. This document specifies what a client must do to interoperate.

## Front Matter

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED",  "MAY", and "OPTIONAL" in this document are to be interpreted as described in [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).

## Connecting
Mister Webhooks hosts a Kafka cluster that contains webhook event data for our customers.

The Web UI for configuring consumers gives you a file when you create a new consumer, called a connection profile. The connection profile file contains all the material you need to connect to our Kafka cluster.

The user will need to provide the name of the Kafka topic to consume from. Each Mister Webhooks endpoint publishes into its own topic, and the Web UI displays the topic name.

### The Connection Profile Format

```json
{
  "consumer_name": "myproject.foobar",
  "auth": {
    "mechanism": "plain",
    "secret": "12345-abcde"
  },
  "kafka": {
    "servers": [
      "b0.mister-webhooks.net.example.com:9092"
    ]
  }
}
```

The `consumer_name` field is used both as the username to authenticate to Kafka with, as well as the consumer group to use for load balancing.

All other fields should be self-explanatory.

### Encryption

As of 24 April 2025, connection encryption is not yet supported. This will change before we begin revenue service.

### Authentication

Mister Webhooks supports the following SASL auth mechanisms:

| Mechanism | JSON identifier |
| --------- | --------------- |
| PLAIN     | `"plain"`       |

With the example connection profile above, you would use  SASL `PLAIN` with a username of `"myproject.foobar"` and a password of `"12345-abcde"` to authenticate.

## Receiving Data

Once you have an authenticated connection to our Kafka brokers, it's time to receive and decode data.

### Principal Components

There are three levels to understanding Mister Webhooks's data format.

The first is a Kafka message, where named topics contain numbered partitions that contain an ordered log of messages stored in arrival order. Each Kafka message has a key, used to choose which partition a message goes into; a set of headers, defined by Mister Webhooks's data plane; and a value which is a byte array.

The second is a message envelope. Envelopes let us use a structured encoding to represent the problem domain of "how do I record an HTTP webhook event?" separate from the details of the Kafka message model. This way the Kafka message model, which is hard to change and enforce changes to, can evolve slowly while the specific event models can change more quickly. A Kafka header, `envelope`, tells us the encoding and schema used for a particular message's `value`. We will handle any backwards-incompatible schema changes by choosing a new `envelope` value for the new schema.

The third is what is carried in the envelope itself, the HTTP event model.

The first and second levels handle system information, while the second and third levels handle domain information. When you write a Mister Webhooks client library your effort will go primarily to level two.

#### The Envelope Format Defined

Kafka headers are key value pairs, where the key is a string and the value is a byte array. As a client library author, you will have to decode the values yourself.

The envelope format consists of the following headers, with the following purposes:

| Header Name | Value Format | Meaning                    | Required |
| ----------- | ------------ | -------------------------- | -------- |
| `envelope`  | single byte  | The encoding+schema to use | yes      |

We expect to expand this when we add support for message-level encryption, or for other features.

Messages that are missing required headers are malformed and client libraries that encounter them MUST cease processing and communicate an error to their caller.

##### The `envelope` header

| Value | Encoding+Schema                                        |
| ----- | ------------------------------------------------------ |
| 0x00  | Reserved (System Internal)                             |
| ...   | Reserved (System Internal)                             |
| 0x7F  | Reserved (System Internal)                             |
| 0x80  | Avro `com.mister_webhooks.data.KafkaMessageEnvelopeV1` |

Envelope values between 0x00 and 0x7F are reserved for internal system use. Unless otherwise indicated, these message types are OPTIONAL and a client library MUST ignore ones it cannot handle.

Envelope values between 0x80 and 0xFF indicate HTTP webhook event data to be decoded and processed by end-user code. Unless otherwise indicated, support for these message types is REQUIRED. A library which encounters envelope values it does not know how to process is out of date, and the error communicated to the caller SHOULD indicate a client library version update is required by the user.

###### The Avro `com.mister_webhooks.data.KafkaMessageEnvelopeV1` schema

Here is the V1 Schema:
```
{
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
}
```

If you can't read Avro schemas yet but can read TypeScript types, it says this:
```
type KafkaMessageEnvelopeV1 = {
  method: "GET" | "HEAD" | "POST" | "PUT" | "DELETE" | "PATCH"
  headers: Map<string, string[]>
  payload: Buffer,
  encoding: "JSON" | "CBOR"
}
```

All of that should be fine, but what's `encoding`, and what's this `CBOR` encoding?

The Mister Webhooks data plane transcodes HTTP payloads to make them more compact -- they're written once, but read many times and stored forever, so it just makes sense. Over time we want to be able to add new, more space-efficient encodings. That's what the `encoding` enumeration is for. We figure that making you, the library author, have to support two encodings will make you structure your code in a way that it's easier for us to add more encodings in the future.

Ok, so what's `CBOR`? It's the RFC8949 Concise Binary Object Representation (https://cbor.io/). It's like JSON, but binary. It has three important qualities: one, it's _widely_ supported by languages; two, a JSON -> CBOR -> JSON round-trip will leave you with the document you started with; three, going from JSON -> CBOR can net you a 20% decrease in data size with no extra effort.
