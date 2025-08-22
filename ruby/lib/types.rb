require "json"
require "avro"
require "cbor"
require "deep-freeze"

module MisterWebhooks
  # An immutable representation of a Mister Webhooks Kafka message.
  class Message
    # The topic this message was received from.
    # @return [String] the topic
    attr_reader :topic

    # The partition this message was received from.
    # @return [Integer] the partition
    attr_reader :partition

    # The offset of this message in its partition.
    # @return [Integer] the offset
    attr_reader :offset

    # The key for this message.
    # @return [String] the key
    attr_reader :key

    # The payload of this message, as it was received.
    # @return [String] the payload
    attr_reader :raw_payload

    # The payload of this message, decoded
    # @return [WebhookEventV1] the decoded payload
    attr_reader :payload

    # The headers for this message.
    # @return [Hash<String, String>] the headers
    attr_reader :headers

    # The timestamp of this message.
    # @return [Time] the timestamp
    attr_reader :timestamp

    # Create a {Message} from a +Rdkafka::Consumer::Message+.
    def initialize(message)
      @topic = message.topic
      @partition = message.partition
      @offset = message.offset
      @raw_payload = message.payload.dup
      @key = message.key.dup
      @headers = message.headers.dup
      @timestamp = message.timestamp.dup

      @topic.freeze
      @raw_payload.freeze
      @key.freeze
      @headers.deep_freeze
      @timestamp.freeze

      @payload = self.class.parse_envelope(@headers, @raw_payload).freeze

      freeze
    end

    # An immutable representation of a webhook event.
    class WebhookEventV1
      # The HTTP method used to send the event.
      # @return [WebhookEvent::METHOD] the method identifier.
      attr_reader :method

      # The HTTP headers captured with the event payload.
      # @return [Hash{String => Array<String>}] the headers.
      attr_reader :headers

      # The HTTP event payload.
      # @return [Object] the payload.
      attr_reader :payload

      # Enumeration of HTTP methods that could be used to
      # transmit webhook events.
      METHOD = [
        METHOD_GET = "GET",
        METHOD_HEAD = "HEAD",
        METHOD_POST = "POST",
        METHOD_PUT = "PUT",
        METHOD_DELETE = "DELETE",
        METHOD_PATCH = "PATCH"
      ].map(&:to_sym)

      private

      def initialize(envelope)
        @method = envelope["method"].to_sym

        unless METHODS.member?(@method)
          raise ArgumentError.new("Invalid HTTP method '#{@method}'")
        end

        @headers = envelope["headers"].deep_freeze

        @payload = case envelope["encoding"]
        when "CBOR"
          CBOR.decode(envelope["payload"])
        when "JSON"
          JSON.decode(envelope["payload"])
        else
          raise EncodingError.new("payload encoding type #{envelope["payload"]} not supported")
        end.deep_freeze

        freeze
      end

      METHODS = Set.new(METHOD)
    end

    class << self
      def parse_envelope(headers, bytes)
        envelope_type = headers["envelope"]

        case envelope_type
        when "\x80".b
          payload_decoder = Avro::IO::BinaryDecoder.new(StringIO.new(bytes))
          envelope = KafkaMessageEnvelopeV1Reader.read(payload_decoder)
          WebhookEventV1.new(envelope)
        when nil
          raise EncodingError.new("envelope header missing")
        else
          raise EncodingError.new("unsupported envelope value #{envelope_type}")
        end
      end
    end

    private

    KafkaMessageEnvelopeV1Reader = Avro::IO::DatumReader.new(
      Avro::Schema.parse(
        <<~AVRO
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
        AVRO
      )
    )
  end
end
