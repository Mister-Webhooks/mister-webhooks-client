require "rdkafka"
require "json"
require "awesome_print"
require "avro"
require "cbor"

require_relative "connection_profile"
require_relative "types"

# Namespace for Mister Webhooks
module MisterWebhooks
  # A Consumer for Mister Webhooks events.
  class Consumer
    # Create a consumer
    # @param [ConnectionProfile] profile the connection profile to connect with.
    # @param [String] topic the endpoint topic to connect to.
    def initialize(profile, topic)
      config = profile.config.merge({
        "client.id": "mister-webhooks-ruby a.b.c",
        "group.instance.id": "mrw",
        "enable.auto.commit": false,
        "enable.auto.offset.store": false
      })

      @consumer = Rdkafka::Config.new(config).consumer
      @consumer.subscribe(topic)
    end

    # Consume messages in an loop. Only returns when the provided block raises an exception, or
    # {#close} is called from another thread. Messages are marked as processed in Mister Webhooks
    # when the handler returns without raising an exception.
    #
    # @yieldparam [Message::WebhookEvent] event the webhook event received by Mister Webhooks.
    def consume
      @consumer.each do |raw_message|
        message = Message.new(raw_message)

        yield(message.payload)

        @consumer.store_offset(message)
        @consumer.commit
      end
    end

    # Disconnect the consumer from Mister Webhooks.
    def close
      @consumer.close
    end
  end
end
