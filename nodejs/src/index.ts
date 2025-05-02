import { Kafka, Consumer, logLevel } from 'kafkajs'
import CBOR from 'cbor2';
import { program } from 'commander';
import fs from 'node:fs';
import avro from 'avsc';

export type WebhookCallback<T> = (parameters: {
  topic: string,
  partition: number,
  offset: bigint,
  key: string,
  method: "GET" | "HEAD" | "POST" | "PUT" | "DELETE" | "PATCH",
  headers: Map<string, string[]>,
  message: T
}) => void

const KafkaMessageEnvelopeV1Schema = avro.Type.forSchema({
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
})
class ConnectionProfile {
  readonly consumerName: string;
  readonly kafka: Kafka;

  constructor(filename: string, logLevel: logLevel) {
    const data = JSON.parse(fs.readFileSync(filename).toString())

    this.consumerName = data.consumer_name

    this.kafka = new Kafka({
      clientId: this.consumerName,
      brokers: data.kafka.servers,
      sasl: {
        mechanism: data.auth.mechanism,
        username: this.consumerName,
        password: data.auth.secret
      },
      logLevel: logLevel,
    });
  }
}

var activeConsumers: MisterWebhooksConsumer<any>[] = []
const shutdownSignals: NodeJS.Signals[] = ['SIGINT', 'SIGTERM']

shutdownSignals.forEach(signal =>
  process.on(signal, async () => {
    console.info("received stop signal, shutting down")

    for (let activeConsumer of activeConsumers.slice()) {
      await activeConsumer.shutdown()
    }
    process.exit(0);
  })
)

export class MisterWebhooksConsumer<T> {
  private c: Consumer | null;
  private conn: ConnectionProfile;
  private topic: string;

  constructor(conn: ConnectionProfile, topic: string) {
    this.c = null;
    this.conn = conn;
    this.topic = topic;
  }

  async consume(callback: WebhookCallback<T>): Promise<void> {
    type WebhookPayload<T> = {
      body: T,
      headers: Map<string, string[]>,
    }

    const kafka = this.conn.kafka;

    this.c = kafka.consumer({ groupId: this.conn.consumerName })

    await this.c.connect()
    await this.c.subscribe({ topic: this.topic, fromBeginning: true })

    activeConsumers.push(this);

    console.info("starting consume loop")

    await this.c.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.headers || !message.headers.envelope) {
          console.error("received malformed Kafka message, 'envelope' header is missing")
        }

        const envelopeType = <number>message.headers!.envelope?.at(0)!

        if (envelopeType < 0x80) {
          console.warn(`received Kafka message with unknown system event type ${envelopeType}, ignoring...`)
        }

        switch (envelopeType) {
          case 0x80: {
            const envelope = KafkaMessageEnvelopeV1Schema.fromBuffer(message.value!);
            let decoded!: T

            switch (envelope.encoding) {
              case 'CBOR': {
                decoded = CBOR.decode(envelope.payload);
                break;
              }
              case 'JSON': {
                decoded = JSON.parse(envelope.payload);
                break;
              }
            }

            if (decoded == undefined) {
              throw new Error(`Kafka message ${topic}.${partition}[${message.offset}] could not be decoded`)
            }

            callback({
              topic: topic,
              partition: partition,
              offset: BigInt(message.offset),
              key: message.key!.toString(),
              method: envelope.method,
              headers: envelope.headers,
              message: decoded,
            })

            break;
          }
        }
      }
    })
  }

  async shutdown() {
    if (this.c) {
      await this.c.disconnect();
      activeConsumers = activeConsumers.filter((c) => c != this)
    }
  }
}

const parseLogLevel = (levelName: string): logLevel => {
  switch (levelName.toLowerCase()) {
    case "nothing": return logLevel.NOTHING;
    case "error": return logLevel.ERROR;
    case "warn": return logLevel.WARN;
    case "info": return logLevel.INFO;
    case "debug": return logLevel.DEBUG;
    default:
      throw new Error(`Configuration error: ${levelName} is not recognized`)
  }
}

program
  .command("reset_to_beginning <topic> <connfile>")
  .description("reset the consumer to start from the beginning")
  .option('--log-level', 'Logging level', parseLogLevel, logLevel.NOTHING)
  .action(async (topic, connfile, cmd) => {
    const conn = new ConnectionProfile(connfile, cmd.logLevel)

    let a = conn.kafka.admin()

    await a.connect()

    await a.resetOffsets({ groupId: conn.consumerName, topic: topic, earliest: true })
    await a.disconnect()
    console.info(`reset offets on ${topic} for ${conn.consumerName}`)
  })

program
  .command("log <topic> <connfile>")
  .description("run the consumer against <topic> using the <connfile> profile")
  .option('--log-level', 'Logging level', parseLogLevel, logLevel.INFO)
  .action(async (topic, connfile, cmd) => {
    await new MisterWebhooksConsumer<any>(new ConnectionProfile(connfile, cmd.logLevel), topic)
      .consume(console.info)
  })


program.parseAsync()
