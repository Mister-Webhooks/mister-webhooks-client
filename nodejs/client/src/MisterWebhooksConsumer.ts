import { EventEmitter } from 'events'
import {
  Kafka,
  logLevel as LogLevel,
  ConsumerCrashEvent,
  Consumer,
  EachMessageHandler,
} from 'kafkajs'
import { MessageOffset } from './MessageOffset'
import { decodeMessage } from './decodeMessage'
import { CACERT } from './CACERT'

export type ConnectionProfileConfig = {
  consumer_name: string
  auth: {
    mechanism: 'plain'
    secret: string
  }
  kafka: {
    bootstrap: string
  }
}

type MessagePayload<T> = {
  topic: string
  partition: number
  offset: MessageOffset
  key: string
  method: 'GET' | 'HEAD' | 'POST' | 'PUT' | 'DELETE' | 'PATCH'
  headers: Record<string, string[]>
  message: T
}

export type MessageProcessor<T = unknown> = (parameters: MessagePayload<T>) => Promise<void>

export const MISTER_WEBHOOKS_EVENT = {
  CONNECTED: 'mrw.connected',
  DISCONNECTED: 'mrw.disconnected',
  STOPPED: 'mrw.stopped',
  CRASHED: 'mrw.crashed',
  ERROR: 'mrw.error',
} as const

export type ConsumerEvent = (typeof MISTER_WEBHOOKS_EVENT)[keyof typeof MISTER_WEBHOOKS_EVENT]

type ExposedEvents = {
  [MISTER_WEBHOOKS_EVENT.CONNECTED]: []
  [MISTER_WEBHOOKS_EVENT.DISCONNECTED]: []
  [MISTER_WEBHOOKS_EVENT.STOPPED]: []
  [MISTER_WEBHOOKS_EVENT.CRASHED]: [ConsumerCrashEvent]
  [MISTER_WEBHOOKS_EVENT.ERROR]: [unknown]
}

export type MisterWebhooksConsumerOptions<T> = {
  config: ConnectionProfileConfig
  topic: string
  handler: MessageProcessor<T>
  manualStart?: boolean
  logLevel?: LogLevel
}

export class MisterWebhooksConsumer<T> extends EventEmitter<ExposedEvents> {
  private readonly consumer: Consumer
  private readonly topic: string
  private readonly handler: MessageProcessor<T>
  private startPromise: Promise<void> | undefined

  constructor({ config, topic, handler, manualStart, logLevel }: MisterWebhooksConsumerOptions<T>) {
    super()
    this.topic = topic
    this.handler = handler

    const kafka = new Kafka({
      clientId: config.consumer_name,
      brokers: [config.kafka.bootstrap],
      ssl: {
        ca: CACERT,
      },
      sasl: {
        mechanism: config.auth.mechanism,
        username: config.consumer_name,
        password: config.auth.secret,
      },
      logLevel,
    })

    this.consumer = kafka.consumer({
      groupId: config.consumer_name,
    })

    this.consumer.on('consumer.connect', () => {
      this.emit('mrw.connected')
    })

    this.consumer.on('consumer.disconnect', () => {
      this.emit('mrw.disconnected')
    })

    this.consumer.on('consumer.stop', () => {
      this.emit('mrw.stopped')
    })

    this.consumer.on('consumer.crash', (evt) => {
      this.emit('mrw.crashed', evt)
    })

    if (!manualStart) {
      void this.start()
    }
  }

  private handleMessage: EachMessageHandler = async ({ topic, message, partition }) => {
    const decodeResult = decodeMessage<T>(message)
    const { decoded, headers, method } = decodeResult
    await this.handler({
      topic,
      partition,
      offset: MessageOffset.fromString(message.offset),
      key: message.key?.toString() ?? 'none',
      method,
      headers,
      message: decoded,
    })
  }

  private startInternal = async () => {
    try {
      await this.consumer.connect()
      await this.consumer.subscribe({
        topic: this.topic,
        fromBeginning: true,
      })
      await this.consumer.run({ eachMessage: this.handleMessage })
    } catch (err) {
      this.emit('mrw.error', err)
    }
  }

  start = (): Promise<void> => {
    if (!this.startPromise) {
      this.startPromise = this.startInternal()
    }
    return this.startPromise
  }

  shutdown = () => this.consumer.disconnect()
}
