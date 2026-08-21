import { EventEmitter } from 'events'
import {
  Kafka,
  logLevel as LogLevel,
  ConsumerCrashEvent,
  Consumer,
  EachMessageHandler,
  Logger,
} from 'kafkajs'
import { MessageOffset } from './MessageOffset'
import { decodeMessage } from './decodeMessage'
import { CACERT } from './CACERT'
import { setInterval } from 'node:timers/promises'

export { logLevel, Logger } from 'kafkajs'

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

export type MessagePayload<MessageType> = {
  topic: string
  partition: number
  offset: MessageOffset
  key: string
  method: 'GET' | 'HEAD' | 'POST' | 'PUT' | 'DELETE' | 'PATCH'
  headers: Record<string, string[]>
  message: MessageType
}

export type MessageProcessor<MessageType = unknown> = (
  logger: Logger,
  parameters: MessagePayload<MessageType>
) => Promise<void>

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

export type StartPoint = Date | 'EARLIEST' | 'LAST_PROCESSED'

export type MisterWebhooksConsumerOptions<MessageType> = {
  config: ConnectionProfileConfig
  topic: string
  handler: MessageProcessor<MessageType>
  startPoint?: StartPoint
  manualStart?: boolean
  logLevel?: LogLevel
}

export class MisterWebhooksConsumer<MessageType> extends EventEmitter<ExposedEvents> {
  private readonly kafka: Kafka
  private readonly config: ConnectionProfileConfig
  private readonly consumer: Consumer
  private readonly topic: string
  private readonly handler: MessageProcessor<MessageType>
  private startPromise: Promise<void> | undefined
  private readonly startPoint: StartPoint
  private readonly handlerLogger: Logger

  constructor({
    config,
    topic,
    handler,
    manualStart,
    logLevel,
    startPoint,
  }: MisterWebhooksConsumerOptions<MessageType>) {
    super()
    this.topic = topic
    this.handler = handler
    this.startPoint = startPoint ?? 'LAST_PROCESSED'

    this.config = config

    this.kafka = new Kafka({
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

    this.handlerLogger = this.kafka.logger()

    this.consumer = this.kafka.consumer({
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

  // eslint-disable-next-line @typescript-eslint/unbound-method
  private handleMessage: EachMessageHandler = async ({ topic, message, partition, heartbeat }) => {
    const decodeResult = decodeMessage<MessageType>(message)
    if (!decodeResult) {
      return
    }
    const { decoded, headers, method } = decodeResult
    const handle = this.handler(this.handlerLogger, {
      topic,
      partition,
      offset: MessageOffset.fromString(message.offset),
      key: message.key?.toString() ?? 'none',
      method,
      headers,
      message: decoded,
    })

    const pendingState = 'pending'

    // eslint-disable-next-line  @typescript-eslint/no-unused-vars
    for await (const _ of setInterval(100)) {
      const outcome = await Promise.race([handle, pendingState])

      if (outcome === pendingState) {
        await heartbeat()
        continue
      }

      await this.consumer.commitOffsets([{ topic, partition, offset: message.offset }])
      break
    }
  }

  private startInternal = async () => {
    try {
      await this.consumer.connect()
      if (this.startPoint !== 'LAST_PROCESSED') {
        await this.adjustStartPoint()
      }

      await this.consumer.subscribe({
        topic: this.topic,
        // this defines what to use if the offset is invalid or not specified... not 100% sure how this applies
        // in the case that we're specifying the offset
        fromBeginning: true,
      })
      await this.consumer.run({ eachMessage: this.handleMessage })
    } catch (err) {
      this.emit('mrw.error', err)
    }
  }

  private adjustStartPoint = async () => {
    const admin = this.kafka.admin()
    const groupId = this.config.consumer_name
    const topic = this.topic
    if (this.startPoint === 'EARLIEST') {
      await admin.resetOffsets({
        groupId,
        topic,
        earliest: true,
      })
    } else if (this.startPoint instanceof Date) {
      await admin.setOffsets({
        groupId,
        topic,
        partitions: await admin.fetchTopicOffsetsByTimestamp(topic, this.startPoint.valueOf()),
      })
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
