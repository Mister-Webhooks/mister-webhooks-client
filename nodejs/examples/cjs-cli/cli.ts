/// <reference types="node" />
const { Command } = require('commander')
const { MisterWebhooksConsumer } = require('@mister-webhooks/client')
const { z } = require('zod')
const fs = require('fs')

const program = new Command()

const stringToJSONSchema = z.string().transform((str: string, ctx: any) => {
  try {
    return JSON.parse(str) as unknown
  } catch {
    ctx.addIssue({ code: 'custom', message: 'Invalid JSON' })
    return z.NEVER
  }
})

const profileValidator = stringToJSONSchema.pipe(
  z.object({
    consumer_name: z.string(),
    auth: z.object({
      mechanism: z.literal('plain'),
      secret: z.string(),
    }),
    kafka: z.object({
      bootstrap: z.string(),
    }),
  })
)

const readProfile = (file: string) => {
  try {
    const fileContents = fs.readFileSync(file, 'utf-8')
    const parsed = profileValidator.safeParse(fileContents)
    if (parsed.success) {
      return parsed.data
    }
    console.error('invalid connection profile')
    console.error(parsed.error.message)
    process.exit(-1)
  } catch {
    console.error('Cannot read ' + file)
    process.exit(-1)
  }
}

program
  .name('sdk-example-cjs')
  .description('Example CLI for using the Mister Webhooks client')
  .requiredOption('-t, --topic <topic>', 'Topic to subscribe to')
  .requiredOption('-f, --file <path>', 'Path to connection profile')

program.parse()

const options = program.opts()
const profile = readProfile(options.file)

const consumer = new MisterWebhooksConsumer({
  config: profile,
  topic: options.topic,
  handler: async (value: unknown) => {
    console.log(JSON.stringify(value, null, 2))
  },
  manualStart: true,
})

consumer.on('mrw.connected', () => {
  console.log('connected')
})
consumer.on('mrw.crashed', (err: unknown) => {
  console.log('crashed', err)
})
consumer.on('mrw.disconnected', () => {
  console.log('disconnected')
})
consumer.on('mrw.stopped', () => {
  console.log('stopped')
})
consumer.on('mrw.error', (err: unknown) => {
  console.error('error', err)
})

async function main() {
  consumer.start()
}

main().catch(console.error)
