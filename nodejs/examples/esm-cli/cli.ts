import { MisterWebhooksConsumer } from '@mister-webhooks/client'
import { z } from 'zod'
import meow from 'meow'
import * as fs from 'fs'

const stringToJSONSchema = z.string().transform((str, ctx) => {
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

const cli = meow(
  `
  Usage
    $ listen.ts --topic <topic> [--topic <additional-topics>] --file <path-to-connection-profile>

  Examples
    $ listen.ts 
`,
  {
    importMeta: import.meta,
    flags: {
      topic: {
        isRequired: true,
        shortFlag: 't',
        type: 'string',
      },
      file: {
        shortFlag: 'f',
        isRequired: true,
        type: 'string',
      },
    },
  }
)

const { topic, file } = cli.flags
const readProfile = (): z.infer<typeof profileValidator> => {
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

const profile = readProfile()

const consumer = new MisterWebhooksConsumer({
  config: profile,
  topic,
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

consumer.start()
