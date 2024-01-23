import { ConsumerGlobalConfig, KafkaConsumer, Message } from 'node-rdkafka'
import { Redis } from 'ioredis'
import { makeSafeTopic } from './helpers'

export class KafkaTopicConsumer {
  cons: KafkaConsumer

  redis: Redis

  private readOffset: number = 0

  constructor(
    config: ConsumerGlobalConfig,
    private topic: string,
    onMessage: (buf: Message) => void,
    offsetKey: string,
    partitionKey: string,
    private onCaughtUp?: () => void,
  ) {
    const redisHost = process.env.REDIS_HOST || 'localhost'
    const redisPort = Number.parseInt(process.env.REDIS_PORT) || 6379

    this.redis = new Redis(redisPort, redisHost)

    this.cons = new KafkaConsumer(config, {})

    this.cons.on('ready', async () => {
      this.cons.subscribe([makeSafeTopic(topic)])
    })

    this.cons
      .on('subscribed', async () => {
        const redisOffset = await this.redis.get(offsetKey)

        const offsetString = redisOffset ?? '0'
        const offset = Number.parseInt(offsetString)

        const partitionString = (await this.redis.get(partitionKey)) ?? '0'
        const partition = Number.parseInt(partitionString)

        const offsets = this.cons.queryWatermarkOffsets(
          topic,
          partition,
          1000,
          (err, offsets) => {
            console.log(topic, 'Offsets', offsets)
            console.log(`Subscribed to ${topic} at ${offset}`)
          },
        )

        this.cons.assign([
          {
            offset: offset,
            topic,
            partition: partition,
          },
        ])

        this.cons.consume()
        this.checkCaughtUp()
      })

      .on('data', (data) => {
        this.readOffset = data.offset

        onMessage(data)
      })

      .on('event.error', (err) => {
        console.warn('Error from consumer', err)
      })

      .on('rebalance', (err, assignment) => {
        console.log(`Reballancing: ${topic} (ignored)`, err, assignment)
      })

      .on('rebalance.error', (err) => {
        console.warn(`Reballancing Error: ${topic}`, err)
      })
      .on('event.log', (log) => {
        console.log(`EventLog ${topic}`, log)
      })

    this.cons.connect()
  }

  checkCaughtUp() {
    if (this.cons.isConnected() === false) {
      return
    }
    this.cons.queryWatermarkOffsets(this.topic, 0, 1000, (err, offsets) => {
      if (err) {
        console.warn(this.topic, err)
      } else {
        if (this.readOffset >= offsets.highOffset - 1) {
          this.onCaughtUp?.()
        }
      }

      setTimeout(() => this.checkCaughtUp(), 1000)
    })
  }

  disconnect() {
    this.cons.unsubscribe()
    this.cons.disconnect()
  }
}
