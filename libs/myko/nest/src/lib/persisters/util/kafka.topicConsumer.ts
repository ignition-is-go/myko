import { KafkaConsumer, Message } from 'node-rdkafka'
import { Redis } from 'ioredis'
import { makeSafeTopic } from './helpers'

export class KafkaTopicConsumer {
  cons: KafkaConsumer

  redis: Redis

  private readOffset: number = 0

  constructor(
    brokers: string[],
    groupId: string,
    private topics: string[],
    onMessage: (buf: Message) => void,
    offsetKey: string,
    partitionKey: string,
    private onCaughtUp?: () => void,
  ) {
    const redisHost = process.env.REDIS_HOST || 'localhost'
    const redisPort = Number.parseInt(process.env.REDIS_PORT) || 6379

    this.redis = new Redis(redisPort, redisHost)

    this.cons = new KafkaConsumer(
      {
        'metadata.broker.list': brokers.join(','),
        'group.id': groupId,
      },
      {
        'auto.offset.reset': 'smallest',
      },
    )

    this.cons.on('ready', async () => {
      this.cons.subscribe(topics.map(makeSafeTopic))
    })

    this.cons
      .on('subscribed', async () => {
        const offsetString = (await this.redis.get(offsetKey)) ?? '0'
        const offset = Number.parseInt(offsetString)

        const partitionString = (await this.redis.get(partitionKey)) ?? '0'
        const partition = Number.parseInt(partitionString)

        this.cons.assign(
          topics.map(makeSafeTopic).map((topic) => ({
            offset: offset,
            topic,
            partition: partition,
          })),
        )

        this.cons.consume()
        this.checkCaughtUp()
      })

      .on('data', (data) => {
        this.readOffset = data.offset

        onMessage(data)
      })
    this.cons.connect()
  }

  checkCaughtUp() {
    if (this.cons.isConnected() === false) {
      return
    }
    this.cons.queryWatermarkOffsets(this.topics[0], 0, 1000, (err, offsets) => {
      if (err) {
        console.warn(this.topics[0], err)
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
