import type { Consumer, ConsumerConfig, Kafka, Message } from 'kafkajs'
import { makeSafeTopic } from './helpers'

export class KafkaTopicConsumer {
  cons: Consumer

  caughtUp = false
  hasSeenData = false
  startTime = performance.now()

  percent = 0

  constructor(
    kafka: Kafka,
    config: ConsumerConfig,
    topic: string,
    onMessage: (buf: Message, percent) => void,
    private onCaughtUp?: () => void,
  ) {
    this.cons = kafka.consumer({ ...config, groupId: crypto.randomUUID() })

    const admin = kafka.admin()

    admin.fetchTopicOffsets(makeSafeTopic(topic)).then((offsets) => {
      const high = offsets[0].high

      if (high === '0') {
        this.caughtUp = true
        this.onCaughtUp?.()
      }
    })

    this.cons.on('consumer.end_batch_process', (e) => {
      this.hasSeenData = true
      const last = Number.parseInt(e.payload.lastOffset)
      const high = Number.parseInt(e.payload.highWatermark)

      if (last === high - 1 && !this.caughtUp) {
        this.caughtUp = true
        this.onCaughtUp?.()
      }

      if (!this.caughtUp) {
        this.percent = last / high
      }
    })

    this.cons
      .connect()
      .then(() => {
        return this.cons.subscribe({
          topic: makeSafeTopic(topic),
          fromBeginning: true,
        })
      })
      .then(() => {
        this.cons.run({
          eachMessage: async ({ message }) => {
            onMessage(message, this.percent)
          },
          autoCommit: true,
        })
      })
  }

  disconnect() {
    this.cons.disconnect()
  }
}
