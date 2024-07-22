import type { Consumer, ConsumerConfig, Kafka, Message } from 'kafkajs'
import { makeSafeTopic } from './helpers'

export class KafkaTopicConsumer {
  cons: Consumer

  caughtUp = false

  constructor(
    kafka: Kafka,
    config: ConsumerConfig,
    topic: string,
    onMessage: (buf: Message) => void,
    private onCaughtUp?: () => void,
  ) {
    this.cons = kafka.consumer({ ...config, groupId: crypto.randomUUID() })

    this.cons.on('consumer.heartbeat', (e) => {
      if (!this.caughtUp) {
        this.caughtUp = true
        this.onCaughtUp?.()
      }
    })

    this.cons.on('consumer.end_batch_process', (e) => {
      const last = Number.parseInt(e.payload.lastOffset)
      const high = Number.parseInt(e.payload.highWatermark)

      if (last === high - 1 && !this.caughtUp) {
        this.caughtUp = true
        this.onCaughtUp?.()
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
            onMessage(message)
          },
          autoCommit: true,
        })
      })
  }

  disconnect() {
    this.cons.disconnect()
  }
}
