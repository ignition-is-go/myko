import type { Consumer, ConsumerConfig, Kafka, Message } from 'kafkajs'
import { makeSafeTopic } from './helpers'

export class KafkaTopicConsumer {
  cons: Consumer

  private readOffset: number = 0

  constructor(
    kafka: Kafka,
    config: ConsumerConfig,
    topic: string,
    onMessage: (buf: Message) => void,
    private onCaughtUp?: () => void,
  ) {
    this.cons = kafka.consumer({ ...config, groupId: crypto.randomUUID() })

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
        })
      })
      .then(() => {
        this.onCaughtUp()
      })
  }

  checkCaughtUp() {}

  disconnect() {
    this.cons.disconnect()
  }
}
