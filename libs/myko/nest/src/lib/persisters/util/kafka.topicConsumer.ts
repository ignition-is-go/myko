import { Redis } from 'ioredis'
import { Consumer, ConsumerConfig, Kafka, Message } from 'kafkajs'
import { makeSafeTopic } from './helpers'

export class KafkaTopicConsumer {
  cons: Consumer

  redis: Redis

  private readOffset: number = 0

  constructor(
    kafka: Kafka,
    config: ConsumerConfig,
    private topic: string,
    onMessage: (buf: Message) => void,
    offsetKey: string,
    partitionKey: string,
    private onCaughtUp?: () => void,
  ) {
    this.cons = kafka.consumer(config)

    this.cons
      .connect()
      .then(() => {
        return this.cons.subscribe({ topic: makeSafeTopic(topic) })
      })
      .then(() => {
        this.cons.run({
          eachMessage: async ({ message }) => {
            onMessage(message)
          },
        })
      })
  }

  checkCaughtUp() {}

  disconnect() {
    this.cons.disconnect()
  }
}
