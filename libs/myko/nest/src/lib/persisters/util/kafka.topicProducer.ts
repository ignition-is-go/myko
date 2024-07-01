import { Kafka, Producer, ProducerConfig } from 'kafkajs'
import { makeSafeTopic } from './helpers'

export class KafkaTopicProducer {
  prod: Producer

  prodConnected = false

  protected sendQueue: Map<string, Buffer> = new Map()

  constructor(
    kafka: Kafka,
    private topic: string,
    config: ProducerConfig,
    private log: (msg: string) => void,
  ) {
    this.prod = kafka.producer(config)

    this.prod.connect().then(() => {
      this.prodConnected = true

      if (this.sendQueue.size > 0) {
        log(`Sending ${this.sendQueue.size} queued messages`)
        ;[...this.sendQueue.entries()].forEach(([key, buf]) =>
          this.send(buf, key),
        )
        this.sendQueue.clear()
      }
    })
  }

  public publish(msg: Buffer, key: string) {
    if (!this.prodConnected) {
      this.log(`Caching Persist due to not connected`)
      this.sendQueue.set(key, msg)
      return
    }

    this.send(msg, key)
  }

  private send(msg: Buffer, key: string) {
    const streamKey = makeSafeTopic(this.topic)

    try {
      this.prod.send({
        messages: [{ key, value: msg }],
        topic: streamKey,
      })
    } catch (e) {
      if (e instanceof Error && e.message === 'Local: Queue full') {
      }
    }
  }
}
