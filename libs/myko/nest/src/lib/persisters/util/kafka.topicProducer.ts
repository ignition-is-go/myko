import { AdminClient, Producer, ProducerGlobalConfig } from 'node-rdkafka'
import { makeSafeTopic } from './helpers'
import { newTopic } from './kafka.newTopic'

export class KafkaTopicProducer {
  prod: Producer

  prodConnected = false

  protected sendQueue: Map<string, Buffer> = new Map()

  constructor(
    private topic: string,
    config: ProducerGlobalConfig,
    private log: (msg: string) => void,
  ) {
    const admin = AdminClient.create(config)

    const safeTopic = makeSafeTopic(topic)

    admin.createTopic(newTopic(safeTopic))

    this.prod = new Producer(config)

    this.prod.connect()
    this.prod.on('ready', () => {
      this.log('Producer is ready')
      this.prod.setPollInterval(100)
      this.prod.poll()
      this.prodConnected = true
      if (this.sendQueue.size > 0) {
        log(`Sending ${this.sendQueue.size} queued messages`)
        ;[...this.sendQueue.entries()].forEach(([key, buf]) =>
          this.send(buf, key),
        )
        this.sendQueue.clear()
      }
    })

    this.prod.on('event.error', (err) => {
      console.warn('Error from producer', err)
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
      this.prod.produce(streamKey, null, msg, key, Date.now())
    } catch (e) {
      if (e instanceof Error && e.message === 'Local: Queue full') {
      }
    }
  }
}
