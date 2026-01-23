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
    // Each consumer needs a unique groupId to avoid rebalancing storms
    // when 90+ entity consumers connect simultaneously
    const baseGroupId = config.groupId ?? crypto.randomUUID()
    this.cons = kafka.consumer({
      ...config,
      groupId: `${baseGroupId}-${makeSafeTopic(topic)}`,
    })

    const admin = kafka.admin()

    admin
      .fetchTopicOffsets(makeSafeTopic(topic))
      .then((offsets) => {
        // Handle empty offsets (new topic with no partitions yet) or high === '0' (empty topic)
        const high = offsets?.[0]?.high

        if (!high || high === '0') {
          this.caughtUp = true
          this.onCaughtUp?.()
        }
        // Topic has data - catch-up will complete when consumer reaches high watermark
        // No timeout: catch-up can legitimately take a long time for large topics
      })
      .catch(() => {
        // Topic doesn't exist yet or other error - treat as empty/caught up
        this.caughtUp = true
        this.onCaughtUp?.()
      })
      .finally(() => {
        void admin.disconnect()
      })

    this.cons.on('consumer.end_batch_process', (e) => {
      this.hasSeenData = true
      const last = Number.parseInt(e.payload.lastOffset, 10)
      const high = Number.parseInt(e.payload.highWatermark, 10)

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
