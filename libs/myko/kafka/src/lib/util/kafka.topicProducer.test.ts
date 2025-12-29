import { describe, expect, test, vi, waitFor } from 'vitest'
import { KafkaTopicProducer } from './kafka.topicProducer'

describe('KafkaTopicProducer', () => {
  test('logs when publish send rejects', async () => {
    const connect = vi.fn().mockResolvedValue(undefined)
    const send = vi.fn().mockRejectedValue(new Error('queue full'))
    const log = vi.fn()

    const producer = new KafkaTopicProducer(
      {
        producer: () => ({
          connect,
          send,
        }),
      } as any,
      'topic',
      {},
      log,
    )

    await connect.mock.results[0].value

    producer.publish(Buffer.from('data'), 'key')

    await waitFor(() => {
      expect(log).toHaveBeenCalledWith(expect.stringContaining('Failed to send message to topic'))
    })
  })
})
