import { describe, expect, test, vi, waitFor } from 'vitest'

const adminMock = {
  createTopics: vi.fn().mockResolvedValue(undefined),
  disconnect: vi.fn().mockResolvedValue(undefined),
}

const producerMock = {
  connect: vi.fn().mockResolvedValue(undefined),
  send: vi.fn().mockResolvedValue(undefined),
}

const consumerMock = {
  connect: vi.fn().mockResolvedValue(undefined),
  subscribe: vi.fn().mockResolvedValue(undefined),
  run: vi.fn().mockResolvedValue(undefined),
  on: vi.fn(),
}

const kafkaInstance = {
  admin: () => adminMock,
  producer: () => producerMock,
  consumer: () => consumerMock,
}

const kafkaCtor = vi.fn(() => kafkaInstance)

vi.mock('kafkajs', () => ({
  Kafka: kafkaCtor,
}))

vi.mock('./util/kafka.topicConsumer', () => ({
  KafkaTopicConsumer: vi.fn().mockImplementation(() => ({})),
}))

vi.mock('./util/kafka.topicProducer', async () => {
  const actual = await vi.importActual<
    typeof import('./util/kafka.topicProducer.ts')
  >('./util/kafka.topicProducer.ts')
  return actual
})

describe('KafkaEntityPersister', () => {
  test('disconnects admin after creating topics', async () => {
    const { KafkaEntityPersister } = await import('./persister')

    new KafkaEntityPersister(
      'entity',
      { enableEventLog: true, brokers: [] },
      { brokers: ['localhost:9092'] } as any,
      {},
      { groupId: 'group' } as any,
      { info: vi.fn() } as any,
    )

    await waitFor(() => {
      expect(adminMock.createTopics).toHaveBeenCalled()
      expect(adminMock.disconnect).toHaveBeenCalled()
    })
  })
})
