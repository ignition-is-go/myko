import { NewTopic } from 'node-rdkafka'

export const newTopic = (name: string): NewTopic => ({
  num_partitions: 1,
  replication_factor: 3,
  topic: name,
  config: {
    'cleanup.policy': 'compact',
  },
})
