import { LiveCollection } from '../src/live-collection.js'

type Item = { id: string; value: number }

const ITEM_COUNT = 10_000
const UPDATE_COUNT = 1_000
const SAMPLES = 7

const initial = Array.from({ length: ITEM_COUNT }, (_, value) => ({
  id: `item-${value}`,
  value,
}))

let consumed = 0

function legacyFullArrayUpdates(): void {
  const items = new Map(initial.map((item) => [item.id, item]))
  for (let value = 0; value < UPDATE_COUNT; value += 1) {
    const item = { id: `item-${value % ITEM_COUNT}`, value }
    items.set(item.id, item)
    const emitted = Array.from(items.values()).map((entry) => entry)
    const subscriberCopy = emitted.slice()
    consumed += subscriberCopy.length
  }
}

function diffNativeUpdates(): void {
  const state = new LiveCollection<Item>().apply({
    sequence: 0n,
    deletes: [],
    upserts: initial,
  })
  for (let value = 0; value < UPDATE_COUNT; value += 1) {
    state.apply({
      sequence: BigInt(value + 1),
      deletes: [],
      upserts: [{ id: `item-${value % ITEM_COUNT}`, value }],
    })
    consumed += state.size
  }
}

function lazyArrayUpdates(): void {
  const state = new LiveCollection<Item>().apply({
    sequence: 0n,
    deletes: [],
    upserts: initial,
  })
  for (let value = 0; value < UPDATE_COUNT; value += 1) {
    state.apply({
      sequence: BigInt(value + 1),
      deletes: [],
      upserts: [{ id: `item-${value % ITEM_COUNT}`, value }],
    })
    const values = state.toArray()
    consumed += values.length + state.toArray().length
  }
}

function median(samples: number[]): number {
  const sorted = samples.toSorted((a, b) => a - b)
  return sorted[Math.floor(sorted.length / 2)]
}

function measure(run: () => void): number {
  run()
  const samples = Array.from({ length: SAMPLES }, () => {
    const started = performance.now()
    run()
    return performance.now() - started
  })
  return median(samples)
}

const legacyMs = measure(legacyFullArrayUpdates)
const diffMs = measure(diffNativeUpdates)
const lazyArrayMs = measure(lazyArrayUpdates)

console.table({
  'legacy full-array': { medianMs: legacyMs.toFixed(3), relative: '1.0x' },
  'diff-native keyed': {
    medianMs: diffMs.toFixed(3),
    relative: `${(legacyMs / diffMs).toFixed(1)}x faster`,
  },
  'lazy array consumed': {
    medianMs: lazyArrayMs.toFixed(3),
    relative: `${(legacyMs / lazyArrayMs).toFixed(1)}x faster`,
  },
})

if (consumed === 0) throw new Error('benchmark result was not consumed')
