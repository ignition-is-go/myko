import { Subject } from 'rxjs'

import { LiveCollection } from '../src/live-collection.js'

type Item = { id: string; value: number }

const ITEM_COUNT = 10_000
const COMPONENT_COUNT = 1_000
const UPDATE_COUNT = 1_000
const SAMPLES = 7

let consumed = 0

function initialState(): LiveCollection<Item> {
  return new LiveCollection<Item>().apply({
    sequence: 0n,
    deletes: [],
    upserts: Array.from({ length: ITEM_COUNT }, (_, value) => ({
      id: `item-${value}`,
      value,
    })),
  })
}

function render(item: Item | undefined): void {
  consumed += item?.value ?? 0
}

function broadcastEveryRevision(): number {
  const state = initialState()
  const revisions = new Subject<LiveCollection<Item>>()
  let notifications = 0
  for (let index = 0; index < COMPONENT_COUNT; index += 1) {
    const id = `item-${index}`
    revisions.subscribe((current) => {
      notifications += 1
      render(current.get(id))
    })
  }
  revisions.next(state)
  for (let update = 0; update < UPDATE_COUNT; update += 1) {
    state.apply({
      sequence: BigInt(update + 1),
      deletes: [],
      upserts: [{ id: `item-${update}`, value: update + ITEM_COUNT }],
    })
    revisions.next(state)
  }
  revisions.complete()
  return notifications
}

function selectImmutableItems(): number {
  const state = initialState()
  let notifications = 0
  const releases: Array<() => void> = []
  for (let index = 0; index < COMPONENT_COUNT; index += 1) {
    const id = `item-${index}`
    releases.push(
      state.subscribeItem(id, (item) => {
        notifications += 1
        render(item)
      }),
    )
  }
  for (let update = 0; update < UPDATE_COUNT; update += 1) {
    state.apply({
      sequence: BigInt(update + 1),
      deletes: [],
      upserts: [{ id: `item-${update}`, value: update + ITEM_COUNT }],
    })
  }
  for (const release of releases) release()
  return notifications
}

function median(samples: number[]): number {
  const sorted = samples.toSorted((a, b) => a - b)
  return sorted[Math.floor(sorted.length / 2)]
}

function measure(run: () => number): { medianMs: number; notifications: number } {
  run()
  let notifications = 0
  const samples = Array.from({ length: SAMPLES }, () => {
    const started = performance.now()
    notifications = run()
    return performance.now() - started
  })
  return { medianMs: median(samples), notifications }
}

const broadcast = measure(broadcastEveryRevision)
const selected = measure(selectImmutableItems)

console.table({
  'broadcast every revision': {
    medianMs: broadcast.medianMs.toFixed(3),
    notifications: broadcast.notifications,
    relative: '1.0x',
  },
  'immutable item selection': {
    medianMs: selected.medianMs.toFixed(3),
    notifications: selected.notifications,
    relative: `${(broadcast.notifications / selected.notifications).toFixed(1)}x fewer notifications`,
  },
})

if (consumed === 0) throw new Error('benchmark result was not consumed')
