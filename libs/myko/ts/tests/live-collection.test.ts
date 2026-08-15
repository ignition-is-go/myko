import { describe, expect, test } from 'bun:test'

import {
  applyCollectionDiff,
  LiveCollection,
  LiveIndex,
} from '../src/live-collection.js'

type Item = { id: string; value: number }

describe('LiveCollection', () => {
  test('applies initial and incremental diffs without replacing keyed state', () => {
    const state = new LiveCollection<Item>()
    const items = state.items

    state.apply({
      sequence: 0n,
      deletes: [],
      upserts: [
        { id: 'a', value: 1 },
        { id: 'b', value: 2 },
      ],
    })

    expect(state.items).toBe(items)
    expect(state.status).toBe('live')
    expect(state.resolved).toBe(true)
    expect(state.revision).toBe(1)
    expect(state.get('a')).toEqual({ id: 'a', value: 1 })
    expect(state.changes.reset).toBe(true)

    state.apply({
      sequence: 1n,
      deletes: ['a'],
      upserts: [{ id: 'b', value: 3 }],
    })

    expect(state.items).toBe(items)
    expect(state.has('a')).toBe(false)
    expect(state.get('b')).toEqual({ id: 'b', value: 3 })
    expect(state.sequence).toBe(1n)
    expect(state.revision).toBe(2)
    expect(state.changes).toMatchObject({
      sequence: 1n,
      reset: false,
      deletes: ['a'],
      upserts: [{ id: 'b', value: 3 }],
    })
  })

  test('materializes arrays lazily and memoizes them per revision', () => {
    const state = new LiveCollection<Item>().apply({
      sequence: 0n,
      deletes: [],
      upserts: [{ id: 'a', value: 1 }],
    })

    const first = state.toArray()
    expect(state.toArray()).toBe(first)

    state.apply({
      sequence: 1n,
      deletes: [],
      upserts: [{ id: 'b', value: 2 }],
    })

    const second = state.toArray()
    expect(second).not.toBe(first)
    expect(second).toEqual([
      { id: 'a', value: 1 },
      { id: 'b', value: 2 },
    ])
    expect(state.toArray()).toBe(second)
  })

  test('a sequence-zero snapshot replaces all previous contents', () => {
    const state = new LiveCollection<Item>().apply({
      sequence: 0n,
      deletes: [],
      upserts: [{ id: 'old', value: 1 }],
    })

    state.apply({
      sequence: 0n,
      deletes: [],
      upserts: [{ id: 'new', value: 2 }],
    })

    expect(state.has('old')).toBe(false)
    expect(state.toArray()).toEqual([{ id: 'new', value: 2 }])
  })

  test('retains the latest data when the live stream errors', () => {
    const state = new LiveCollection<Item>().apply({
      sequence: 0n,
      deletes: [],
      upserts: [{ id: 'a', value: 1 }],
    })

    state.fail('disconnected')

    expect(state.status).toBe('error')
    expect(state.error?.message).toBe('disconnected')
    expect(state.get('a')).toEqual({ id: 'a', value: 1 })
    expect(state.changes.upserts).toEqual([])
  })

  test('does not report an initial error as resolved data', () => {
    const state = new LiveCollection<Item>().fail('unavailable')

    expect(state.status).toBe('error')
    expect(state.resolved).toBe(false)
  })
})

test('applyCollectionDiff supports framework-native Map implementations', () => {
  const target = new Map<string, Item>([['old', { id: 'old', value: 0 }]])

  const changes = applyCollectionDiff(target, {
    sequence: 0n,
    deletes: [],
    upserts: [{ id: 'new', value: 1 }],
  })

  expect([...target]).toEqual([['new', { id: 'new', value: 1 }]])
  expect(changes.reset).toBe(true)
})

type GroupedItem = Item & { group: string }

describe('LiveIndex', () => {
  test('groups once and updates only changed memberships', () => {
    let selectorCalls = 0
    const source = new LiveCollection<GroupedItem>().apply({
      sequence: 0n,
      deletes: [],
      upserts: [
        { id: 'a', value: 1, group: 'red' },
        { id: 'b', value: 2, group: 'blue' },
      ],
    })
    const index = new LiveIndex((item: GroupedItem) => {
      selectorCalls += 1
      return item.group
    }).update(source)

    expect(selectorCalls).toBe(2)
    expect(index.get('red').get('a')?.value).toBe(1)
    expect(index.changes).toEqual({ reset: true, keys: ['red', 'blue'] })
    const red = index.get('red')
    const blueValues = index.values('blue')
    expect(index.values('blue')).toBe(blueValues)

    source.apply({
      sequence: 1n,
      deletes: [],
      upserts: [{ id: 'a', value: 3, group: 'blue' }],
    })
    index.update(source)

    expect(selectorCalls).toBe(3)
    expect(red.size).toBe(0)
    expect(index.has('red')).toBe(false)
    expect(index.get('blue').size).toBe(2)
    expect(index.get('blue').get('a')?.value).toBe(3)
    expect(index.values('blue')).not.toBe(blueValues)
    expect(index.changes).toEqual({ reset: false, keys: ['red', 'blue'] })

    source.apply({ sequence: 2n, deletes: ['b'], upserts: [] })
    index.update(source)
    expect(selectorCalls).toBe(3)
    expect(index.get('blue').size).toBe(1)

    const blue = index.get('blue')
    source.apply({
      sequence: 3n,
      deletes: [],
      upserts: [{ id: 'a', value: 4, group: 'blue' }],
    })
    index.update(source)
    expect(index.get('blue')).toBe(blue)
    expect(blue.get('a')?.value).toBe(4)

    source.fail('group stream failed')
    index.update(source)
    expect(index.status).toBe('error')
    expect(index.error?.message).toBe('group stream failed')
    expect(index.changes).toEqual({ reset: false, keys: [] })
  })

  test('rebuilds from authoritative state after skipped revisions', () => {
    const source = new LiveCollection<GroupedItem>().apply({
      sequence: 0n,
      deletes: [],
      upserts: [{ id: 'a', value: 1, group: 'old' }],
    })
    const index = new LiveIndex((item: GroupedItem) => item.group).update(source)

    source.apply({
      sequence: 1n,
      deletes: [],
      upserts: [{ id: 'a', value: 2, group: 'middle' }],
    })
    source.apply({
      sequence: 2n,
      deletes: [],
      upserts: [{ id: 'a', value: 3, group: 'new' }],
    })
    index.update(source)

    expect(index.has('old')).toBe(false)
    expect(index.has('middle')).toBe(false)
    expect(index.get('new').get('a')?.value).toBe(3)
    expect(index.sourceRevision).toBe(source.revision)
  })
})
