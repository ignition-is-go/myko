import { describe, expect, test } from 'bun:test'

import { LiveCollection, applyCollectionDiff } from '../src/live-collection.js'

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
