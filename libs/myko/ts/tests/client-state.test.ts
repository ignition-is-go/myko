import { describe, expect, test } from 'bun:test'
import type { Subject } from 'rxjs'

import { MykoClient, type Query, type View } from '../src/client.js'

type Item = { id: string; value: number }

type ClientInternals = {
  activeQueries: Map<string, unknown>
  activeViews: Map<string, unknown>
  queryResponseRoutes: Map<string, Subject<unknown>>
  viewResponseRoutes: Map<string, Subject<unknown>>
  queryErrorRoutes: Map<string, Subject<unknown>>
  sharedQueryResponseStreams: Map<string, unknown>
  sharedViewResponseStreams: Map<string, unknown>
}

const query: Query<Item> = {
  queryId: 'ItemsByQuery',
  queryItemType: 'Item',
  query: {},
}

const view: View<Item> = {
  viewId: 'ItemsByView',
  viewItemType: 'Item',
  view: {},
}

describe('MykoClient watchQueryState', () => {
  test('single-flights identical state watches and applies incremental updates', () => {
    const client = new MykoClient()
    const first = client.watchQueryState(query)
    const second = client.watchQueryState(query)
    expect(second).toBe(first)

    const seen: number[] = []
    const firstSub = first.subscribe((state) => seen.push(state.revision))
    const secondSub = second.subscribe()
    const internals = client as unknown as ClientInternals
    expect(internals.activeQueries.size).toBe(1)

    const tx = [...internals.activeQueries.keys()][0]
    internals.queryResponseRoutes.get(tx)?.next({
      event: 'ws:m:query-response',
      data: {
        tx,
        sequence: '0',
        deletes: [],
        upserts: [
          { itemType: 'Item', item: { id: 'a', value: 1 } },
          { itemType: 'Item', item: { id: 'b', value: 2 } },
        ],
      },
    })
    internals.queryResponseRoutes.get(tx)?.next({
      event: 'ws:m:query-response',
      data: {
        tx,
        sequence: '1',
        deletes: ['a'],
        upserts: [{ itemType: 'Item', item: { id: 'b', value: 3 } }],
      },
    })

    expect(seen).toEqual([1, 2])
    let latest: Item | undefined
    const replay = second.subscribe((state) => {
      latest = state.get('b')
    })
    expect(latest).toEqual({ id: 'b', value: 3 })

    replay.unsubscribe()
    firstSub.unsubscribe()
    secondSub.unsubscribe()
    expect(internals.activeQueries.size).toBe(0)
  })

  test('surfaces terminal errors while retaining the latest collection', () => {
    const client = new MykoClient()
    const states = client.watchQueryState(query)
    const seen: Array<{ status: string; size: number; error?: string }> = []
    const subscription = states.subscribe((state) => {
      seen.push({
        status: state.status,
        size: state.size,
        error: state.error?.message,
      })
    })
    const internals = client as unknown as ClientInternals
    const tx = [...internals.activeQueries.keys()][0]

    internals.queryResponseRoutes.get(tx)?.next({
      event: 'ws:m:query-response',
      data: {
        tx,
        sequence: '0',
        deletes: [],
        upserts: [{ itemType: 'Item', item: { id: 'a', value: 1 } }],
      },
    })
    internals.queryErrorRoutes.get(tx)?.next({
      event: 'ws:m:query-error',
      data: { tx, message: 'query failed' },
    })

    expect(seen).toEqual([
      { status: 'live', size: 1, error: undefined },
      { status: 'error', size: 1, error: 'query failed' },
    ])
    subscription.unsubscribe()
  })

  test('shares one wire subscription across array, diff, and state projections', () => {
    const client = new MykoClient()
    const arrays = client.watchQuery(query)
    const diffs = client.watchQueryDiff(query)
    const states = client.watchQueryState(query)
    const internals = client as unknown as ClientInternals

    expect(internals.activeQueries.size).toBe(1)
    expect(internals.sharedQueryResponseStreams.size).toBe(1)

    const seenArrays: Item[][] = []
    const seenSequences: bigint[] = []
    const seenStateSizes: number[] = []
    const arraySub = arrays.subscribe((items) => seenArrays.push(items))
    const diffSub = diffs.subscribe((diff) => seenSequences.push(diff.sequence))
    const stateSub = states.subscribe((state) => seenStateSizes.push(state.size))
    const tx = [...internals.activeQueries.keys()][0]
    internals.queryResponseRoutes.get(tx)?.next({
      event: 'ws:m:query-response',
      data: {
        tx,
        sequence: '0',
        deletes: [],
        upserts: [{ itemType: 'Item', item: { id: 'a', value: 1 } }],
      },
    })

    expect(seenArrays).toEqual([[{ id: 'a', value: 1 }]])
    expect(seenSequences).toEqual([0n])
    expect(seenStateSizes).toEqual([1])

    arraySub.unsubscribe()
    diffSub.unsubscribe()
    expect(internals.activeQueries.size).toBe(1)
    stateSub.unsubscribe()
    expect(internals.activeQueries.size).toBe(0)
    expect(internals.sharedQueryResponseStreams.size).toBe(0)
  })

  test('shares one wire subscription across view projections', () => {
    const client = new MykoClient()
    const arrays = client.watchView(view)
    const diffs = client.watchViewDiff(view)
    const states = client.watchViewState(view)
    const internals = client as unknown as ClientInternals

    expect(internals.activeViews.size).toBe(1)
    expect(internals.sharedViewResponseStreams.size).toBe(1)

    const arraySub = arrays.subscribe()
    const diffSub = diffs.subscribe()
    const stateSub = states.subscribe()
    const tx = [...internals.activeViews.keys()][0]
    internals.viewResponseRoutes.get(tx)?.next({
      event: 'ws:m:view-response',
      data: {
        tx,
        sequence: '0',
        deletes: [],
        upserts: [{ itemType: 'Item', item: { id: 'a', value: 1 } }],
      },
    })

    arraySub.unsubscribe()
    diffSub.unsubscribe()
    expect(internals.activeViews.size).toBe(1)
    stateSub.unsubscribe()
    expect(internals.activeViews.size).toBe(0)
    expect(internals.sharedViewResponseStreams.size).toBe(0)
  })
})
