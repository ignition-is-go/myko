import { describe, expect, test } from 'bun:test'
import { firstValueFrom, type Observable, of } from 'rxjs'

import type {
  Command,
  MykoClient,
  Query,
  Report,
} from '../src/client.js'
import { bindGraph } from '../src/graph-client.js'
import { LiveCollection, type LiveIndex } from '../src/live-collection.js'

type Edge = { id: string; fromId: string; toId: string }
type Node = { id: string; label: string }

function query<T>(queryId: string, value: Record<string, unknown>): Query<T> {
  return { queryId, queryItemType: queryId, query: value }
}

function report<T>(reportId: string, value: Record<string, unknown>): Report<T> {
  return { reportId, report: value }
}

function command<T>(commandId: string, value: Record<string, unknown>): Command<T> {
  return { commandId, command: value }
}

const graph = {
  from: (endpoint: string) => query<Edge>('EdgeGraphFrom', { endpoint }),
  fromMany: (endpoints: string[]) => query<Edge>('EdgeGraphFromMany', { endpoints }),
  to: (endpoint: string) => query<Edge>('EdgeGraphTo', { endpoint }),
  toMany: (endpoints: string[]) => query<Edge>('EdgeGraphToMany', { endpoints }),
  between: (a: string, b: string) => query<Edge>('EdgeGraphBetween', { a, b }),
  targetsFrom: (endpoint: string) => query<Node>('EdgeGraphTargetsFrom', { endpoint }),
  targetsFromMany: (endpoints: string[]) =>
    query<Node>('EdgeGraphTargetsFromMany', { endpoints }),
  sourcesTo: (endpoint: string) => query<Node>('EdgeGraphSourcesTo', { endpoint }),
  sourcesToMany: (endpoints: string[]) =>
    query<Node>('EdgeGraphSourcesToMany', { endpoints }),
  countFrom: (endpoint: string) => report<number>('EdgeGraphCountFrom', { endpoint }),
  countTo: (endpoint: string) => report<number>('EdgeGraphCountTo', { endpoint }),
  countBetween: (a: string, b: string) =>
    report<number>('EdgeGraphCountBetween', { a, b }),
  existsBetween: (a: string, b: string) =>
    report<boolean>('EdgeGraphExistsBetween', { a, b }),
  connect: (edge: Edge) => command<void>('ConnectEdge', { edge }),
  connectMany: (edges: Edge[]) => command<number>('ConnectEdges', { edges }),
  ensure: (edge: Edge) => command<Edge>('EnsureEdge', { edge }),
  disconnect: (id: string) => command<boolean>('DeleteEdge', { id }),
  disconnectMany: (ids: string[]) => command<number>('DeleteEdges', { ids }),
} as const

const eagerGraph = {
  ...graph,
  $schema: {
    pairProjection: 'eager',
    aAdjacency: 'eager',
    bAdjacency: 'demandDriven',
  },
} as const

describe('bindGraph', () => {
  test('binds endpoint scope once across edge, related, and aggregate operations', async () => {
    const calls: Array<{
      kind: string
      id: string
      payload: unknown
      options?: unknown
    }> = []
    const state = new LiveCollection<Edge>()
    const nodeState = new LiveCollection<Node>()
    const client = {
      watchQueryState(value: Query<unknown>) {
        calls.push({ kind: 'query', id: value.queryId, payload: value.query })
        return of(value.queryId.includes('Targets') ? nodeState : state)
      },
      watchReport(value: Report<unknown>) {
        calls.push({ kind: 'report', id: value.reportId, payload: value.report })
        return of(3)
      },
      sendCommand(value: Command<unknown>, options?: unknown) {
        calls.push({
          kind: 'command',
          id: value.commandId,
          payload: value.command,
          options,
        })
        return Promise.resolve(true)
      },
    } as unknown as MykoClient

    const bound = bindGraph(client, graph)
    const scoped = bound.from('node-a')
    expect(bound.schema).toBeNull()
    expect(scoped.plan.strategy).toBe('unknown')
    const typedEdges: Observable<LiveCollection<Edge>> = scoped.edges()
    const typedTargets: Observable<LiveCollection<Node>> = scoped.targets()
    expect(await firstValueFrom(typedEdges)).toBe(state)
    expect(await firstValueFrom(typedTargets)).toBe(nodeState)
    expect(await firstValueFrom(scoped.count())).toBe(3)

    const edge: Edge = { id: 'edge-a-b', fromId: 'node-a', toId: 'node-b' }
    await bound.connect(edge, { timeoutMs: 250 })
    expect(calls).toEqual([
      { kind: 'query', id: 'EdgeGraphFrom', payload: { endpoint: 'node-a' } },
      {
        kind: 'query',
        id: 'EdgeGraphTargetsFrom',
        payload: { endpoint: 'node-a' },
      },
      {
        kind: 'report',
        id: 'EdgeGraphCountFrom',
        payload: { endpoint: 'node-a' },
      },
      {
        kind: 'command',
        id: 'ConnectEdge',
        payload: { edge },
        options: { timeoutMs: 250 },
      },
    ])
  })

  test('exposes one batched state query for many endpoints', async () => {
    const ids: string[] = []
    const collection = new LiveCollection<Node>()
    const client = {
      watchQueryState(value: Query<unknown>) {
        ids.push(value.queryId)
        return of(collection)
      },
    } as unknown as MykoClient
    const bound = bindGraph(client, graph)
    const typed: Observable<LiveCollection<Node>> = bound
      .fromMany(['node-a', 'node-b'])
      .targets()
    expect(await firstValueFrom(typed)).toBe(collection)
    expect(ids).toEqual(['EdgeGraphTargetsFromMany'])
  })

  test('builds a stable incremental edge index for grouped endpoint rendering', async () => {
    const state = new LiveCollection<Edge>().apply({
      sequence: 0n,
      deletes: [],
      upserts: [
        { id: 'edge-a-b', fromId: 'node-a', toId: 'node-b' },
        { id: 'edge-a-c', fromId: 'node-a', toId: 'node-c' },
        { id: 'edge-b-c', fromId: 'node-b', toId: 'node-c' },
      ],
    })
    const client = {
      watchQueryState() {
        return of(state)
      },
    } as unknown as MykoClient
    const bound = bindGraph(client, graph)
    const grouped: Observable<LiveIndex<string, Edge>> = bound
      .fromMany(['node-a', 'node-b'])
      .edgesBy((edge) => edge.fromId)
    const index = await firstValueFrom(grouped)

    expect(index.get('node-a').size).toBe(2)
    expect(index.get('node-b').size).toBe(1)
  })

  test('exposes fine-grained edge and related-entity selections', async () => {
    const edge: Edge = {
      id: 'edge-a-b',
      fromId: 'node-a',
      toId: 'node-b',
    }
    const target: Node = { id: 'node-b', label: 'B' }
    const edgeState = new LiveCollection<Edge>().apply({
      sequence: 0n,
      deletes: [],
      upserts: [edge],
    })
    const targetState = new LiveCollection<Node>().apply({
      sequence: 0n,
      deletes: [],
      upserts: [target],
    })
    const calls: string[] = []
    const stateFor = (value: Query<unknown>) =>
      value.queryId.includes('Targets') ? targetState : edgeState
    const client = {
      watchQuerySelection<T>(
        value: Query<unknown>,
        select: (state: LiveCollection<Edge> | LiveCollection<Node>) => T,
      ) {
        calls.push(`select:${value.queryId}`)
        return of(select(stateFor(value)))
      },
      watchQueryItem(value: Query<unknown>, id: string) {
        calls.push(`item:${value.queryId}:${id}`)
        return of(stateFor(value).get(id))
      },
      watchQueryHas(value: Query<unknown>, id: string) {
        calls.push(`has:${value.queryId}:${id}`)
        return of(stateFor(value).has(id))
      },
    } as unknown as MykoClient

    const from = bindGraph(client, graph).from('node-a')
    expect(await firstValueFrom(from.selectEdges((state) => state.size))).toBe(1)
    expect(await firstValueFrom(from.edge(edge.id))).toBe(edge)
    expect(await firstValueFrom(from.hasEdge('missing'))).toBe(false)
    expect(await firstValueFrom(from.selectTargets((state) => state.size))).toBe(1)
    expect(await firstValueFrom(from.target(target.id))).toBe(target)
    expect(await firstValueFrom(from.hasTarget('missing'))).toBe(false)
    expect(calls).toEqual([
      'select:EdgeGraphFrom',
      'item:EdgeGraphFrom:edge-a-b',
      'has:EdgeGraphFrom:missing',
      'select:EdgeGraphTargetsFrom',
      'item:EdgeGraphTargetsFrom:node-b',
      'has:EdgeGraphTargetsFrom:missing',
    ])
  })

  test('exposes execution plans and ergonomic mutable windows', async () => {
    const calls: Array<{ id: string; window: unknown }> = []
    const client = {
      watchQueryWindowed(value: Query<unknown>, options: unknown) {
        calls.push({ id: value.queryId, window: options })
        return {
          tx: `tx-${calls.length}`,
          results$: of([]),
          windowInfo$: of({ totalCount: 0, window: null }),
          setWindow() {},
        }
      },
    } as unknown as MykoClient

    const bound = bindGraph(client, eagerGraph)
    expect(bound.schema).toBe(eagerGraph.$schema)

    const from = bound.fromMany(['node-a', 'node-b'])
    expect(from.plan).toMatchObject({
      strategy: 'eagerEndpoint',
      initialization: 'indexLookup',
      liveUpdates: 'routed',
      windowing: 'pushdown',
    })
    expect(bound.from('node-c').plan).toBe(from.plan)
    expect(Object.isFrozen(from.plan)).toBe(true)
    expect(from.edgesWindowed({ offset: 25, limit: 10 }).tx).toBe('tx-1')
    expect(from.targetsWindowed({ offset: 0, limit: 5 }).tx).toBe('tx-2')

    const to = bound.to('node-b')
    expect(to.plan).toMatchObject({
      strategy: 'demandDrivenScan',
      initialization: 'canonicalScan',
      windowing: 'materialized',
    })
    expect(bound.between('node-a', 'node-b').plan).toMatchObject({
      strategy: 'eagerPair',
      windowing: 'pushdown',
    })
    expect(calls).toEqual([
      {
        id: 'EdgeGraphFromMany',
        window: { window: { offset: 25, limit: 10 } },
      },
      {
        id: 'EdgeGraphTargetsFromMany',
        window: { window: { offset: 0, limit: 5 } },
      },
    ])
  })
})
