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
type TraversalResult = {
  nodes: Array<{ entityType: string; id: string }>
  edgeIds: string[]
  truncated: boolean
}
type TraversalOptions = { maxDepth: number; maxNodes: number }

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
  traverseFrom: (start: string, options: TraversalOptions) =>
    report<TraversalResult>('EdgeGraphTraverseFrom', { start, ...options }),
  traverseTo: (start: string, options: TraversalOptions) =>
    report<TraversalResult>('EdgeGraphTraverseTo', { start, ...options }),
  connect: (edge: Edge) => command<void>('ConnectEdge', { edge }),
  connectMany: (edges: Edge[]) => command<number>('ConnectEdges', { edges }),
  syncFrom: (endpoint: string, edges: Edge[]) =>
    command<{ inserted: number }>('SyncEdgesFrom', {
      endpoint,
      scope: null,
      edges,
    }),
  syncTo: (endpoint: string, edges: Edge[]) =>
    command<{ inserted: number }>('SyncEdgesTo', {
      endpoint,
      scope: null,
      edges,
    }),
  ensure: (edge: Edge) => command<Edge>('EnsureEdge', { edge }),
  disconnect: (id: string) => command<boolean>('DeleteEdge', { id }),
  disconnectMany: (ids: string[]) => command<number>('DeleteEdges', { ids }),
} as const

const exactGraph = {
  ...graph,
  fromId: (endpoint: string, id: string) =>
    query<Edge>('EdgeGraphFromId', { endpoint, id }),
  fromIds: (endpoint: string, ids: string[]) =>
    query<Edge>('EdgeGraphFromIds', { endpoint, ids }),
  toId: (endpoint: string, id: string) =>
    query<Edge>('EdgeGraphToId', { endpoint, id }),
  toIds: (endpoint: string, ids: string[]) =>
    query<Edge>('EdgeGraphToIds', { endpoint, ids }),
  betweenId: (a: string, b: string, id: string) =>
    query<Edge>('EdgeGraphBetweenId', { a, b, id }),
  betweenIds: (a: string, b: string, ids: string[]) =>
    query<Edge>('EdgeGraphBetweenIds', { a, b, ids }),
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
    const traversed: Observable<TraversalResult> = scoped.traverse({
      maxDepth: 3,
      maxNodes: 100,
    })
    expect(await firstValueFrom(traversed)).toBe(3 as unknown as TraversalResult)

    const edge: Edge = { id: 'edge-a-b', fromId: 'node-a', toId: 'node-b' }
    await scoped.sync([edge], { timeoutMs: 125 })
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
        kind: 'report',
        id: 'EdgeGraphTraverseFrom',
        payload: { start: 'node-a', maxDepth: 3, maxNodes: 100 },
      },
      {
        kind: 'command',
        id: 'SyncEdgesFrom',
        payload: { endpoint: 'node-a', scope: null, edges: [edge] },
        options: { timeoutMs: 125 },
      },
      {
        kind: 'command',
        id: 'ConnectEdge',
        payload: { edge },
        options: { timeoutMs: 250 },
      },
    ])
  })

  test('requires and forwards a scope for scoped endpoint reconciliation', async () => {
    const calls: Command<unknown>[] = []
    const scopedGraph = {
      ...graph,
      syncFrom: (endpoint: string, scope: string, edges: Edge[]) =>
        command<{ inserted: number }>('SyncEdgesFrom', {
          endpoint,
          scope,
          edges,
        }),
    } as const
    const client = {
      sendCommand(value: Command<unknown>) {
        calls.push(value)
        return Promise.resolve({ inserted: 1 })
      },
    } as unknown as MykoClient
    const edge: Edge = { id: 'edge-a-b', fromId: 'node-a', toId: 'node-b' }
    await bindGraph(client, scopedGraph).from('node-a').sync([edge], 'tenant-a')
    expect(calls[0]).toEqual(
      command('SyncEdgesFrom', {
        endpoint: 'node-a',
        scope: 'tenant-a',
        edges: [edge],
      }),
    )
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

    const bound = bindGraph(client, exactGraph)
    const from = bound.from('node-a')
    expect(await firstValueFrom(from.selectEdges((state) => state.size))).toBe(1)
    expect(await firstValueFrom(from.edge(edge.id))).toBe(edge)
    expect(await firstValueFrom(from.hasEdge('missing'))).toBe(false)
    expect(await firstValueFrom(from.selectTargets((state) => state.size))).toBe(1)
    expect(await firstValueFrom(from.target(target.id))).toBe(target)
    expect(await firstValueFrom(from.hasTarget('missing'))).toBe(false)
    expect(await firstValueFrom(bound.to('node-b').edge(edge.id))).toBe(edge)
    expect(
      await firstValueFrom(bound.between('node-a', 'node-b').hasEdge(edge.id)),
    ).toBe(true)
    expect(calls).toEqual([
      'select:EdgeGraphFrom',
      'item:EdgeGraphFromId:edge-a-b',
      'has:EdgeGraphFromId:missing',
      'select:EdgeGraphTargetsFrom',
      'item:EdgeGraphTargetsFrom:node-b',
      'has:EdgeGraphTargetsFrom:missing',
      'item:EdgeGraphToId:edge-a-b',
      'has:EdgeGraphBetweenId:edge-a-b',
    ])
  })

  test('falls back to broad graph queries for legacy descriptors', async () => {
    const calls: string[] = []
    const client = {
      watchQueryItem(value: Query<unknown>) {
        calls.push(value.queryId)
        return of(undefined)
      },
      watchQueryHas(value: Query<unknown>) {
        calls.push(value.queryId)
        return of(false)
      },
    } as unknown as MykoClient

    const bound = bindGraph(client, graph)
    await firstValueFrom(bound.from('node-a').edge('edge-a-b'))
    await firstValueFrom(bound.to('node-b').hasEdge('edge-a-b'))
    await firstValueFrom(
      bound.between('node-a', 'node-b').edge('edge-a-b'),
    )
    expect(calls).toEqual([
      'EdgeGraphFrom',
      'EdgeGraphTo',
      'EdgeGraphBetween',
    ])
  })

  test('routes selected edge batches through exact scoped queries', async () => {
    const calls: Array<{ id: string; payload: unknown }> = []
    const state = new LiveCollection<Edge>()
    const client = {
      watchQueryState(value: Query<unknown>) {
        calls.push({ id: value.queryId, payload: value.query })
        return of(state)
      },
    } as unknown as MykoClient
    const bound = bindGraph(client, exactGraph)
    const ids = ['edge-c', 'edge-a']

    const from: Observable<LiveCollection<Edge>> = bound
      .from('node-a')
      .edgesByIds(ids)
    const to: Observable<LiveCollection<Edge>> = bound
      .to('node-b')
      .edgesByIds(ids)
    const between: Observable<LiveCollection<Edge>> = bound
      .between('node-a', 'node-b')
      .edgesByIds(ids)
    expect(await firstValueFrom(from)).toBe(state)
    expect(await firstValueFrom(to)).toBe(state)
    expect(await firstValueFrom(between)).toBe(state)
    expect(calls).toEqual([
      {
        id: 'EdgeGraphFromIds',
        payload: { endpoint: 'node-a', ids },
      },
      {
        id: 'EdgeGraphToIds',
        payload: { endpoint: 'node-b', ids },
      },
      {
        id: 'EdgeGraphBetweenIds',
        payload: { a: 'node-a', b: 'node-b', ids },
      },
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
