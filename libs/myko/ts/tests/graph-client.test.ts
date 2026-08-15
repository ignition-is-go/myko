import { describe, expect, test } from 'bun:test'
import { firstValueFrom, type Observable, of } from 'rxjs'

import type {
  Command,
  MykoClient,
  Query,
  Report,
} from '../src/client.js'
import { bindGraph } from '../src/graph-client.js'
import { LiveCollection } from '../src/live-collection.js'

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

describe('bindGraph', () => {
  test('binds endpoint scope once across edge, related, and aggregate operations', async () => {
    const calls: Array<{ kind: string; id: string; payload: unknown }> = []
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
      sendCommand(value: Command<unknown>) {
        calls.push({ kind: 'command', id: value.commandId, payload: value.command })
        return Promise.resolve(true)
      },
    } as unknown as MykoClient

    const bound = bindGraph(client, graph)
    const scoped = bound.from('node-a')
    const typedEdges: Observable<LiveCollection<Edge>> = scoped.edges()
    const typedTargets: Observable<LiveCollection<Node>> = scoped.targets()
    expect(await firstValueFrom(typedEdges)).toBe(state)
    expect(await firstValueFrom(typedTargets)).toBe(nodeState)
    expect(await firstValueFrom(scoped.count())).toBe(3)

    const edge: Edge = { id: 'edge-a-b', fromId: 'node-a', toId: 'node-b' }
    await bound.connect(edge)
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
      { kind: 'command', id: 'ConnectEdge', payload: { edge } },
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
})
