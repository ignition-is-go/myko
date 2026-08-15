import type { Observable } from 'rxjs'

import type {
  Command,
  CommandResult,
  MykoClient,
  Query,
  QueryWatchOptions,
  Report,
  ReportResult,
} from './client.js'
import type { LiveCollection } from './live-collection.js'

type MethodResult<G, K extends PropertyKey> = K extends keyof G
  ? G[K] extends (...args: never[]) => infer R
    ? R
    : never
  : never

type MethodArgs<G, K extends PropertyKey> = K extends keyof G
  ? G[K] extends (...args: infer A) => unknown
    ? A
    : never
  : never

type MethodFirstArg<G, K extends PropertyKey> = MethodArgs<G, K> extends [
  infer A,
  ...unknown[],
]
  ? A
  : never

type QueryState<Q> = Q extends Query<infer T>
  ? T extends { id: string }
    ? Observable<LiveCollection<T>>
    : never
  : never

type ReportState<R> = R extends Report<unknown>
  ? Observable<ReportResult<R>>
  : never

type CommandPromise<C> = C extends Command<unknown>
  ? Promise<CommandResult<C>>
  : never

type RelatedScope<G, K extends PropertyKey, Name extends string> = K extends keyof G
  ? {
      [P in Name]: (
        options?: QueryWatchOptions,
      ) => QueryState<MethodResult<G, K>>
    }
  : object

type EndpointScope<
  G,
  EdgeKey extends PropertyKey,
  RelatedKey extends PropertyKey,
  RelatedName extends string,
  CountKey extends PropertyKey,
> = {
  edges: (options?: QueryWatchOptions) => QueryState<MethodResult<G, EdgeKey>>
} & (CountKey extends keyof G
  ? { count: () => ReportState<MethodResult<G, CountKey>> }
  : object) &
  RelatedScope<G, RelatedKey, RelatedName>

type BetweenScope<G> = {
  edges: (options?: QueryWatchOptions) => QueryState<MethodResult<G, 'between'>>
  count: () => ReportState<MethodResult<G, 'countBetween'>>
  exists: () => ReportState<MethodResult<G, 'existsBetween'>>
}

type BatchScopes<G> = 'fromMany' extends keyof G
  ? {
      fromMany: (
        endpoints: MethodFirstArg<G, 'fromMany'>,
      ) => EndpointScope<G, 'fromMany', 'targetsFromMany', 'targets', never>
      toMany: (
        endpoints: MethodFirstArg<G, 'toMany'>,
      ) => EndpointScope<G, 'toMany', 'sourcesToMany', 'sources', never>
    }
  : object

/** Client-bound facade over one generated `*Graph` descriptor. */
export type BoundGraph<G> = {
  from: (
    endpoint: MethodFirstArg<G, 'from'>,
  ) => EndpointScope<G, 'from', 'targetsFrom', 'targets', 'countFrom'>
  to: (
    endpoint: MethodFirstArg<G, 'to'>,
  ) => EndpointScope<G, 'to', 'sourcesTo', 'sources', 'countTo'>
  between: (
    a: MethodFirstArg<G, 'between'>,
    b: MethodArgs<G, 'between'> extends [unknown, infer B, ...unknown[]]
      ? B
      : never,
  ) => BetweenScope<G>
  connect: (
    edge: MethodFirstArg<G, 'connect'>,
  ) => CommandPromise<MethodResult<G, 'connect'>>
  connectMany: (
    edges: MethodFirstArg<G, 'connectMany'>,
  ) => CommandPromise<MethodResult<G, 'connectMany'>>
  ensure: (
    edge: MethodFirstArg<G, 'ensure'>,
  ) => CommandPromise<MethodResult<G, 'ensure'>>
  disconnect: (
    id: MethodFirstArg<G, 'disconnect'>,
  ) => CommandPromise<MethodResult<G, 'disconnect'>>
  disconnectMany: (
    ids: MethodFirstArg<G, 'disconnectMany'>,
  ) => CommandPromise<MethodResult<G, 'disconnectMany'>>
} & BatchScopes<G>

type DynamicFactory = (...args: unknown[]) => unknown

function factory(graph: object, key: string): DynamicFactory {
  const candidate = (graph as Record<string, unknown>)[key]
  if (typeof candidate !== 'function') {
    throw new TypeError(`Graph helper '${key}' is not available`)
  }
  return candidate as DynamicFactory
}

/** Bind generated graph query/report/command factories to a client instance. */
export function bindGraph<G extends object>(
  client: MykoClient,
  graph: G,
): BoundGraph<G> {
  const queryState = (key: string, args: unknown[], options?: QueryWatchOptions) =>
    client.watchQueryState(
      factory(graph, key)(...args) as Query<unknown> & {
        $res?: () => { id: string }[]
      },
      options,
    )
  const reportState = (key: string, args: unknown[]) =>
    client.watchReport(factory(graph, key)(...args) as Report<unknown>)
  const command = (key: string, args: unknown[]) =>
    client.sendCommand(factory(graph, key)(...args) as Command<unknown>)

  const endpoint = (
    edgeKey: string,
    relatedKey: string,
    relatedName: 'targets' | 'sources',
    countKey: string | null,
    args: unknown[],
  ) => {
    const scope: Record<string, unknown> = {
      edges: (options?: QueryWatchOptions) => queryState(edgeKey, args, options),
    }
    if (relatedKey in graph) {
      scope[relatedName] = (options?: QueryWatchOptions) =>
        queryState(relatedKey, args, options)
    }
    if (countKey) {
      scope.count = () => reportState(countKey, args)
    }
    return scope
  }

  const bound: Record<string, unknown> = {
    from: (value: unknown) =>
      endpoint('from', 'targetsFrom', 'targets', 'countFrom', [value]),
    to: (value: unknown) =>
      endpoint('to', 'sourcesTo', 'sources', 'countTo', [value]),
    between: (a: unknown, b: unknown) => ({
      edges: (options?: QueryWatchOptions) =>
        queryState('between', [a, b], options),
      count: () => reportState('countBetween', [a, b]),
      exists: () => reportState('existsBetween', [a, b]),
    }),
    connect: (edge: unknown) => command('connect', [edge]),
    connectMany: (edges: unknown) => command('connectMany', [edges]),
    ensure: (edge: unknown) => command('ensure', [edge]),
    disconnect: (id: unknown) => command('disconnect', [id]),
    disconnectMany: (ids: unknown) => command('disconnectMany', [ids]),
  }
  if ('fromMany' in graph) {
    bound.fromMany = (values: unknown) =>
      endpoint('fromMany', 'targetsFromMany', 'targets', null, [values])
    bound.toMany = (values: unknown) =>
      endpoint('toMany', 'sourcesToMany', 'sources', null, [values])
  }
  return bound as BoundGraph<G>
}
