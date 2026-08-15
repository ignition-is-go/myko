import { map, type Observable } from 'rxjs'

import type {
  Command,
  CommandOptions,
  CommandResult,
  MykoClient,
  Query,
  QuerySelectionOptions,
  QueryWindow,
  QueryWindowInfo,
  QueryWatchOptions,
  Report,
  ReportResult,
} from './client.js'
import { type LiveCollection, LiveIndex } from './live-collection.js'

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

type QueryEntity<Q> = Q extends Query<infer T>
  ? T extends { id: string }
    ? T
    : never
  : never

type QueryState<Q> = Observable<LiveCollection<QueryEntity<Q>>>

type QueryIndexState<Q, K> = Observable<LiveIndex<K, QueryEntity<Q>>>

type SelectQueryState<Q> = <S>(
  select: (state: LiveCollection<QueryEntity<Q>>) => S,
  options?: QuerySelectionOptions<S>,
) => Observable<S>

type QueryItemState<Q> = Observable<QueryEntity<Q> | undefined>

/** Mutable bounded graph query backed by the ordinary query-window protocol. */
export type WindowedGraphQuery<Q> = {
  tx: string
  results$: Observable<QueryEntity<Q>[]>
  windowInfo$: Observable<QueryWindowInfo>
  setWindow: (window: QueryWindow | null) => void
}

export type GraphDescriptorSchema = {
  pairProjection: 'intersectAdjacency' | 'eager'
  aAdjacency: 'demandDriven' | 'eager'
  bAdjacency: 'demandDriven' | 'eager'
}

/** Static execution strategy derived from the generated graph schema. */
export type GraphQueryPlan = {
  readonly strategy:
    | 'eagerEndpoint'
    | 'eagerPair'
    | 'adjacencyFilter'
    | 'demandDrivenScan'
    | 'unknown'
  readonly initialization: 'indexLookup' | 'canonicalScan' | 'unknown'
  readonly liveUpdates: 'routed' | 'unknown'
  readonly windowing: 'pushdown' | 'materialized' | 'unknown'
  readonly reason: string
}

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
    } & {
      [P in `${Name}Windowed`]: (
        window: QueryWindow,
      ) => WindowedGraphQuery<MethodResult<G, K>>
    } & {
      [P in `select${Capitalize<Name>}`]: SelectQueryState<MethodResult<G, K>>
    } & {
      [P in Name extends 'targets' ? 'target' : 'source']: (
        id: string,
        options?: QueryWatchOptions,
      ) => QueryItemState<MethodResult<G, K>>
    } & {
      [P in Name extends 'targets' ? 'hasTarget' : 'hasSource']: (
        id: string,
        options?: QueryWatchOptions,
      ) => Observable<boolean>
    }
  : object

type EndpointScope<
  G,
  EdgeKey extends PropertyKey,
  RelatedKey extends PropertyKey,
  RelatedName extends string,
  CountKey extends PropertyKey,
> = {
  readonly plan: GraphQueryPlan
  edges: (options?: QueryWatchOptions) => QueryState<MethodResult<G, EdgeKey>>
  selectEdges: SelectQueryState<MethodResult<G, EdgeKey>>
  edge: (
    id: string,
    options?: QueryWatchOptions,
  ) => QueryItemState<MethodResult<G, EdgeKey>>
  hasEdge: (id: string, options?: QueryWatchOptions) => Observable<boolean>
  edgesWindowed: (
    window: QueryWindow,
  ) => WindowedGraphQuery<MethodResult<G, EdgeKey>>
  edgesBy: <K>(
    keyOf: (edge: QueryEntity<MethodResult<G, EdgeKey>>) => K,
    options?: QueryWatchOptions,
  ) => QueryIndexState<MethodResult<G, EdgeKey>, K>
} & (CountKey extends keyof G
  ? { count: () => ReportState<MethodResult<G, CountKey>> }
  : object) &
  RelatedScope<G, RelatedKey, RelatedName>

type BetweenScope<G> = {
  readonly plan: GraphQueryPlan
  edges: (options?: QueryWatchOptions) => QueryState<MethodResult<G, 'between'>>
  selectEdges: SelectQueryState<MethodResult<G, 'between'>>
  edge: (
    id: string,
    options?: QueryWatchOptions,
  ) => QueryItemState<MethodResult<G, 'between'>>
  hasEdge: (id: string, options?: QueryWatchOptions) => Observable<boolean>
  edgesWindowed: (
    window: QueryWindow,
  ) => WindowedGraphQuery<MethodResult<G, 'between'>>
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
  readonly schema: GraphDescriptorSchema | null
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
    options?: CommandOptions,
  ) => CommandPromise<MethodResult<G, 'connect'>>
  connectMany: (
    edges: MethodFirstArg<G, 'connectMany'>,
    options?: CommandOptions,
  ) => CommandPromise<MethodResult<G, 'connectMany'>>
  ensure: (
    edge: MethodFirstArg<G, 'ensure'>,
    options?: CommandOptions,
  ) => CommandPromise<MethodResult<G, 'ensure'>>
  disconnect: (
    id: MethodFirstArg<G, 'disconnect'>,
    options?: CommandOptions,
  ) => CommandPromise<MethodResult<G, 'disconnect'>>
  disconnectMany: (
    ids: MethodFirstArg<G, 'disconnectMany'>,
    options?: CommandOptions,
  ) => CommandPromise<MethodResult<G, 'disconnectMany'>>
} & BatchScopes<G>

type DynamicFactory = (...args: unknown[]) => unknown

function descriptorSchema(graph: object): GraphDescriptorSchema | null {
  const schema = (graph as { $schema?: unknown }).$schema
  if (!schema || typeof schema !== 'object') return null
  const candidate = schema as Partial<GraphDescriptorSchema>
  if (
    (candidate.pairProjection !== 'intersectAdjacency' &&
      candidate.pairProjection !== 'eager') ||
    (candidate.aAdjacency !== 'demandDriven' &&
      candidate.aAdjacency !== 'eager') ||
    (candidate.bAdjacency !== 'demandDriven' &&
      candidate.bAdjacency !== 'eager')
  ) {
    return null
  }
  return candidate as GraphDescriptorSchema
}

function endpointPlan(
  schema: GraphDescriptorSchema | null,
  position: 'a' | 'b',
): GraphQueryPlan {
  if (!schema) {
    return {
      strategy: 'unknown',
      initialization: 'unknown',
      liveUpdates: 'unknown',
      windowing: 'unknown',
      reason: 'This graph descriptor predates generated schema metadata.',
    }
  }
  const adjacency = position === 'a' ? schema.aAdjacency : schema.bAdjacency
  if (adjacency === 'eager') {
    return {
      strategy: 'eagerEndpoint',
      initialization: 'indexLookup',
      liveUpdates: 'routed',
      windowing: 'pushdown',
      reason: `Endpoint ${position.toUpperCase()} has an eager adjacency projection.`,
    }
  }
  return {
    strategy: 'demandDrivenScan',
    initialization: 'canonicalScan',
    liveUpdates: 'routed',
    windowing: 'materialized',
    reason: `Endpoint ${position.toUpperCase()} is demand-driven; initialization scans canonical edges.`,
  }
}

function pairPlan(schema: GraphDescriptorSchema | null): GraphQueryPlan {
  if (!schema) return endpointPlan(null, 'a')
  if (schema.pairProjection === 'eager') {
    return {
      strategy: 'eagerPair',
      initialization: 'indexLookup',
      liveUpdates: 'routed',
      windowing: 'pushdown',
      reason: 'Exact endpoint pairs have an eager pair projection.',
    }
  }
  if (schema.aAdjacency === 'eager' || schema.bAdjacency === 'eager') {
    return {
      strategy: 'adjacencyFilter',
      initialization: 'indexLookup',
      liveUpdates: 'routed',
      windowing: 'materialized',
      reason: 'Exact pairs filter the available eager endpoint adjacency.',
    }
  }
  return {
    strategy: 'demandDrivenScan',
    initialization: 'canonicalScan',
    liveUpdates: 'routed',
    windowing: 'materialized',
    reason: 'Exact pairs have no eager projection; initialization scans canonical edges.',
  }
}

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
  const schema = descriptorSchema(graph)
  const plans = Object.freeze({
    a: Object.freeze(endpointPlan(schema, 'a')),
    b: Object.freeze(endpointPlan(schema, 'b')),
    pair: Object.freeze(pairPlan(schema)),
  })
  const queryState = (key: string, args: unknown[], options?: QueryWatchOptions) =>
    client.watchQueryState(
      factory(graph, key)(...args) as Query<unknown> & {
        $res?: () => { id: string }[]
      },
      options,
    )
  const reportState = (key: string, args: unknown[]) =>
    client.watchReport(factory(graph, key)(...args) as Report<unknown>)
  const querySelection = <S>(
    key: string,
    args: unknown[],
    select: (state: LiveCollection<{ id: string }>) => S,
    options?: QuerySelectionOptions<S>,
  ) =>
    client.watchQuerySelection(
      factory(graph, key)(...args) as Query<unknown> & {
        $res?: () => { id: string }[]
      },
      select,
      options,
    )
  const queryItem = (
    key: string,
    args: unknown[],
    id: string,
    options?: QueryWatchOptions,
  ) =>
    client.watchQueryItem(
      factory(graph, key)(...args) as Query<{ id: string }>,
      id,
      options,
    )
  const queryHas = (
    key: string,
    args: unknown[],
    id: string,
    options?: QueryWatchOptions,
  ) =>
    client.watchQueryHas(
      factory(graph, key)(...args) as Query<{ id: string }>,
      id,
      options,
    )
  const command = (
    key: string,
    args: unknown[],
    options?: CommandOptions,
  ) =>
    client.sendCommand(
      factory(graph, key)(...args) as Command<unknown>,
      options,
    )
  const windowed = (key: string, args: unknown[], window: QueryWindow) =>
    client.watchQueryWindowed(
      factory(graph, key)(...args) as Query<unknown> & {
        $res?: () => { id: string }[]
      },
      { window },
    )
  const queryIndex = <T extends { id: string }, K>(
    key: string,
    args: unknown[],
    keyOf: (item: T) => K,
    options?: QueryWatchOptions,
  ) => {
    const index = new LiveIndex<K, T>(keyOf)
    return queryState(key, args, options).pipe(
      map((state) => index.update(state as unknown as LiveCollection<T>)),
    )
  }

  const endpoint = (
    edgeKey: string,
    relatedKey: string,
    relatedName: 'targets' | 'sources',
    countKey: string | null,
    args: unknown[],
    plan: GraphQueryPlan,
  ) => {
    const scope: Record<string, unknown> = {
      plan,
      edges: (options?: QueryWatchOptions) => queryState(edgeKey, args, options),
      selectEdges: <S>(
        select: (state: LiveCollection<{ id: string }>) => S,
        options?: QuerySelectionOptions<S>,
      ) => querySelection(edgeKey, args, select, options),
      edge: (id: string, options?: QueryWatchOptions) =>
        queryItem(edgeKey, args, id, options),
      hasEdge: (id: string, options?: QueryWatchOptions) =>
        queryHas(edgeKey, args, id, options),
      edgesWindowed: (window: QueryWindow) => windowed(edgeKey, args, window),
      edgesBy: <T extends { id: string }, K>(
        keyOf: (item: T) => K,
        options?: QueryWatchOptions,
      ) => queryIndex(edgeKey, args, keyOf, options),
    }
    if (relatedKey in graph) {
      const capitalizedName =
        relatedName === 'targets' ? 'Targets' : 'Sources'
      const singularName = relatedName === 'targets' ? 'target' : 'source'
      scope[relatedName] = (options?: QueryWatchOptions) =>
        queryState(relatedKey, args, options)
      scope[`select${capitalizedName}`] = <S>(
        select: (state: LiveCollection<{ id: string }>) => S,
        options?: QuerySelectionOptions<S>,
      ) => querySelection(relatedKey, args, select, options)
      scope[singularName] = (id: string, options?: QueryWatchOptions) =>
        queryItem(relatedKey, args, id, options)
      scope[`has${capitalizedName.slice(0, -1)}`] = (
        id: string,
        options?: QueryWatchOptions,
      ) => queryHas(relatedKey, args, id, options)
      scope[`${relatedName}Windowed`] = (window: QueryWindow) =>
        windowed(relatedKey, args, window)
    }
    if (countKey) {
      scope.count = () => reportState(countKey, args)
    }
    return scope
  }

  const bound: Record<string, unknown> = {
    schema,
    from: (value: unknown) =>
      endpoint(
        'from',
        'targetsFrom',
        'targets',
        'countFrom',
        [value],
        plans.a,
      ),
    to: (value: unknown) =>
      endpoint(
        'to',
        'sourcesTo',
        'sources',
        'countTo',
        [value],
        plans.b,
      ),
    between: (a: unknown, b: unknown) => ({
      plan: plans.pair,
      edges: (options?: QueryWatchOptions) =>
        queryState('between', [a, b], options),
      selectEdges: <S>(
        select: (state: LiveCollection<{ id: string }>) => S,
        options?: QuerySelectionOptions<S>,
      ) => querySelection('between', [a, b], select, options),
      edge: (id: string, options?: QueryWatchOptions) =>
        queryItem('between', [a, b], id, options),
      hasEdge: (id: string, options?: QueryWatchOptions) =>
        queryHas('between', [a, b], id, options),
      edgesWindowed: (window: QueryWindow) =>
        windowed('between', [a, b], window),
      count: () => reportState('countBetween', [a, b]),
      exists: () => reportState('existsBetween', [a, b]),
    }),
    connect: (edge: unknown, options?: CommandOptions) =>
      command('connect', [edge], options),
    connectMany: (edges: unknown, options?: CommandOptions) =>
      command('connectMany', [edges], options),
    ensure: (edge: unknown, options?: CommandOptions) =>
      command('ensure', [edge], options),
    disconnect: (id: unknown, options?: CommandOptions) =>
      command('disconnect', [id], options),
    disconnectMany: (ids: unknown, options?: CommandOptions) =>
      command('disconnectMany', [ids], options),
  }
  if ('fromMany' in graph) {
    bound.fromMany = (values: unknown) =>
      endpoint(
        'fromMany',
        'targetsFromMany',
        'targets',
        null,
        [values],
        plans.a,
      )
    bound.toMany = (values: unknown) =>
      endpoint(
        'toMany',
        'sourcesToMany',
        'sources',
        null,
        [values],
        plans.b,
      )
  }
  return bound as BoundGraph<G>
}
