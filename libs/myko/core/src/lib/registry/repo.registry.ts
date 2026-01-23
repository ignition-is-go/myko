import { filter } from 'rxjs'
import type { IRepo, Repo, RepoFactory } from '../aggregates/repo.abstract'
import { MYKO_ITEM_TYPE } from '../constants'
import type { Client } from '../modules'
import type { Persister, PersisterFactory } from '../persisters'
import type { IContext, MEvent, MItem, MItemConstructor } from '../types'
import { relationRegistry } from './relation.registry'

// Lazy imports to avoid circular initialization during bundling.
// The import chain: repo.registry -> history/repo.manager -> aggregates/repo.abstract -> types -> events
// triggers @MykoItem decorators which call initRepo before defaultOpts is initialized.
const getRepoWithHistory = () =>
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  (require('../history/repo.manager') as typeof import('../history/repo.manager'))
    .RepoWithHistory

const getItemName = (entity: MItemConstructor<MItem>) =>
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  (require('../types') as typeof import('../types')).getItemName(entity)

const repos = new Map<string, Repo<MItem>>()
const searchKeys = new Map<string, string[]>()

const defaultOpts: {
  defaultPersisterFactory?: PersisterFactory
  defaultRepoFactory?: RepoFactory
  persisterOverrides?: PersisterOverrideData[]
  repoOverrides?: RepoOverrideData[]
} = {}

export const getAllRepos = () => Array.from(repos.values())

const needsPersister: string[] = []

export const setDefaultRepoOptions = (args: {
  persisterFactory?: PersisterFactory
  persisterOverrides?: PersisterOverrideData[]
  repoFactory?: RepoFactory
  repoOverrides?: RepoOverrideData[]
}): void => {
  defaultOpts.defaultPersisterFactory = args.persisterFactory

  defaultOpts.persisterOverrides = [...(args?.persisterOverrides ?? [])]

  defaultOpts.defaultRepoFactory = args.repoFactory

  defaultOpts.repoOverrides = args.repoOverrides ?? [
    ...(args?.repoOverrides ?? []),
  ]

  needsPersister.forEach((itemName) => {
    createRepo(itemName)
  })
}

export const initRepo = <T extends MItem>(item: MItemConstructor<T>): void => {
  const itemName = Reflect.getMetadata(MYKO_ITEM_TYPE, item)

  if (!itemName) {
    throw new Error('No item name found')
  }

  createRepo(itemName)
}

export const repo = <T extends MItem>(
  item: MItemConstructor<T>,
  ctx: IContext,
): IRepo<T> => {
  const itemName = Reflect.getMetadata(MYKO_ITEM_TYPE, item)

  if (!itemName) {
    throw new Error('No item name found')
  }

  return repoName<T>(itemName, ctx)
}

export const repoName = <T extends MItem>(
  itemName: string,
  ctx: IContext,
): IRepo<T> => {
  const base = liveRepoName<T>(itemName)

  const RepoWithHistory = getRepoWithHistory()
  const withHistory = new RepoWithHistory(ctx, itemName, base, (clientId) =>
    liveRepoName<Client>('Client').watchId(clientId),
  )

  if (!withHistory) {
    const err = `Repo not found for ${itemName}`
    throw new Error(err)
  }

  return withHistory
}

export const liveRepo = <T extends MItem>(
  item: MItemConstructor<T>,
): Repo<T> => {
  const itemName = Reflect.getMetadata(MYKO_ITEM_TYPE, item)

  if (!itemName) {
    throw new Error('No item name found')
  }

  return liveRepoName<T>(itemName)
}

export const liveRepoName = <T extends MItem>(itemName: string): Repo<T> => {
  if (!itemName) {
    throw new Error('No item name found')
  }

  if (repos.has(itemName)) {
    return repos.get(itemName) as unknown as Repo<T>
  }

  const err = `Repo not found for ${itemName}`
  throw new Error(err)
}

// Lazy import to break circular dependency with busses module
// (eventBus imports from registry, registry imports eventBus)
const getEventBus = () =>
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  require('../busses').eventBus as import('../busses').EventBus

const createRepo = <T extends MItem>(itemName: string) => {
  const persisterFactory =
    defaultOpts.persisterOverrides?.find((x) => x.itemName === itemName)
      ?.persister ?? defaultOpts.defaultPersisterFactory

  const persister = persisterFactory?.(itemName)

  const repoFactory =
    defaultOpts.repoOverrides?.find((x) => x.itemName === itemName)?.repo ??
    defaultOpts.defaultRepoFactory

  if (!persister) {
    needsPersister.push(itemName)
    return
  }

  if (!repoFactory) {
    return
  }

  const newRepo = repoFactory(itemName, {
    persister: persister as unknown as Persister<T>,
    searchIndeces: buildSerachKeys(itemName),
  })
  if (persister) {
    getEventBus()
      .subject$.pipe(filter((x) => x.itemType === itemName))
      .subscribe((e) => persister.persist(e as MEvent<T>))
  }

  repos.set(itemName, newRepo as unknown as Repo<MItem>)
}

export const addSearchProperty = <T extends MItem>(
  item: MItemConstructor<T>,
  property: string,
): void => {
  const itemName = Reflect.getMetadata(MYKO_ITEM_TYPE, item)

  if (!itemName) {
    throw new Error('No item name found')
  }

  if (!searchKeys.has(itemName)) {
    searchKeys.set(itemName, [])
  }

  searchKeys.get(itemName)?.push(property)
}

export const buildSerachKeys = (itemName: string): any[] => {
  const searchKeys: string[] = []
  relationRegistry.forEach((relation) => {
    if (relation.type === 'searchable' && relation.localType === itemName)
      searchKeys.push(relation.localKey)
  })

  return searchKeys
}

export type PersisterOverrideData = {
  itemName: string
  persister: PersisterFactory
}

export type RepoOverrideData = {
  itemName: string
  repo: RepoFactory
}

export const persisterOverride = <T extends MItem>(
  entity: MItemConstructor<T>,
  persister: PersisterFactory,
): PersisterOverrideData =>
  ({
    itemName: getItemName(entity),
    persister,
  }) satisfies PersisterOverrideData

export const repoOverride = <T extends MItem>(
  entity: MItemConstructor<T>,
  repo: RepoFactory,
): RepoOverrideData =>
  ({
    itemName: getItemName(entity),
    repo,
  }) satisfies RepoOverrideData
