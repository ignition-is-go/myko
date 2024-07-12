import { filter } from 'rxjs'
import { Repo, type RepoOptions } from '../aggregates/repo'
import { eventBus } from '../busses'
import { MYKO_ITEM_TYPE } from '../constants'
import type { PersisterFactory } from '../persisters'
import type { MEvent, MItem, MItemConstructor, Stream } from '../types'
import { relationRegistry } from './relation.registry'

const repos = new Map<string, Repo<MItem>>()
const searchKeys = new Map<string, string[]>()

let defaultOpts: {
  defaultPersisterFactory?: PersisterFactory
} = {}

const needsPersister: string[] = []

export const setDefaultRepoOptions = (args: {
  persisterFactory?: PersisterFactory
}) => {
  defaultOpts.defaultPersisterFactory = args.persisterFactory

  needsPersister.forEach((itemName) => {
    createRepo(itemName, buildRepoOptions(itemName))
  })
}

export const repo = <T extends MItem>(item: MItemConstructor<T>): Repo<T> => {
  const itemName = Reflect.getMetadata(MYKO_ITEM_TYPE, item)

  if (!itemName) {
    throw new Error('No item name found')
  }

  return repoName(itemName)
}

export const initRepo = <T extends MItem>(item: MItemConstructor<T>) => {
  const itemName = Reflect.getMetadata(MYKO_ITEM_TYPE, item)

  if (!itemName) {
    throw new Error('No item name found')
  }

  createRepo(itemName, buildRepoOptions(itemName))
}

export const repoName = <T extends MItem>(itemName: string): Repo<T> => {
  if (!itemName) {
    throw new Error('No item name found')
  }

  if (repos.has(itemName)) {
    return repos.get(itemName) as unknown as Repo<T>
  }

  const err = `Repo not found for ${itemName}`
  throw new Error(err)
}

const createRepo = <T extends MItem>(
  itemName: string,
  options: RepoOptions<T>,
) => {
  const persister = defaultOpts?.defaultPersisterFactory?.(itemName)

  if (!persister) {
    needsPersister.push(itemName)
    return
  }

  const newRepo = new Repo<T>(itemName, {
    stream: persister?.output.pipe(
      filter((x) => x.itemType === itemName),
    ) as Stream<T>,
    ...options,
  })
  if (persister) {
    eventBus.subject$
      .pipe(filter((x) => x.itemType === itemName))
      .subscribe((e) => persister.persist(e as MEvent<T>))
  }

  repos.set(itemName, newRepo as unknown as Repo<MItem>)
}

export const addSearchProperty = <T extends MItem>(
  item: MItemConstructor<T>,
  property: string,
) => {
  const itemName = Reflect.getMetadata(MYKO_ITEM_TYPE, item)

  if (!itemName) {
    throw new Error('No item name found')
  }

  if (!searchKeys.has(itemName)) {
    searchKeys.set(itemName, [])
  }

  searchKeys.get(itemName)?.push(property)
}

export const buildRepoOptions = <T extends MItem>(
  itemName: string,
): RepoOptions<T> => {
  const searchKeys = []
  relationRegistry.forEach((relation) => {
    if (relation.type === 'searchable' && relation.localType === itemName)
      searchKeys.push(relation.localKey)
  })

  return {
    searchIndeces: searchKeys,
  } satisfies RepoOptions<T>
}
