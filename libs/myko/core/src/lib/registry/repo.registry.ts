import { filter } from 'rxjs'
import { Repo } from '../aggregates/repo'
import { eventBus } from '../busses'
import { MYKO_ITEM_TYPE } from '../constants'
import type { PersisterFactory } from '../persisters'
import type { MEvent, MItem, MItemConstructor, Stream } from '../types'
import { getServer } from './self.registry'

const repos = new Map<string, Repo<MItem>>()

let defaultOpts: {
  defaultPersisterFactory?: PersisterFactory
} = {}

const needsPersister: string[] = []

export const setDefaultRepoOptions = (args: {
  persisterFactory?: PersisterFactory
}) => {
  defaultOpts.defaultPersisterFactory = args.persisterFactory

  needsPersister.forEach((itemName) => {
    createRepo(itemName)
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

  createRepo(itemName)
}

export const repoName = <T extends MItem>(itemName: string): Repo<T> => {
  if (!itemName) {
    throw new Error('No item name found')
  }

  if (repos.has(itemName)) {
    return repos.get(itemName) as unknown as Repo<T>
  }

  throw new Error('Repo not found')
}

const createRepo = <T extends MItem>(itemName: string) => {
  const persister = defaultOpts?.defaultPersisterFactory?.(
    itemName,
    getServer(),
  )

  if (!persister) {
    needsPersister.push(itemName)
    return
  }

  const newRepo = new Repo<T>(itemName, {
    stream: persister?.output.pipe(
      filter((x) => x.itemType === itemName),
    ) as Stream<T>,
  })
  if (persister) {
    eventBus.subject$
      .pipe(filter((x) => x.itemType === itemName))
      .subscribe((e) => persister.persist(e as MEvent<T>))
  }

  repos.set(itemName, newRepo as unknown as Repo<MItem>)
}
