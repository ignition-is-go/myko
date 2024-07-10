import { filter } from 'rxjs'
import { Repo, RepoOptions } from '../aggregates/repo'
import { eventBus } from '../busses'
import { MYKO_ITEM_TYPE } from '../constants'
import type { PersisterFactory } from '../persisters'
import type { MEvent, MItem, MItemConstructor, Stream } from '../types'
import { getServer } from './self.registry'

const repoOptions = new Map<string, RepoOptions<MItem>>()

let defaultOpts: {
  defaultPersisterFactory?: PersisterFactory
} = {}

export const setDefaultRepoOptions = (args: {
  persisterFactory?: PersisterFactory
}) => {
  defaultOpts.defaultPersisterFactory = args.persisterFactory
}

const repos = new Map<string, Repo<MItem>>()

export const repo = <T extends MItem>(item: MItemConstructor<T>): Repo<T> => {
  const itemName = Reflect.getMetadata(MYKO_ITEM_TYPE, item)

  if (!itemName) {
    throw new Error('No item name found')
  }

  console.log('REPO FOR', itemName)
  return repoName(itemName)
}

export const repoName = <T extends MItem>(itemName: string): Repo<T> => {
  if (!itemName) {
    throw new Error('No item name found')
  }

  if (repos.has(itemName)) {
    return repos.get(itemName) as unknown as Repo<T>
  }

  const persister = defaultOpts?.defaultPersisterFactory?.(
    itemName,
    getServer(),
  )

  console.log('PERSISTER for ', itemName)

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

  return newRepo
}
