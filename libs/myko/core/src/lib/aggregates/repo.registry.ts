import { eventBus } from '../busses'
import { MYKO_ITEM_TYPE } from '../constants'
import type { Persister } from '../persisters'
import { Stream, ofItems, type MItem, type MItemConstructor } from '../types'
import { Repo, RepoOptions } from './repo'

const repoOptions = new Map<string, RepoOptions<MItem>>()

let defaultOpts: {
  defaultPersisterFactory?: (ent: MItemConstructor<MItem>) => Persister<MItem>
} = {}

export const setDefaultRepoOptions = (args: {
  persisterFactory?: (ent: MItemConstructor<MItem>) => Persister<MItem>
}) => {
  defaultOpts.defaultPersisterFactory = args.persisterFactory
}

const repos = new Map<string, Repo<MItem>>()

export const repo = <T extends MItem>(item: MItemConstructor<T>): Repo<T> => {
  const itemName = Reflect.getMetadata(MYKO_ITEM_TYPE, item)

  if (!itemName) {
    throw new Error('No item name found')
  }

  if (repos.has(itemName)) {
    return repos.get(itemName) as unknown as Repo<T>
  }

  const persister = defaultOpts?.defaultPersisterFactory?.(item)

  const newRepo = new Repo<T>(item, {
    stream: persister?.output.pipe(ofItems(item)) as Stream<T>,
  })

  if (persister) {
    eventBus.subject$.pipe(ofItems(item)).subscribe((e) => persister.persist(e))
  }

  repos.set(itemName, newRepo as unknown as Repo<MItem>)

  return newRepo
}
