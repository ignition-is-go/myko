import { filter, Subscription } from 'rxjs'
import {
  makeDel,
  makeSet,
  MEventType,
  MItem,
  recalculateHash,
  type ID,
  type MEvent,
  type MSaga,
  type Type,
} from '../types'
import { commandBus, type AMykoCommandBus } from './command.bus'
import { ObservableBus } from './observable.bus'

import { v4 as uuid } from 'uuid'
import { onAllInit } from '../hooks'
import {
  getServer,
  propertyDefaults,
  relationRegistry,
  repoName,
} from '../registry'

export type MykoSagaType = Type<MSaga>

/**
 * Abstract class representing a custom event bus in the Myko application.
 * Extends the `ObservableBus` class and provides methods for publishing set and delete events,
 * as well as registering sagas.
 */
export abstract class AMykoEventBus extends ObservableBus<MEvent> {
  abstract getServerId(): ID

  constructor(private commandBus: AMykoCommandBus) {
    super()
    this.subscriptions = []

    onAllInit(() => {
      this.establishRelations()
    })
  }

  private establishRelations() {
    relationRegistry.forEach(async (relation) => {
      switch (relation.type) {
        case 'belongs-to': {
          const sub = this.subject$
            .pipe(
              filter(
                (e) =>
                  e.sourceId === this.getServerId() &&
                  e.changeType === MEventType.DEL &&
                  e.itemType === relation.foreignType,
              ),
            )
            .subscribe(async (e) => {
              const localRepo = repoName(relation.localType)

              const ff: typeof localRepo.getFilter =
                localRepo.getFilter.bind(localRepo)

              if (!ff) {
                throw new Error(`No Filter for ${relation.localType}`)
              }

              const affected = await ff(
                (item) => item[relation.localKey] === e.item.id,
              )

              affected.forEach((item) => {
                this.publishDel(item, e.tx)
              })
            })
          this.subscriptions.push(sub)
          break
        }

        case 'owns-many': {
          const ftRepo = repoName(relation.foreignType)

          const ltRepo = repoName(relation.localType)

          const getChildrenFilter: typeof ftRepo.getFilter =
            ftRepo.getFilter.bind(ftRepo)

          const getParentsFilter: typeof ltRepo.getFilter =
            ltRepo.getFilter.bind(ltRepo)

          if (!getChildrenFilter || !getParentsFilter) {
            console.warn(
              'missing getFilters for',
              relation.foreignType,
              relation.localType,
              relation,
            )
            return
          }

          const allParents = await getParentsFilter(() => true)
          const allChildrenIds = allParents.flatMap(
            (parent) => parent[relation.localKey],
          )

          const orphans = await getChildrenFilter(
            (child) => !allChildrenIds.includes(child.id),
          )

          if (orphans.length > 0) {
            console.log(
              orphans.length,
              'orphans found for',
              relation.localType,
              'owns many',
              relation.foreignType,
            )

            this.publishAll(orphans.map((orphan) => makeDel(orphan, 'startup')))
          }

          const forwardDeletes = this.subject$
            .pipe(
              filter(
                (e) =>
                  e.sourceId === this.getServerId() &&
                  e.changeType === MEventType.DEL &&
                  e.itemType === relation.localType,
              ),
            )
            .subscribe(async (e) => {
              const fRepo = repoName(relation.foreignType)

              const ids: typeof fRepo.getIds = fRepo.getIds.bind(fRepo)

              if (!ids) {
                throw new Error(`No getIds for ${relation.foreignType}`)
              }

              const affected = await ids(e.item[relation.localKey])

              affected.forEach((item) => {
                this.publishDel(item, e.tx)
              })
            })
          this.subscriptions.push(forwardDeletes)

          const cleanupIds = this.subject$
            .pipe(
              filter(
                (e) =>
                  e.sourceId === this.getServerId() &&
                  e.changeType === MEventType.DEL &&
                  e.itemType === relation.foreignType,
              ),
            )
            .subscribe(async (event) => {
              const localRepo = repoName(relation.localType)
              const ff: typeof localRepo.getFilter =
                localRepo.getFilter.bind(localRepo)
              const affected = await ff((e) =>
                e[relation.localKey].includes(event.item.id),
              )

              affected.forEach((item) => {
                const newIds = item[relation.localKey].filter(
                  (id) => id !== event.item.id,
                )

                item[relation.localKey] = newIds

                recalculateHash(item)

                this.publishSet(item, event.tx)
              })
            })

          this.subscriptions.push(cleanupIds)
          break
        }

        case 'ensure-for': {
          const { dependencies, localType } = relation

          const foreignTypes = dependencies.map((d) => d.foreignType)

          const ensure = async (
            tx: ID,
            override?: { name: string; value: MItem },
          ) => {
            const foreigns = await Promise.all(
              foreignTypes.map(async (foreignType) => {
                if (override && override.name === foreignType) {
                  return {
                    name: foreignType,
                    values: [override.value],
                  }
                }

                const fr = repoName(foreignType)
                const getForeign: typeof fr.getFilter = fr.getFilter.bind(fr)

                if (!getForeign) {
                  throw new Error(`No Foreign getFilters for ${foreignType}`)
                }

                return {
                  name: foreignType,
                  values: await getForeign((f) => true),
                }
              }),
            )

            const localRepo = repoName(localType)
            const getLocal: typeof localRepo.getFilter =
              localRepo.getFilter.bind(localRepo)

            if (!getLocal) {
              throw new Error(`No Local getFilters for ${localType}`)
            }

            const multiplex = getCombinations(foreigns)

            multiplex.forEach(async (combination) => {
              const exists = await getLocal((item) =>
                dependencies.every(
                  (d) =>
                    item[d.localKey] ===
                    combination[d.foreignType][d.foreignKey],
                ),
              )

              if (exists.length === 0) {
                const props = dependencies.reduce((acc, d) => {
                  acc[d.localKey] = combination[d.foreignType][d.foreignKey]
                  return acc
                }, {} as any)

                propertyDefaults
                  .get(localType)
                  ?.forEach((value, propertyKey) => {
                    props[propertyKey] = value
                  })

                const newItem = new relation.makeDefault({
                  id: uuid(),
                  ...props,
                })

                this.publishSet(newItem, tx)
              }
            })
          }

          const ensureDeps = this.subject$
            .pipe(
              filter(
                (e) =>
                  e.sourceId === this.getServerId() &&
                  e.changeType === MEventType.SET &&
                  dependencies.some((d) => d.foreignType === e.itemType),
              ),
            )
            .subscribe((event) => {
              ensure(event.tx, { name: event.itemType, value: event.item })
            })

          this.subscriptions.push(ensureDeps)

          ensure('server-init')
          break
        }
      }
    })
  }

  private readonly subscriptions: Subscription[]

  /**
   * Publishes a set event for an item to the event bus.
   *
   * @template T - The type of the item being published.
   * @param item - The item to be published.
   * @param tx - The transaction ID.
   */
  publishSet<T extends MItem>(item: T, tx: ID) {
    recalculateHash(item)
    this.publish(makeSet(item, tx))
  }

  /**
   * Publishes a delete event for the specified item with the given transaction ID.
   * @param item The item to delete.
   * @param tx The transaction ID.
   */
  publishDel<T extends MItem>(item: T, tx: ID) {
    recalculateHash(item)
    this.publish(makeDel(item, tx))
  }

  /**
   * Publishes multiple events to the event bus.
   * @param event An array of MEvent objects representing the events to be published.
   */
  publishAll(event: MEvent[]) {
    event.forEach((e) => recalculateHash(e.item))
    event.forEach((e) => this.publish(e))
  }

  abstract publish<T extends MEvent>(Event: T): Promise<void>
}

const isFunction = (a: any) => typeof a === 'function'

const getCombinations = (arrays: { name: string; values: MItem[] }[]) => {
  const result = []

  function helper(current, index) {
    if (index === arrays.length) {
      result.push(current)
    } else {
      for (let i = 0; i < arrays[index].values.length; i++) {
        const newItem = {
          ...current,
          [arrays[index].name]: arrays[index].values[i],
        }
        helper(newItem, index + 1)
      }
    }
  }

  helper({}, 0)

  return result
}

export class EventBus extends AMykoEventBus {
  constructor(commandBus: AMykoCommandBus) {
    super(commandBus)
  }

  getServerId(): ID {
    return getServer().id
  }

  publish<T extends MEvent>(event: T): Promise<void> {
    this.subject$.next(event)
    return Promise.resolve()
  }
}

export const eventBus: EventBus = new EventBus(commandBus)
