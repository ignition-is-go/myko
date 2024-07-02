import 'reflect-metadata'
import { filter, from, mergeMap, Observable, Subscription } from 'rxjs'
import {
  makeDel,
  makeSet,
  MEvent,
  MEventType,
  MItem,
  MSaga,
  recalculateHash,
  Type,
  type ID,
} from '../types'
import type { AMykoCommandBus } from './command.bus'
import { ObservableBus } from './observable.bus'

import { v4 as uuid } from 'uuid'
import { onInit } from '../hooks'
import {
  getFilters,
  getIds,
  inheritsRegistry,
  propertyDefaults,
  relationRegistry,
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
    this.establishRelations()
  }

  private establishRelations() {
    relationRegistry.forEach((relation) => {
      switch (relation.type) {
        case 'belongs-to': {
          // remove orphans
          // onInit([relation.localType, relation.foreignType], () => {
          //   const all = getFilters.get(relation.localType)(() => true)
          //   all.forEach((item) => {
          //     const getParents = getIds.get(relation.foreignType)
          //     if (!getParents) {
          //       console.warn('No getIds for', relation.foreignType, relation)
          //       return
          //     }
          //     const parent = getParents([item[relation.localKey]])
          //     if (parent.length < 1) {
          //       console.log(
          //         'Orphan found! Deleting',
          //         relation.localType,
          //         item.id,
          //       )
          //       this.publishDel(item, 'startup')
          //     }
          //   })
          // })

          const sub = this.subject$
            .pipe(
              filter(
                (e) =>
                  e.sourceId === this.getServerId() &&
                  e.changeType === MEventType.DEL &&
                  e.itemType === relation.foreignType,
              ),
            )
            .subscribe((e) => {
              const parentClass = inheritsRegistry.get(relation.localType)

              const ff =
                getFilters.get(relation.localType) ??
                (parentClass && getFilters.get(parentClass))

              if (!ff) {
                throw new Error(`No Filter for ${relation.localType}`)
              }

              const affected = ff(
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
          setTimeout(() => {
            const foreignParent = inheritsRegistry.get(relation.foreignType)
            const localParent = inheritsRegistry.get(relation.localType)

            const getChildrenFilter =
              getFilters.get(relation.foreignType) ??
              (foreignParent && getFilters.get(foreignParent))

            const getParentsFilter =
              getFilters.get(relation.localType) ??
              (localParent && getFilters.get(localParent))

            if (!getChildrenFilter || !getParentsFilter) {
              console.warn(
                'missing getFilters for',
                relation.foreignType,
                relation.localType,
                relation,
              )
              return
            }

            const allParents = getParentsFilter(() => true)
            const allChildrenIds = allParents.flatMap(
              (parent) => parent[relation.localKey],
            )

            const orphans = getChildrenFilter(
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

              this.publishAll(
                orphans.map((orphan) => makeDel(orphan, 'startup')),
              )
            }
          }, 2000)

          const forwardDeletes = this.subject$
            .pipe(
              filter(
                (e) =>
                  e.sourceId === this.getServerId() &&
                  e.changeType === MEventType.DEL &&
                  e.itemType === relation.localType,
              ),
            )
            .subscribe((e) => {
              const parentClass = inheritsRegistry.get(relation.foreignType)

              const ids =
                getIds.get(relation.foreignType) ??
                (parentClass && getIds.get(parentClass))

              if (!ids) {
                throw new Error(`No getIds for ${relation.foreignType}`)
              }

              const affected = ids(e.item[relation.localKey])

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
            .subscribe((event) => {
              const ff = getFilters.get(relation.localType)
              const affected = ff((e) =>
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

          const ensure = (
            tx: ID,
            override?: { name: string; value: MItem },
          ) => {
            const foreigns = foreignTypes.map((foreignType) => {
              if (override && override.name === foreignType) {
                return {
                  name: foreignType,
                  values: [override.value],
                }
              }

              const parentClass = inheritsRegistry.get(foreignType)

              const getForeign =
                getFilters.get(foreignType) ??
                (parentClass && getFilters.get(parentClass))

              if (!getForeign) {
                throw new Error(`No Foreign getFilters for ${foreignType}`)
              }

              return {
                name: foreignType,
                values: getForeign((f) => true),
              }
            })

            const parentClass = inheritsRegistry.get(localType)

            const getLocal =
              getFilters.get(localType) ??
              (parentClass && getFilters.get(parentClass))
            if (!getLocal) {
              throw new Error(`No Local getFilters for ${localType}`)
            }

            const multiplex = getCombinations(foreigns)

            multiplex.forEach((combination) => {
              const exists = getLocal((item) =>
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

          onInit([...foreignTypes, relation.localType], () => {
            ensure('server-init')
          })
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

  protected registerSaga(saga: MSaga) {
    if (!isFunction(saga)) {
      throw new Error(
        'Cannot Register Saga - Must retrun Observable of Commands',
      )
    }
    const stream$ = saga(
      this.subject$.pipe(filter((x) => x.sourceId === this.getServerId())),
    )
    if (!(stream$ instanceof Observable)) {
      throw new Error(
        'Cannot Register Saga - Must retrun Observable of Commands',
      )
    }

    const subscription = stream$
      .pipe(
        filter((e) => !!e),
        mergeMap((command) =>
          from(
            this.commandBus.execute(command).catch((e) => {
              console.error(e)
              return e
            }),
          ),
        ),
      )
      .subscribe({
        error: (error) => {
          console.error(`Error in Command Handler executed by Saga`, error)
        },
      })

    this.subscriptions.push(subscription)
  }
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
