import 'reflect-metadata'
import { filter, from, mergeMap, Observable, Subscription, tap } from 'rxjs'
import {
  MItem,
  MEvent,
  MSaga,
  MCommand,
  Type,
  makeSet,
  makeDel,
  MEventType,
  recalculateHash,
  ID,
} from '../types'
import { AMykoCommandBus } from './command.bus'
import { ObservableBus } from './observable.bus'

import {
  getFilters,
  getIds,
  relationRegistry,
  propertyDefaults,
} from '../registry'
import { onInit } from '../hooks'
import { v4 as uuid } from 'uuid'

export type MykoSagaType = Type<MSaga>

export abstract class AMykoEventBus extends ObservableBus<MEvent> {
  constructor(private commandBus: AMykoCommandBus) {
    super()
    this.subscriptions = []
    this.establishRelations()
  }

  private establishRelations() {
    relationRegistry.forEach((relation) => {
      switch (relation.type) {
        case 'belongs-to': {
          const sub = this.subject$
            .pipe(
              filter(
                (e) =>
                  e.changeType === MEventType.DEL &&
                  e.itemType === relation.foreignType,
              ),
            )
            .subscribe((e) => {
              const ff = getFilters.get(relation.localType)

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
          const forwardDeletes = this.subject$
            .pipe(
              filter(
                (e) =>
                  e.changeType === MEventType.DEL &&
                  e.itemType === relation.localType,
              ),
            )
            .subscribe((e) => {
              const ids = getIds.get(relation.foreignType)

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

              const getForeign = getFilters.get(foreignType)
              if (!getForeign) {
                throw new Error(`No Foreign getFilters for ${foreignType}`)
              }

              return {
                name: foreignType,
                values: getForeign((f) => true),
              }
            })

            const getLocal = getFilters.get(localType)
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
                console.log(combination)
                console.log(localType, newItem)
                this.publishSet(newItem, tx)
              }
            })
          }

          const ensureDeps = this.subject$
            .pipe(
              filter(
                (e) =>
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

  publishSet<T extends MItem>(item: T, tx: ID) {
    recalculateHash(item)
    this.publish(makeSet(item, tx))
  }

  publishDel<T extends MItem>(item: T, tx: ID) {
    recalculateHash(item)
    this.publish(makeDel(item, tx))
  }

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
    const stream$ = saga(this.subject$.pipe())
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
          console.error(`Error in Command Handler executed by Saga`)
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
