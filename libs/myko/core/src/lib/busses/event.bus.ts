import 'reflect-metadata'
import { filter, from, mergeMap, Observable, Subscription } from 'rxjs'
import {
  MItem,
  MEvent,
  MSaga,
  MCommand,
  Type,
  makeSet,
  makeDel,
} from '../types'
import { AMykoCommandBus } from './command.bus'
import { ObservableBus } from './observable.bus'

export type MykoSagaType = Type<MSaga>

export abstract class AMykoEventBus extends ObservableBus<MEvent> {
  constructor(private commandBus: AMykoCommandBus) {
    super()
    this.subscriptions = []
  }

  private readonly subscriptions: Subscription[]

  publishSet<T extends MItem>(item: T) {
    this.publish(makeSet(item))
  }

  publishDel<T extends MItem>(item: T) {
    this.publish(makeDel(item))
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
        mergeMap((command) => from(this.commandBus.execute(command))),
      )
      .subscribe({
        error: (error) => {
          console.error(`Error in Command Handler executed by Saga`)
          throw error
        },
      })

    this.subscriptions.push(subscription)
  }
}

const isFunction = (a: any) => typeof a === 'function'
