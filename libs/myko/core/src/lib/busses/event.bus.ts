import 'reflect-metadata'
import {
  filter,
  from,
  mergeMap,
  MonoTypeOperatorFunction,
  Observable,
  OperatorFunction,
  Subscription,
  tap,
} from 'rxjs'
import {
  IMykoItem,
  IMykoEvent,
  MykoEventType,
  IMykoSaga,
  IMykoCommand,
  Constructor,
  MYKO_ITEM_TYPE,
} from '../types'
import { AMykoCommandBus } from './command.bus'
import { ObservableBus } from './observable.bus'

export type MykoSagaType = Constructor<
  IMykoSaga<IMykoEvent<IMykoItem, MykoEventType>, IMykoCommand>
>

export abstract class AMykoEventBus extends ObservableBus<
  IMykoEvent<IMykoItem, MykoEventType>
> {
  constructor(private commandBus: AMykoCommandBus) {
    super()
    this.subscriptions = []
  }

  private readonly subscriptions: Subscription[]

  abstract publish<T extends IMykoEvent<IMykoItem, MykoEventType>>(
    Event: T,
  ): Promise<void>

  protected registerSaga(
    saga: IMykoSaga<IMykoEvent<IMykoItem, MykoEventType>, IMykoCommand>,
  ) {
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
