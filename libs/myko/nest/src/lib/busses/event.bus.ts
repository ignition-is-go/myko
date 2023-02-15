import { Injectable } from '@nestjs/common'
import {
  AMykoEventBus,
  IMykoCommand,
  IMykoEvent,
  IMykoItem,
  IMykoSaga,
  makeDel,
  makeSet,
  MykoEventType,
  MykoSagaType,
  MYKO_SAGA_METADATA,
} from '@myko/core'
import { ModuleRef } from '@nestjs/core'
import { MykoCommandBus } from './command.bus'

@Injectable()
export class MykoEventBus extends AMykoEventBus {
  constructor(private moduleRef: ModuleRef, commandBus: MykoCommandBus) {
    super(commandBus)
  }

  publish<T extends IMykoEvent<IMykoItem, MykoEventType>>(
    event: T,
  ): Promise<void> {
    this.subject$.next(event)
    return
  }

  publishSet<T extends IMykoItem>(item: T) {
    this.publish(makeSet(item))
  }

  publishDel<T extends IMykoItem>(item: T) {
    this.publish(makeDel(item))
  }

  public registerSagas(types: MykoSagaType[]) {
    const sagas: IMykoSaga<
      IMykoEvent<IMykoItem, MykoEventType>,
      IMykoCommand
    >[] = types
      .map((target) => {
        const metadata = Reflect.getMetadata(MYKO_SAGA_METADATA, target) || []
        const instance = this.moduleRef.get(target, { strict: false })
        if (!instance) {
          throw new Error(
            'Cannot Register Saga - Must retrun Observablde of Commands',
          )
        }
        return metadata.map((key: string) => instance[key].bind(instance))
      })
      .reduce((a, b) => a.concat(b), [])

    sagas.forEach((saga) => this.registerSaga(saga))
  }
}
