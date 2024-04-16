import {
  AMykoEventBus,
  MEvent,
  MSaga,
  MYKO_SAGA_METADATA,
  MykoSagaType,
} from '@myko/core'
import { Injectable, Optional } from '@nestjs/common'
import { ModuleRef } from '@nestjs/core'
import { MykoBackplaneClient } from '../myko.backplane.client'
import { MykoCommandBus } from './command.bus'

@Injectable()
export class MykoEventBus extends AMykoEventBus {
  constructor(
    private moduleRef: ModuleRef,
    commandBus: MykoCommandBus,
    @Optional()
    private backplane?: MykoBackplaneClient,
  ) {
    super(commandBus)
  }

  async publish<T extends MEvent>(event: T): Promise<void> {
    if (!event.sourceId && !!this.serverId) {
      Reflect.set(event, 'sourceId', this.serverId)
    }
    this.subject$.next(event)
    if (this.backplane) this.backplane.publishEvent(event)
    return
  }

  public registerSagas(types: MykoSagaType[]) {
    const sagas: MSaga[] = types
      .map((target) => {
        const metadata = Reflect.getMetadata(MYKO_SAGA_METADATA, target) || []
        const instance = this.moduleRef.get(target, { strict: false })
        if (!instance) {
          throw new Error(
            'Cannot Register Saga - Must return Observable of Commands',
          )
        }
        return metadata.map((key: string) => instance[key].bind(instance))
      })
      .reduce((a, b) => a.concat(b), [])

    sagas.forEach((saga) => this.registerSaga(saga))
  }
}
