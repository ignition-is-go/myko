import { Inject, Injectable } from '@nestjs/common'
import {
  AMykoEventBus,
  MEvent,
  MSaga,
  MykoSagaType,
  MYKO_SAGA_METADATA,
  Server,
  ID,
} from '@myko/core'
import { ModuleRef } from '@nestjs/core'
import { MykoCommandBus } from './command.bus'
import { WebSocket } from 'ws'
import { MykoBackplaneClient } from '../myko.backplane.client'

@Injectable()
export class MykoEventBus extends AMykoEventBus {
  constructor(
    private moduleRef: ModuleRef,
    commandBus: MykoCommandBus,
    private backplane: MykoBackplaneClient,
  ) {
    super(commandBus)
    try {
      this.connect()
    } catch {}
  }

  private async connect() {
    try {
      this.client = new WebSocket('ws://127.0.0.1:5156')
    } catch (e) {
      console.log(e)
      setTimeout(() => {
        this.connect()
      }, 1000)
    }

    this.client.on('open', () => {
      console.log('connected')
    })

    this.client.on('close', () => {
      setTimeout(() => {
        this.connect()
      }, 1000)
    })
  }

  async publish<T extends MEvent>(event: T): Promise<void> {
    if (!event.sourceId && !!this.serverId) {
      Reflect.set(event, 'sourceId', this.serverId)
    }
    this.subject$.next(event)
    this.backplane.publishEvent(event)
    return
  }

  public registerSagas(types: MykoSagaType[]) {
    const sagas: MSaga[] = types
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
