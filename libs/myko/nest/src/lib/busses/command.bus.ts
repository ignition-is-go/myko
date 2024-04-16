import {
  AMykoCommandBus,
  MCommandHandlerType,
  MYKO_HANDLER_COMMAND_ID_KEY,
} from '@myko/core'
import { Injectable } from '@nestjs/common'
import { ModuleRef } from '@nestjs/core'

@Injectable()
export class MykoCommandBus extends AMykoCommandBus {
  constructor(private moduleRef: ModuleRef) {
    super()
  }

  protected registerHandler(handler: MCommandHandlerType): void {
    const instance = this.moduleRef.get(handler, {
      strict: false,
    })

    if (!instance) {
      throw new Error(`Cannot find instance of ${handler.constructor.name}`)
    }
    const commandId = Reflect.getMetadata(MYKO_HANDLER_COMMAND_ID_KEY, handler)

    this.bind(instance, commandId)
  }
}
