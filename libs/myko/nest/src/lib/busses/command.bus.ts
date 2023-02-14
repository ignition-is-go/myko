import { Injectable } from '@nestjs/common'
import {
  AMykoCommandBus,
  IMykoCommand,
  MykoCommandHandlerType,
  MYKO_COMMAND_ID_KEY,
  MYKO_HANDLER_COMMAND_ID_KEY,
} from '@myko/core'
import { ModuleRef } from '@nestjs/core'
import { LoggerService } from '@rship/logging'

@Injectable()
export class MykoCommandBus extends AMykoCommandBus {
  constructor(private moduleRef: ModuleRef, private logger: LoggerService) {
    super()
  }

  execute<T extends IMykoCommand, R = any>(command: T): Promise<R> {
    const commandId = Reflect.getMetadata(MYKO_COMMAND_ID_KEY, command)
    const handler = this.handlers.get(commandId)

    const err = `Handler not Provided for ${command.constructor.name} [${commandId}]. Check your module's providers array, and that the command is decorated with @MykoCommand(id: string)`

    if (!handler) {
      this.logger
        .getLogger('CommandBus')
        .dev.error({ message: err, data: command })
      return
    }

    handler.execute(command)
    return
  }

  protected registerHandler(handler: MykoCommandHandlerType): void {
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
