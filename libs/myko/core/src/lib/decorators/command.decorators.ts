import { MYKO_COMMAND_ID_KEY, MYKO_HANDLER_COMMAND_ID_KEY } from '../constants'
import { addCommandDoc } from '../registry'
import { MCommandResponse, MCommand, MCommandHandler } from '../types'

export const MykoCommand =
  (commandId: string) =>
  <T extends MCommand<MCommandResponse<T>>>(
    target: new (...args: any[]) => T,
  ) => {
    const original: any = target

    const commandName = Object.getOwnPropertyDescriptors(original)?.name.value

    const paramtypes =
      Reflect.getMetadata('design:paramtypes', original)?.map((x) => x.name) ??
      []

    addCommandDoc(
      {
        commandId,
        commandName,
        ctor: original,
      },
      paramtypes,
    )

    if (!commandId) {
      throw new Error('commandId is undefined')
    }

    const withType: any = function (...args: any[]) {
      const typed = new original(...args)
      Reflect.defineMetadata(MYKO_COMMAND_ID_KEY, commandId, typed)
      return typed
    }
    Reflect.defineMetadata(MYKO_COMMAND_ID_KEY, commandId, withType)
    return withType
  }

export const MykoCommandHandler = <T extends MCommand<MCommandResponse<T>>>(
  command: new (...args: any[]) => T,
) => {
  return (target: new (...args: any[]) => MCommandHandler<T>) => {
    const commandId = Reflect.getMetadata(MYKO_COMMAND_ID_KEY, command)
    Reflect.defineMetadata(MYKO_HANDLER_COMMAND_ID_KEY, commandId, target)
  }
}
