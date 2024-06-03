import 'reflect-metadata'
import { MYKO_COMMAND_ID_KEY } from '../constants'
import { MCommand, MCommandHandler, MCommandResponse, Type } from '../types'
import { ObservableBus } from './observable.bus'

export type MCommandHandlerType = Type<MCommandHandler<MCommand<unknown>>>

/**
 * Abstract class representing a command bus in the Myko framework.
 * A command bus is responsible for executing commands and returning their responses.
 * Subclasses of this class should implement the `registerHandler` method to bind command handlers.
 */
export abstract class AMykoCommandBus extends ObservableBus<MCommand> {
  constructor() {
    super()
  }

  /**
   * Executes a command and returns the command response.
   * @param command - The command to be executed.
   * @returns A promise that resolves to the command response.
   * @throws If a handler is not provided for the command.
   */
  async execute<T extends MCommand<MCommandResponse<T>>>(
    command: T,
  ): Promise<MCommandResponse<T>> {
    const commandId = Reflect.getMetadata(MYKO_COMMAND_ID_KEY, command)
    const handler = this.handlers.get(commandId)

    const err = `Handler not Provided for ${command.constructor.name} [${commandId}]. Check your module's providers array, and that the command is decorated with @MykoCommand(id: string)`

    if (!handler) {
      console.error(err, command)
      throw err
    }
    // console.log(commandId)
    return (await handler.execute(command)) as MCommandResponse<T>
  }

  protected handlers: Map<string, MCommandHandler<MCommand<unknown>>> =
    new Map()

  /**
   * Binds a command handler to the specified ID.
   *
   * @template T - The type of the command.
   * @param {MCommandHandler<T>} handler - The command handler to bind.
   * @param {string} id - The ID to bind the command handler to.
   * @returns {void}
   */
  bind<T extends MCommand<MCommandResponse<T>>>(
    handler: MCommandHandler<T>,
    id: string,
  ): void {
    this.handlers.set(id, handler)
  }

  /**
   * Registers the given command handlers.
   *
   * @param handlers - An array of command handlers to register.
   */
  register(handlers: MCommandHandlerType[]) {
    handlers.forEach((h) => this.registerHandler(h))
  }

  /**
   * Abstract method to register a command handler.
   * @param handler - The command handler to register.
   * @returns {void}
   */
  protected abstract registerHandler(handler: MCommandHandlerType): void
}
