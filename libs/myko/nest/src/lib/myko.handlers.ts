import { MCommandHandler, MykoCommandHandler } from '@myko/core'
import { ClientCommand, wrapCommandWS } from '@myko/ws'
import { SocketRegistry } from '../types'

@MykoCommandHandler(ClientCommand)
export class ClientCommandHandler implements MCommandHandler<ClientCommand> {
  constructor(private reg: SocketRegistry) {}

  async execute(command: ClientCommand): Promise<void> {
    const socket = this.reg.get(command.clientId)

    if (!socket) {
      console.log(command)
      return
    }

    return socket.send(
      JSON.stringify(wrapCommandWS(command.command, 'rship-server')),
    )
  }
}
