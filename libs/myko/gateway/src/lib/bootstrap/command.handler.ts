import {
  commandBus,
  MykoCommandError,
  MykoLogger,
  unwrapCommand,
  type ID,
  type MCommand,
  type MWrappedCommand,
} from '@myko/core'
import {
  wrapCommandErrorWS,
  wrapCommandResponseWS,
  type WSMMessage,
} from '@myko/ws'
import type { Subject } from 'rxjs'
import { CommandNotAuthorized } from '../exceptions'
import { getAuth } from '../registry'

export const handleCommand = async (
  clientId: ID,
  command: MWrappedCommand,
  respond: Subject<{ clientId: ID; data: WSMMessage }>,
) => {
  const txid = command.command.tx

  const auth = getAuth()

  if (auth) {
    const res = await auth.canActivate(command.command.userToken).catch((e) => {
      respond.next({
        clientId,
        data: wrapCommandErrorWS(new CommandNotAuthorized(command.command.tx)),
      })
      return false
    })

    if (!res) {
      return
    }
  }

  const unwrapped = unwrapCommand(command) as MCommand<unknown>
  const res = await commandBus.execute(unwrapped).catch((e) => {
    new MykoLogger('Gateway').error(e.message, e.stack)
    if (e instanceof MykoCommandError) {
      const wrapped = wrapCommandErrorWS(e)

      respond.next({
        clientId: clientId,
        data: wrapped,
      })
    }
  })

  respond.next({
    clientId: clientId,
    data: wrapCommandResponseWS(txid, res),
  })
}
