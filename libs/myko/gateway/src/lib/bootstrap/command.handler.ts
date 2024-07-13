import {
  commandBus,
  unwrapCommand,
  type ID,
  type MCommand,
  type MWrappedCommand,
} from '@myko/core'
import {
  MykoCommandError,
  wrapCommandResponseWS,
  type WSMMessage,
} from '@myko/ws'
import type { Subject } from 'rxjs'
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
        data: new MykoCommandError(txid, e.message),
      })
      return false
    })

    if (!res) {
      return
    }
  }

  const unwrapped = unwrapCommand(command) as MCommand<unknown>
  const res = await commandBus.execute(unwrapped).catch((e) => {
    respond.next({
      clientId: clientId,
      data: new MykoCommandError(txid, e.message),
    })
  })

  respond.next({
    clientId: clientId,
    data: wrapCommandResponseWS(txid, res),
  })
}
