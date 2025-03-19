import {
  commandBus,
  getHistoryProvider,
  MCommand,
  MykoCommandError,
  MykoLogger,
  type ID,
  type MWrappedCommand,
} from '@myko/core'
import {
  wrapCommandErrorWS,
  wrapCommandResponseWS,
  wrapError,
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
  const userToken = command.command.userToken

  const auth = getAuth()

  const fail = () => {
    respond.next({
      clientId,
      data: wrapCommandErrorWS(new CommandNotAuthorized(command.command.tx)),
    })
  }

  if (auth) {
    if (!userToken) {
      fail()
      return false
    }
    const res = await auth.canActivate(userToken).catch((_) => {
      fail()
      return false
    })

    if (!res) {
      fail()
      return
    }

    const userId = await auth.getUserId(userToken)
    if (userId) {
      getHistoryProvider().recordUserTransaction(userId, txid)
    }
  }

  const unwrapped = MCommand.fromWrappedCommand(command).withContext({
    commandClientId: clientId,
    tx: txid,
  })
  // unwrapCommand(command) as MCommand<unknown>
  await commandBus
    .execute(unwrapped)
    .then((res) => {
      respond.next({
        clientId: clientId,
        data: wrapCommandResponseWS(txid, res),
      })
    })
    .catch((e) => {
      new MykoLogger('Gateway').error(e.message, e.stack)
      if (e instanceof MykoCommandError) {
        const wrapped = wrapCommandErrorWS(e)
        respond.next({
          clientId: clientId,
          data: wrapped,
        })
      } else {
        const wrapped = wrapError(e, txid)
        respond.next({
          clientId: clientId,
          data: wrapped,
        })
      }
    })
}
