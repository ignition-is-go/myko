import {
  Client,
  commandBus,
  getHistoryProvider,
  liveRepo,
  MCommand,
  MykoCommandError,
  MykoLogger,
  noAuthCommands,
  type ID,
  type MWrappedCommand,
} from '@myko/core'
import { allowedDuringWindback } from '@myko/core/src/lib/registry/windback.registry'
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
  const txId = command.command.tx

  const fail = () => {
    respond.next({
      clientId,
      data: wrapCommandErrorWS(new CommandNotAuthorized(command.command.tx)),
    })
  }

  const proceedRes = await proceed(fail, respond, clientId, command)

  if (!proceedRes) {
    return
  }

  const unwrapped = MCommand.fromWrappedCommand(command).withContext({
    commandClientId: command.command.commandClientId ?? clientId,
    tx: txId,
    lineage: ['client'],
  })
  // unwrapCommand(command) as MCommand<unknown>
  await commandBus
    .execute(unwrapped)
    .then((res) => {
      respond.next({
        clientId: clientId,
        data: wrapCommandResponseWS(txId, res),
      })
    })
    .catch((e) => {
      new MykoLogger(command.commandId).error(
        e.message,
        e.stack,
        command.command.tx,
      )
      if (e instanceof MykoCommandError) {
        const wrapped = wrapCommandErrorWS(e)
        respond.next({
          clientId: clientId,
          data: wrapped,
        })
      } else {
        const wrapped = wrapError(e, txId)
        respond.next({
          clientId: clientId,
          data: wrapped,
        })
      }
    })
}

const proceed = async (
  fail: () => void,
  respond: Subject<{ clientId: ID; data: WSMMessage }>,
  clientId: ID,
  command: MWrappedCommand,
) => {
  const txId = command.command.tx
  const userToken = command.command.userToken
  const logger = new MykoLogger('CommandAuth')

  const auth = getAuth()

  if (!auth) {
    return true
  }

  const pub = noAuthCommands.has(command.commandId)

  if (pub) {
    return true
  }

  if (!userToken) {
    logger.warn(
      `Command ${command.commandId} rejected: No user token provided`,
      { clientId, txId },
    )
    fail()
    return false
  }
  const res = await auth.canActivate(userToken).catch((error) => {
    logger.error(`Command ${command.commandId} auth failed: ${error.message}`, {
      clientId,
      txId,
      stack: error.stack,
    })
    fail()
    return false
  })

  if (!res) {
    logger.warn(
      `Command ${command.commandId} rejected: canActivate returned false`,
      { clientId, txId },
    )
    fail()
    return
  }

  const userId = await auth.getUserId(userToken)
  if (userId) {
    try {
      getHistoryProvider().recordUserTransaction(userId, txId)
    } catch (_e) {}
  }

  const client = await liveRepo(Client).getId(clientId)

  if (!client) {
    respond.next({
      clientId,

      data: wrapCommandErrorWS(new MykoCommandError(txId, 'Client not found')),
    })
    return
  }

  if (!!client.windback && !allowedDuringWindback.has(command.commandId)) {
    respond.next({
      clientId,
      data: wrapCommandErrorWS(
        new MykoCommandError(txId, 'Client is in windback mode'),
      ),
    })
    return
  }

  return true
}
