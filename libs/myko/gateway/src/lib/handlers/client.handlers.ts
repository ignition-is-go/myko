import {
  ClearClientWindbackTime,
  Client,
  ClientCommand,
  ClientStatus,
  ContextPhantom,
  DeleteClientsByServerId,
  eventBus,
  GetClientsByIds,
  GetClientsByQuery,
  getHistoryProvider,
  getHostId,
  liveRepo,
  makeDel,
  makeSet,
  MCommand,
  MEventType,
  MItem,
  MSagaHandler,
  MykoCommandError,
  MykoCommandHandler,
  MykoLogger,
  MykoQueryHandler,
  MykoReportHandler,
  MykoSaga,
  ofItems,
  ofType,
  queryBus,
  SetClientWindbackTime,
  WindbackStatus,
  type MCommandHandler,
  type MEventStream,
  type MLiveReportResult,
  type MQueryHandler,
  type MReportHandler,
} from '@myko/core'
import { wrapCommandOnlyWS } from '@myko/ws'
import { ok } from 'assert'
import {
  EMPTY,
  filter,
  firstValueFrom,
  map,
  shareReplay,
  switchMap,
  take,
  takeUntil,
  tap,
  timeout,
  TimeoutError,
  type Observable,
} from 'rxjs'
import { getRx, getTx, peers } from '../registry'
import { clientCommandMonitor as ccm } from '../telemetry/client_command.monitor'

const inflightByClient = new Map<string, number>()
const getMaxInflightPerClient = (() => {
  let cached: number | undefined
  return () => {
    try {
      if (cached !== undefined) return cached
      const v = process?.env?.['MYKO_CCMD_MAX_INFLIGHT_PER_CLIENT']
      const n = Number(v)
      cached = Number.isFinite(n) && n > 0 ? n : undefined
      return cached
    } catch {
      return undefined
    }
  }
})()

const CCMD_LOGGER = new MykoLogger('ClientCommandHandler')

@MykoCommandHandler(ClientCommand)
export class ClientCommandHandler implements MCommandHandler<ClientCommand> {
  async execute(command: ClientCommand) {
    const logger = CCMD_LOGGER
    const startAt = Date.now()

    const CCMD_TIMEOUT_MS = (() => {
      try {
        const v = process?.env?.['MYKO_CCMD_TIMEOUT_MS']
        const n = Number(v)
        return Number.isFinite(n) && n > 0 ? n : undefined
      } catch {
        return undefined
      }
    })()
    const innerWrappedCommand = command.command
    const effectiveCommand = innerWrappedCommand.command

    logger.verbose('Begin client command', {
      tag: innerWrappedCommand.commandId,
      tx: effectiveCommand.tx,
      clientId: command.client?.id,
      serverId: command.client?.serverId,
    })

    if (!command.client) {
      throw new MykoCommandError(command.tx, 'Client Not Found')
    }

    if (command.client.serverId !== getHostId()) {
      // forward to server
      logger.verbose('Forwarding to peer', {
        tag: innerWrappedCommand.commandId,
        tx: effectiveCommand.tx,
        fromServerId: getHostId(),
        toServerId: command.client.serverId,
      })
      const peer = peers.getPeer(command.client.serverId)

      if (!peer) {
        logger.error('Peer Not Found', {
          tag: innerWrappedCommand.commandId,
          tx: effectiveCommand.tx,
          peerServerId: command.client.serverId,
        })
        throw new MykoCommandError(
          command.tx,
          'Peer Not Found for ' + command.command.commandId,
        )
      }
      ccm.forward(
        innerWrappedCommand.commandId,
        effectiveCommand.tx,
        getHostId(),
        command.client.serverId,
      )
      return peer.sendCommand(command)
    }

    logger.verbose('Dispatch to client', {
      tag: innerWrappedCommand.commandId,
      tx: effectiveCommand.tx,
      clientId: command.client.id,
    })
    ccm.begin(
      innerWrappedCommand.commandId,
      effectiveCommand.tx,
      command.client.id,
    )
    // Per-client in-flight gating
    const _maxInflight = getMaxInflightPerClient()
    const _clientId = command.client.id
    if (_maxInflight && typeof _clientId === 'string') {
      const _curr = inflightByClient.get(_clientId) ?? 0
      if (_curr >= _maxInflight) {
        ccm.err(effectiveCommand.tx, 'Backpressure')
        throw new MykoCommandError(
          command.tx,
          'Client Backpressure: too many in-flight',
        )
      }
      inflightByClient.set(_clientId, _curr + 1)
    }
    const _decInflight = () => {
      const _c = inflightByClient.get(_clientId) ?? 0
      inflightByClient.set(_clientId, Math.max(0, _c - 1))
    }

    getTx().next({
      clientId: command.client.id,
      data: wrapCommandOnlyWS(command.command),
    })
    logger.verbose('client-command:dispatched-to-client', {
      tag: innerWrappedCommand.commandId,
      tx: effectiveCommand.tx,
      clientId: command.client.id,
    })

    const disconnect$ = liveRepo(Client)
      .watchId(command.client.id)
      .pipe(
        map((x) => !!x),
        filter((x) => !x),
        take(1), // Complete after first disconnect to prevent subscription leak
        tap(() => {
          ccm.disconnected(effectiveCommand.tx)
          logger.verbose('client-command:client-disconnected', {
            tag: innerWrappedCommand.commandId,
            tx: effectiveCommand.tx,
            clientId: command.client.id,
          })
        }),
        shareReplay(1), // Share subscription across takeUntil usages
      )

    const disconnectLogged$ = disconnect$.pipe(
      tap(() =>
        logger.verbose('Client disconnected before response', {
          tag: innerWrappedCommand.commandId,
          tx: effectiveCommand.tx,
          clientId: command.client.id,
        }),
      ),
    )

    let warnTimer: any
    const waitWarnMs = 5000
    warnTimer = setTimeout(() => {
      logger.verbose(`Waiting >${waitWarnMs}ms for client response`, {
        tag: innerWrappedCommand.commandId,
        tx: effectiveCommand.tx,
        clientId: command.client.id,
      })
    }, waitWarnMs)

    let response$ = getRx().pipe(
      filter((x) => x.clientId === command.client.id),
      filter(
        (x) =>
          (x.data.event === 'ws:m:command-response' ||
            x.data.event === 'ws:m:command-error') &&
          x.data.data.tx === effectiveCommand.tx,
      ),
      tap((x) =>
        logger.verbose('client-command:response-event', {
          tag: innerWrappedCommand.commandId,
          tx: effectiveCommand.tx,
          clientId: command.client.id,
          event: x.data.event,
        }),
      ),
      takeUntil(disconnect$),
    )
    if (CCMD_TIMEOUT_MS) {
      response$ = response$.pipe(timeout(CCMD_TIMEOUT_MS))
    }
    let timedOut = false
    const res = await firstValueFrom(response$)
      .then((val) => {
        if (warnTimer) clearTimeout(warnTimer)
        logger.verbose('Observed client response event', {
          tag: innerWrappedCommand.commandId,
          tx: effectiveCommand.tx,
          clientId: command.client.id,
          event: val?.data?.event,
        })
        return val
      })
      .catch((e) => {
        if (warnTimer) clearTimeout(warnTimer)
        if (e instanceof TimeoutError) {
          timedOut = true
          ccm.timeout(effectiveCommand.tx)
        } else {
          logger.error('Error awaiting client response', {
            tag: innerWrappedCommand.commandId,
            tx: effectiveCommand.tx,
            clientId: command.client.id,
            error: (e as Error)?.message,
          })
        }
        return undefined as any
      })

    if (!res) {
      logger.error('No response from client', {
        tag: innerWrappedCommand.commandId,
        tx: effectiveCommand.tx,
        clientId: command.client.id,
      })
      _decInflight()
      throw new MykoCommandError(
        command.tx,
        timedOut ? 'Client Timed Out' : 'Client Disconnected',
      )
    }

    if (res.data.event === 'ws:m:command-error') {
      logger.error('Client responded with command error', {
        tag: innerWrappedCommand.commandId,
        tx: effectiveCommand.tx,
        clientId: command.client.id,
        message: res.data.data.message,
      })
      ccm.err(effectiveCommand.tx, res.data.data.message)
      _decInflight()
      throw new MykoCommandError(command.tx, res.data.data.message)
    }

    if (res.data.event === 'ws:m:command-response') {
      const durationMs = Date.now() - startAt
      logger.verbose('Completed client command', {
        tag: innerWrappedCommand.commandId,
        tx: effectiveCommand.tx,
        clientId: command.client.id,
        durationMs,
      })
      ccm.ok(effectiveCommand.tx)
      _decInflight()
      return res.data.data.response
    }

    _decInflight()
    throw new MykoCommandError(command.tx, 'Unknown Error')
  }
}

@MykoQueryHandler(GetClientsByIds)
export class GetClientsByIdsHandler implements MQueryHandler<GetClientsByIds> {
  execute(query: GetClientsByIds): Observable<any> {
    return liveRepo(Client).watchIds(query.ids)
  }
}

@MykoQueryHandler(GetClientsByQuery)
export class GetClientsByQueryHandler
  implements MQueryHandler<GetClientsByQuery>
{
  execute(query: GetClientsByQuery): Observable<any> {
    return liveRepo(Client).watch(query.partial)
  }
}

@MykoCommandHandler(DeleteClientsByServerId)
export class DeleteClientsByServerIdHandler
  implements MCommandHandler<DeleteClientsByServerId>
{
  async execute(command: DeleteClientsByServerId): Promise<void> {
    const clients = await liveRepo(Client).get({ serverId: command.serverId })
    eventBus.publishAll(clients.map((c) => makeDel(c, command.tx)))
  }
}

@MykoReportHandler(ClientStatus)
export class ClientStatusHandler implements MReportHandler<ClientStatus> {
  execute(report: ClientStatus): MLiveReportResult<ClientStatus> {
    return queryBus.watch(new GetClientsByQuery({}).withContext(report)).pipe(
      map((clients) => {
        return {
          online: clients.some((c) => c.id === report.clientId),
        }
      }),
    )
  }
}

@MykoCommandHandler(SetClientWindbackTime)
export class SetClientWindbackTimeHandler
  implements MCommandHandler<SetClientWindbackTime>
{
  async execute(command: SetClientWindbackTime): Promise<true> {
    try {
      getHistoryProvider()
    } catch (e) {
      throw new MykoCommandError(
        command.tx,
        'History not provided. No Undo Functionality',
      )
    }

    console.log('Setting windback time', command.windback)

    const existing = await liveRepo(Client).getId(command.commandClientId)

    ok(existing, 'Client not found')

    const client = new Client({
      ...existing,
      windback: command.windback,
      hash: undefined,
    })

    eventBus.publish(makeSet(client, command.tx))
    return true
  }
}

@MykoCommandHandler(ClearClientWindbackTime)
export class ClearClientWindbackTimeHandler
  implements MCommandHandler<ClearClientWindbackTime>
{
  async execute(command: ClearClientWindbackTime): Promise<void> {
    const existing = await liveRepo(Client).getId(command.commandClientId)

    ok(existing, 'Client not found')

    const client = new Client({
      ...existing,
      windback: undefined,
      hash: undefined,
    })

    eventBus.publish(makeSet(client, command.tx))
  }
}

@MykoReportHandler(WindbackStatus)
export class WindbackStatusHandler implements MReportHandler<WindbackStatus> {
  execute(report: WindbackStatus): MLiveReportResult<WindbackStatus> {
    return liveRepo(Client)
      .watchId(report.commandClientId)
      .pipe(
        map((client) => {
          return client?.windback
        }),
      )
  }
}

// Clean up inflight tracking when clients disconnect to prevent memory leak
@MykoSaga()
export class ClientInflightCleanupSaga implements MSagaHandler {
  execute(stream: MEventStream<MItem>): Observable<MCommand & ContextPhantom> {
    return stream.pipe(
      ofItems(Client),
      ofType(MEventType.DEL),
      tap((e) => inflightByClient.delete(e.item.id)),
      filter((_) => false),
      switchMap(() => EMPTY),
    )
  }
}
