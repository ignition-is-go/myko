import {
  Client,
  ID,
  MCommand,
  MWrappedCommand,
  MWrappedQuery,
  MWrappedReport,
  Server,
  commandBus,
  eventBus,
  queryBus,
  repo,
  reportBus,
  setDefaultRepoOptions,
  setServer,
  unwrapCommand,
  unwrapQuery,
  unwrapReport,
  wrapItem,
} from '@myko/core'
import {
  MCOMMAND_EVENT,
  MEVENT_EVENT,
  MPING_EVENT,
  MQUERY_CANCEL,
  MQUERY_EVENT,
  MQUERY_RESPONSE_EVENT,
  MREPORT_CANCEL,
  MREPORT_EVENT,
  MykoCommandError,
  WSMQueryResponse,
  wrapCommandResponseWS,
  wrapReportResponseWS,
  type WSMMessage,
} from '@myko/ws'
import { randomUUID } from 'crypto'
import { DateTime } from 'luxon'
import { Observable, Subject, catchError, filter, map, takeUntil } from 'rxjs'
import { getAuth, setAdapterBusses, setAuth } from '../registry'
import type { MykoGatewayBootstrapOptions } from './types'

export const bootstrap = (args: MykoGatewayBootstrapOptions) => {
  const {
    defaultPersister,
    port,
    wsAdapter,
    authService,
    version,
    address,
    groupId,
  } = args

  console.clear()
  const startString = `Listening: ${address}:${port} @ ${version}`
  const border = ''.padEnd(startString.length, '=')
  console.log(border)
  console.log(startString)
  console.log(border)

  setAuth(authService)

  setDefaultRepoOptions({
    persisterFactory: defaultPersister,
  })

  const tx = new Subject<{ clientId: ID; data: WSMMessage }>()
  const rx = new Subject<{ clientId: ID; data: WSMMessage }>()

  rx.subscribe(async (x) => {
    switch (x.data.event) {
      case MCOMMAND_EVENT: {
        await handleCommand(x.clientId, x.data.data, tx)
        break
      }

      case MQUERY_EVENT: {
        handleQuery(x.clientId, x.data.data, tx)
        break
      }

      case MQUERY_CANCEL: {
        unsub.next(x.data.tx)
        break
      }

      case MREPORT_EVENT: {
        handleReport(x.clientId, x.data.data, tx)
        break
      }

      case MREPORT_CANCEL: {
        unsub.next(x.data.tx)
        break
      }

      case MEVENT_EVENT: {
        eventBus.publish(x.data.data)
        break
      }

      case MPING_EVENT: {
        tx.next({
          clientId: x.clientId,

          data: {
            event: MPING_EVENT,
            data: x.data.data,
          },
        })
        break
      }

      default: {
        console.log('Unknown message', x)
      }
    }
  })

  setAdapterBusses({ tx, rx })

  const serverId = randomUUID()

  const server = new Server({
    address: args.address,
    groupId: args.groupId,
    id: serverId,
    port: port,
    startedAt: DateTime.utc().toISO(),
    version: args.version,
  })

  setServer(server)

  wsAdapter({
    port,
    rx,
    tx,
    serverId,
  })

  eventBus.subject$.subscribe((x) => {
    console.log(`${x.changeType}: ${x.itemType}`)
  })
}

const handleCommand = async (
  clientId: ID,
  command: MWrappedCommand,
  tx: Subject<{ clientId: ID; data: WSMMessage }>,
) => {
  const txid = command.command.tx

  const auth = getAuth()

  if (auth) {
    const res = await auth.canActivate(command.command.userToken).catch((e) => {
      tx.next({
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
    tx.next({
      clientId: clientId,
      data: new MykoCommandError(txid, e.message),
    })
  })

  tx.next({
    clientId: clientId,
    data: wrapCommandResponseWS(txid, res),
  })
}

const clientDisconnect = (clientId: ID) =>
  repo(Client)
    .watchId(clientId)
    .pipe(
      map((x) => !!x),
      filter((x) => !x),
    )

const unsub = new Subject<ID>()

const handleReport = (
  clientId: ID,
  wrappedReport: MWrappedReport,
  respond: Subject<{ clientId: ID; data: WSMMessage }>,
) => {
  const report = unwrapReport(wrappedReport)

  const response = reportBus.watch(report).pipe(
    map((r) => wrapReportResponseWS(report.tx, r)),
    catchError((e) => {
      console.log(e)
      throw e
    }),
    takeUntil(clientDisconnect(clientId)),
    takeUntil(unsub.pipe(filter((u) => u === report.tx))),
  )

  response.subscribe((x) => {
    respond.next({
      clientId: clientId,
      data: x,
    })
  })
}

const handleQuery = (
  clientId: ID,
  query: MWrappedQuery,
  respond: Subject<{ clientId: ID; data: WSMMessage }>,
) => {
  const q = unwrapQuery(query)

  const tx = q.tx

  const asSent = new Map<ID, string>()

  let sequence = -1

  const response = queryBus.watch(q).pipe(
    catchError((e) => {
      console.log(query)
      console.log(e)
      throw e
    }),
    map((x) => x.filter((x) => !!x)),
    map((curr) => {
      const currMap = new Map(curr.map((x) => [x.id, x]))

      const upserts = curr.filter(
        (x) =>
          x.hash == null ||
          x.hash === undefined ||
          !asSent.has(x.id) ||
          asSent.get(x.id) !== x.hash,
      )
      const deletes = Array.from(asSent.keys()).filter((x) => !currMap.has(x))

      upserts.forEach((x) => asSent.set(x.id, x.hash))

      deletes.forEach((x) => asSent.delete(x))

      sequence = sequence + 1

      return {
        data: {
          deletes: [...deletes],
          sequence: sequence,
          upserts: upserts.map((x) => wrapItem(x)),
        },
        event: MQUERY_RESPONSE_EVENT,
        tx,
      } satisfies WSMQueryResponse
    }),
    catchError((e) => {
      console.log(e)
      throw e
    }),
    filter((x) => x.data.deletes.length > 0 || x.data.upserts.length > 0),
    takeUntil(clientDisconnect(clientId)),
    takeUntil(unsub.pipe(filter((u) => u === q.tx))),
  ) as Observable<WSMQueryResponse>

  response.subscribe((x) => {
    respond.next({
      clientId: clientId,
      data: x,
    })
  })
}
