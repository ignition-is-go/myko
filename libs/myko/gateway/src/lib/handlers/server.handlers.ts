import {
  Client,
  eventBus,
  EventContainer,
  GetConnectedServer,
  GetEventLog,
  getEvents,
  getHostId,
  GetPeerServers,
  getServer,
  GetServers,
  GetServersByClientIds,
  GetServersByQuery,
  MykoLogger,
  MykoQueryHandler,
  MykoReportHandler,
  onAllInit,
  PeerAlive,
  PeerLastSeen,
  repo,
  reportBus,
  Server,
  ServerEventLog,
  type MLiveQueryResult,
  type MQueryHandler,
  type MReportHandler,
} from '@myko/core'
import { randomUUID } from 'crypto'
import { DateTime } from 'luxon'
import { uniq } from 'ramda'
import {
  combineLatest,
  debounceTime,
  filter,
  interval,
  map,
  Observable,
  of,
  startWith,
  switchMap,
} from 'rxjs'
import { peers } from '../registry'

onAllInit(async () => {
  try {
    getServer()
  } catch (e) {
    new MykoLogger('Gateway Handler').info('Not a Server, Skipping Purge')
    return
  }

  const server = getServer()

  const prev = await repo(Server).get({
    address: server.address,
    port: server.port,
  })

  const serverStartTx = randomUUID()

  for (const p of prev) {
    eventBus.publishDel(p, serverStartTx)
  }

  eventBus.publishSet(server, serverStartTx)
})

@MykoQueryHandler(GetServers)
export class GetServersHandler implements MQueryHandler<GetServers> {
  execute(_: GetServers): MLiveQueryResult<GetServers> {
    return repo(Server).watch({})
  }
}

@MykoQueryHandler(GetConnectedServer)
export class GetConnectedServerHandler
  implements MQueryHandler<GetConnectedServer>
{
  execute(_: GetConnectedServer): MLiveQueryResult<GetConnectedServer> {
    return repo(Server).watch({ id: getHostId() })
  }
}

@MykoQueryHandler(GetPeerServers)
export class GetPeerServersHandler implements MQueryHandler<GetPeerServers> {
  execute(_: GetPeerServers): MLiveQueryResult<GetPeerServers> {
    return repo(Server).watchFilter(
      (s) => s.address != getServer().address || s.port != getServer().port,
    )
  }
}

@MykoQueryHandler(GetServersByQuery)
export class GetServersByQueryHandler
  implements MQueryHandler<GetServersByQuery>
{
  execute(query: GetServersByQuery): MLiveQueryResult<GetServersByQuery> {
    return repo(Server).watch(query.query)
  }
}

@MykoQueryHandler(GetServersByClientIds)
export class GetServersByClientIdsHandler
  implements MQueryHandler<GetServersByClientIds>
{
  execute(
    query: GetServersByClientIds,
  ): MLiveQueryResult<GetServersByClientIds> {
    return repo(Client)
      .watchIds(query.clientIds)
      .pipe(
        map((clients) => clients.map((c) => c.serverId)),
        map(uniq),
        switchMap((serverIds) => repo(Server).watchIds(serverIds)),
      )
  }
}

@MykoReportHandler(PeerAlive)
export class PeerAliveHandler implements MReportHandler<PeerAlive> {
  execute(report: PeerAlive) {
    return interval(1000).pipe(
      switchMap((_) => {
        peers.assertPeer(report.peerId)

        const peer = peers.getPeer(report.peerId)
        if (!peer) {
          new MykoLogger('PeerAliveHandler').info(
            `Peer Not Found ${report.peerId}`,
          )
          return of(false) as Observable<number | false>
        }

        return peer.ping().catch((_e) => {
          new MykoLogger('PeerAliveHandler').info(`Peer Dead, ${report.peerId}`)
          return false
        })
      }),
    ) as Observable<number | false>
  }
}

@MykoReportHandler(PeerLastSeen)
export class PeerLastSeenHandler implements MReportHandler<PeerLastSeen> {
  execute(report: PeerLastSeen) {
    return reportBus.watch(new PeerAlive(report.peerId)).pipe(
      filter((x) => x !== false),
      map((_x) => DateTime.utc().toISO()),
    )
  }
}

@MykoReportHandler(ServerEventLog)
export class ServerEventLogHandler implements MReportHandler<ServerEventLog> {
  execute(_report: ServerEventLog) {
    return eventBus.subject$.pipe(filter((x) => x.sourceId == getHostId()))
  }
}

@MykoQueryHandler(GetEventLog)
export class GetEventLogHandler implements MQueryHandler<GetEventLog> {
  execute(query: GetEventLog): Observable<EventContainer[]> {
    const time = query.time

    const all = [...getEvents.values()]

    return combineLatest(
      all.map((fn) => fn(time).pipe(startWith([] as EventContainer[]))),
    ).pipe(
      map((x) =>
        x
          .flat()
          .filter((x) => x.event.tx !== undefined)
          .sort((a, b) => a.id.localeCompare(b.id)),
      ),
      debounceTime(50),
    )
  }
}
