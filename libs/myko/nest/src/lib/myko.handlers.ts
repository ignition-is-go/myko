import {
  ClientRepo,
  EventContainer,
  GetClientsByIds,
  GetEventLog,
  GetItemsByTypeAndIds,
  MCommandHandler,
  MItem,
  MQueryHandler,
  MykoCommandHandler,
  MykoProtocol,
  MykoQueryHandler,
  getEvents,
} from '@myko/core'
import { ClientCommand, wrapCommandWS } from '@myko/ws'
import { MykoCommandError, SocketRegistry } from '../types'
import { Observable, combineLatest, debounceTime, map, startWith } from 'rxjs'
import { watchIds } from '@myko/core/src/lib/registry'
import { clientProtocols, encoders } from './registry/client.protocols'
@MykoCommandHandler(ClientCommand)
export class ClientCommandHandler implements MCommandHandler<ClientCommand> {
  constructor(private reg: SocketRegistry) {}

  async execute(command: ClientCommand): Promise<void> {
    const sockets = this.reg.get(command.clientId)

    if (!sockets || sockets.size === 0) {
      throw new MykoCommandError(command.tx, 'Exec Not Connected')
    }
    sockets.forEach((socket) => {
      socket.send(
        encoders.get(clientProtocols.get(socket) ?? MykoProtocol.JSON)(
          wrapCommandWS(command.command, 'rship-server'),
        ),
      )
    })
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

@MykoQueryHandler(GetClientsByIds)
export class GetClientsByIdsHandler implements MQueryHandler<GetClientsByIds> {
  constructor(private repo: ClientRepo) {}
  execute(query: GetClientsByIds): Observable<any> {
    return this.repo.watchIds(query.ids)
  }
}

@MykoQueryHandler(GetItemsByTypeAndIds)
export class GetItemByTypeAndIdHandler
  implements MQueryHandler<GetItemsByTypeAndIds>
{
  constructor() {}
  execute(query: GetItemsByTypeAndIds): Observable<MItem[]> {
    const watchTypeByIds = watchIds.get(query.type)
    return watchTypeByIds(query.ids)
  }
}
