import {
  EventContainer,
  GetEventLog,
  MCommandHandler,
  MQueryHandler,
  MykoCommandHandler,
  MykoQueryHandler,
  getEvents,
} from '@myko/core'
import { ClientCommand, wrapCommandWS } from '@myko/ws'
import { MykoCommandError, SocketRegistry } from '../types'
import { Observable, combineLatest, debounceTime, map, startWith } from 'rxjs'
@MykoCommandHandler(ClientCommand)
export class ClientCommandHandler implements MCommandHandler<ClientCommand> {
  constructor(private reg: SocketRegistry) {}

  async execute(command: ClientCommand): Promise<void> {
    const socket = this.reg.get(command.clientId)

    if (!socket) {
      throw new MykoCommandError(command.tx, 'Exec Not Connected')
    }

    return socket.send(
      JSON.stringify(wrapCommandWS(command.command, 'rship-server')),
    )
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
