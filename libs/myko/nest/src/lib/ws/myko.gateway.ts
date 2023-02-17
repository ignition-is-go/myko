import {
  MessageBody,
  SubscribeMessage,
  WebSocketGateway,
} from '@nestjs/websockets'
import {
  MCOMMAND_EVENT,
  MEVENT_EVENT,
  MQUERY_EVENT,
  MYKO_WS_PORT,
  wrapCommandResponseWS,
  wrapQueryResponseWS,
  WSMCommand,
  WSMCommandResponse,
  WSMEvent,
  WSMQuery,
  WSMQueryResponse,
} from '@myko/ws'
import { MykoCommandBus, MykoEventBus, MykoQueryBus } from '../busses'
import { MQueryable, unwrapCommand, unwrapQuery, wrapItem } from '@myko/core'

import { filter, map, Observable, startWith, Subject, switchMap } from 'rxjs'

@WebSocketGateway(MYKO_WS_PORT)
export class MykoGateway {
  constructor(
    private event: MykoEventBus,
    private command: MykoCommandBus,
    private query: MykoQueryBus,
  ) {}

  @SubscribeMessage(MEVENT_EVENT)
  async onEvent(
    @MessageBody()
    event: WSMEvent['data'],
  ) {
    this.event.publish(event)
  }

  @SubscribeMessage(MCOMMAND_EVENT)
  async onCommand(
    @MessageBody() wrappedCommand: WSMCommand['data'],
  ): Promise<WSMCommandResponse> {
    const cmd = unwrapCommand(wrappedCommand)
    await this.command.execute(cmd)
    return wrapCommandResponseWS(cmd.tx)
  }

  @SubscribeMessage(MQUERY_EVENT)
  onQuery(
    @MessageBody() wrappedQuery: WSMQuery['data'],
  ): Observable<WSMQueryResponse> {
    const q = unwrapQuery(wrappedQuery)

    return this.event.subject$.pipe(
      filter((e) => e.itemType === wrappedQuery.queryItemType),
      switchMap(
        (_) =>
          new Promise<MQueryable>((res) => {
            setTimeout(() => {
              res(this.query.execute(q))
            }, 10)
          }),
      ),
      startWith(this.query.execute(q)),
      map((x) => x.map((r) => wrapItem(r))),
      map((r) => wrapQueryResponseWS(r, q.tx)),
    )
  }
}
