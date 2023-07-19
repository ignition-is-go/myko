import {
  ConnectedSocket,
  MessageBody,
  OnGatewayConnection,
  OnGatewayDisconnect,
  SubscribeMessage,
  WebSocketGateway,
} from '@nestjs/websockets'
import {
  MCOMMAND_EVENT,
  MEVENT_EVENT,
  MQUERY_CANCEL,
  MQUERY_EVENT,
  MYKO_WS_PORT,
  wrapCommandResponseWS,
  wrapQueryResponseWS,
  WSMCommand,
  WSMCommandResponse,
  WSMEvent,
  WSMQuery,
  WSMQueryCancel,
  WSMQueryResponse,
} from '@myko/ws'
import { MykoCommandBus, MykoEventBus, MykoQueryBus } from '../busses'
import {
  ID,
  MykoProtocol,
  ProtocolMessages,
  Server,
  unwrapCommand,
  unwrapQuery,
  wrapItem,
} from '@myko/core'

import { catchError, filter, map, Observable, Subject, takeUntil } from 'rxjs'
import { Inject, UseFilters, UseGuards } from '@nestjs/common'
import { MykoGuard } from './myko.guard'
import { WsExceptionFilter } from './myko.exception-filter'
import { clientProtocols } from '../registry/client.protocols'
import { SocketRegistry } from '../registry/socket.registry'
import { ConfigService } from '@nestjs/config'
import { SERVER_TOKEN } from '../../types'
import { WebSocket } from 'ws'
import { parse } from 'url'

@WebSocketGateway(Number(process.env.MYKO_PORT), { path: '/myko' })
@UseGuards(MykoGuard)
@UseFilters(new WsExceptionFilter())
export class MykoGateway implements OnGatewayConnection {
  private unsub = new Subject<ID>()

  constructor(
    private event: MykoEventBus,
    private command: MykoCommandBus,
    private query: MykoQueryBus,
    private reg: SocketRegistry,
    private config: ConfigService,
    @Inject(SERVER_TOKEN) private server: Server,
  ) {}

  handleConnection(client: WebSocket, ...args: any[]) {
    const params = parse(args[0].url, true).query
    const clientId = params.clientId as ID
    if (!clientId) {
      client.close()
      return
    }
    try {
      this.reg.register(clientId, client, this.server.id)
    } catch (e) {
      client.close(1002, e.message)
    }
  }

  @SubscribeMessage(MEVENT_EVENT)
  async onEvent(
    @MessageBody()
    event: WSMEvent['data'],
  ) {
    this.event.publish(event)
  }

  @SubscribeMessage(ProtocolMessages.SwitchToMSGPACK)
  async onSwitchToMSGPACK(@ConnectedSocket() socket): Promise<void> {
    socket.send(ProtocolMessages.SwitchToMSGPACK)
    clientProtocols.set(socket, MykoProtocol.MSGPACK)
  }

  @SubscribeMessage(MCOMMAND_EVENT)
  async onCommand(
    @MessageBody() wrappedCommand: WSMCommand['data'],
  ): Promise<WSMCommandResponse> {
    const cmd = unwrapCommand(wrappedCommand)
    const res = await this.command.execute(cmd)
    return wrapCommandResponseWS(cmd.tx, res)
  }

  @SubscribeMessage(MQUERY_CANCEL)
  onQueryCancel(@MessageBody() tx: WSMQueryCancel['data']) {
    this.unsub.next(tx)
  }

  @SubscribeMessage(MQUERY_EVENT)
  onQuery(
    @MessageBody() wrappedQuery: WSMQuery['data'],
  ): Observable<WSMQueryResponse> {
    const q = unwrapQuery(wrappedQuery)

    return this.query.watch(q).pipe(
      map((x) => x.filter((x) => !!x)),
      map((x) => x.map((r) => wrapItem(r))),
      catchError((e) => {
        console.log(wrappedQuery)
        console.log(e)
        throw e
      }),
      map((r) => wrapQueryResponseWS(r, q.tx)),
      catchError((e) => {
        console.log(e)
        throw e
      }),
      takeUntil(this.unsub.pipe(filter((u) => u === q.tx))),
    )
  }
}
