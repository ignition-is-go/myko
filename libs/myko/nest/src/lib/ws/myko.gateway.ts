import {
  ConnectedSocket,
  MessageBody,
  OnGatewayConnection,
  SubscribeMessage,
  WebSocketGateway,
} from '@nestjs/websockets'
import {
  MCOMMAND_EVENT,
  MEVENT_EVENT,
  MPING_EVENT,
  MQUERY_CANCEL,
  MQUERY_EVENT,
  MREPORT_CANCEL,
  MREPORT_EVENT,
  wrapCommandResponseWS,
  wrapQueryResponseWS,
  wrapReportResponseWS,
  WSMCommand,
  WSMCommandResponse,
  WSMEvent,
  WSMQuery,
  WSMQueryCancel,
  WSMQueryResponse,
  WSMReport,
  WSMReportCancel,
  WSMReportResponse,
  WSPingEvent,
} from '@myko/ws'
import {
  MykoCommandBus,
  MykoEventBus,
  MykoQueryBus,
  MykoReportBus,
} from '../busses'
import {
  ID,
  isAllInit,
  MykoProtocol,
  ProtocolMessages,
  unwrapCommand,
  unwrapQuery,
  unwrapReport,
  wrapItem,
} from '@myko/core'

import { catchError, filter, map, Observable, Subject, takeUntil } from 'rxjs'
import { UseFilters, UseGuards } from '@nestjs/common'
import { MykoGuard } from './myko.guard'
import { WsExceptionFilter } from './myko.exception-filter'
import { clientProtocols } from '../registry/client.protocols'
import { SocketRegistry } from '../registry/socket.registry'
import { WebSocket } from 'ws'

@WebSocketGateway(Number(process.env.MYKO_PORT ?? 5155), { path: '/myko' })
@UseGuards(MykoGuard)
@UseFilters(new WsExceptionFilter())
export class MykoGateway implements OnGatewayConnection {
  private unsub = new Subject<ID>()

  constructor(
    private event: MykoEventBus,
    private command: MykoCommandBus,
    private query: MykoQueryBus,
    private report: MykoReportBus,
    private reg: SocketRegistry,
  ) {}

  handleConnection(client: WebSocket) {
    try {
      this.reg.register(client)
    } catch (e) {
      client.close(1002, e.message)
    }
  }

  @SubscribeMessage(MEVENT_EVENT)
  async onEvent(
    @MessageBody()
    event: WSMEvent,
  ) {
    this.event.publish(event.data)
  }

  @SubscribeMessage(ProtocolMessages.SwitchToMSGPACK)
  async onSwitchToMSGPACK(@ConnectedSocket() socket): Promise<void> {
    socket.send(ProtocolMessages.SwitchToMSGPACK)
    clientProtocols.set(socket, MykoProtocol.MSGPACK)
  }

  @SubscribeMessage(MCOMMAND_EVENT)
  async onCommand(
    @MessageBody() wrappedCommand: WSMCommand,
  ): Promise<WSMCommandResponse> {
    const cmd = unwrapCommand(wrappedCommand.data)
    const res = await this.command.execute(cmd)
    return wrapCommandResponseWS(cmd.tx, res)
  }

  @SubscribeMessage(MQUERY_CANCEL)
  onQueryCancel(@MessageBody() cancel: WSMQueryCancel) {
    this.unsub.next(cancel.tx)
  }

  @SubscribeMessage(MQUERY_EVENT)
  onQuery(@MessageBody() wrappedQuery: WSMQuery): Observable<WSMQueryResponse> {
    const q = unwrapQuery(wrappedQuery.data)

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

  @SubscribeMessage(MREPORT_CANCEL)
  onReportCancel(@MessageBody() cancel: WSMReportCancel) {
    this.unsub.next(cancel.tx)
  }

  @SubscribeMessage(MREPORT_EVENT)
  onReport(
    @MessageBody() wrappedReport: WSMReport,
  ): Observable<WSMReportResponse> {
    const report = unwrapReport(wrappedReport.data)

    return this.report.watch(report).pipe(
      map((r) => wrapReportResponseWS(report.tx, r)),
      catchError((e) => {
        console.log(e)
        throw e
      }),
      takeUntil(this.unsub.pipe(filter((u) => u === report.tx))),
    )
  }

  @SubscribeMessage(MPING_EVENT)
  onPing(@MessageBody() wrappedPing: WSPingEvent) {
    return {
      data: wrappedPing.data,
      event: MPING_EVENT,
    } satisfies WSPingEvent
  }
}
