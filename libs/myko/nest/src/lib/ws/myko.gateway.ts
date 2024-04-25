import {
  ID,
  MykoProtocol,
  ProtocolMessages,
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
  wrapCommandResponseWS,
  wrapReportResponseWS,
} from '@myko/ws'
import {
  ConnectedSocket,
  MessageBody,
  OnGatewayConnection,
  SubscribeMessage,
  WebSocketGateway,
} from '@nestjs/websockets'
import {
  MykoCommandBus,
  MykoEventBus,
  MykoQueryBus,
  MykoReportBus,
} from '../busses'

import { UseFilters, UseGuards } from '@nestjs/common'
import { Observable, Subject, catchError, filter, map, takeUntil } from 'rxjs'
import { WebSocket } from 'ws'
import { clientProtocols } from '../registry/client.protocols'
import { SocketRegistry } from '../registry/socket.registry'
import { WsExceptionFilter } from './myko.exception-filter'
import { MykoGuard } from './myko.guard'

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

    const tx = q.tx

    const asSent = new Map<ID, string>()

    return this.query.watch(q).pipe(
      catchError((e) => {
        console.log(wrappedQuery)
        console.log(e)
        throw e
      }),
      map((x) => x.filter((x) => !!x)),
      map((curr) => {
        const currMap = new Map(curr.map((x) => [x.id, x]))

        const upserts = curr.filter(
          (x) => !asSent.has(x.id) || asSent.get(x.id) !== x.hash,
        )
        const deletes = Array.from(asSent.keys()).filter((x) => !currMap.has(x))

        upserts.forEach((x) => asSent.set(x.id, x.hash))

        deletes.forEach((x) => asSent.delete(x))

        return {
          data: {
            deletes: [...deletes],
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
