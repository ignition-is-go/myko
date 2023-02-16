import {
  MessageBody,
  SubscribeMessage,
  WebSocketGateway,
} from '@nestjs/websockets'
import {
  MCOMMAND_EVENT,
  MEVENT_EVENT,
  MQUERY_EVENT,
  WSMCommand,
  WSMEvent,
  WSMQuery,
} from '@myko/ws'
import { MykoCommandBus, MykoEventBus, MykoQueryBus } from '../busses'
import { unwrapCommand, unwrapQuery } from '@myko/core'

@WebSocketGateway(5155)
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
  onCommand(@MessageBody() wrappedCommand: WSMCommand['data']) {
    const cmd = unwrapCommand(wrappedCommand)
    this.command.execute(cmd)
  }

  @SubscribeMessage(MQUERY_EVENT)
  onQuery(@MessageBody() wrappedQuery: WSMQuery['data']) {
    const q = unwrapQuery(wrappedQuery)
    return this.query.execute(q)
  }
}
