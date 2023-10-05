import {
  ID,
  Client,
  DeleteClientsByServerId,
  Server,
  wrapCommand,
  SetClientId,
} from '@myko/core'
import { Inject, Injectable } from '@nestjs/common'
import { MykoCommandBus, MykoEventBus, MykoQueryBus } from '../busses'
import type { WebSocket } from 'ws'
import { LoggerService } from '@rship/logging'
import { SERVER_TOKEN } from '../../types'
import { v4 as uuid } from 'uuid'
import { wrapCommandOnlyWS, wrapCommandWS } from '@myko/ws'
@Injectable()
export class SocketRegistry extends Map<ID, WebSocket> {
  constructor(
    private events: MykoEventBus,
    private query: MykoQueryBus,
    private logger: LoggerService,
    private command: MykoCommandBus,
    @Inject(SERVER_TOKEN) private me: Server,
  ) {
    super()
  }

  register(socket: WebSocket) {
    const id = uuid()

    const serverId = this.me.id

    socket.on('close', () => {
      this.events.publishDel(
        new Client({ id, serverId }),
        'client-disconnected',
      )
      this.delete(id)
    })
    const c = new Client({ id, serverId })
    this.logger.getLogger('SocketRegistry').dev.info(`Client Connected`)
    this.events.publishSet(c, 'client-connected')

    const ws = wrapCommandWS(new SetClientId(id))
    socket.send(JSON.stringify(ws))
    this.set(id, socket)
  }
}
