import {
  ID,
  Client,
  GetClientsByIds,
  GetConnectedServer,
  GetServersByQuery,
  makeSet,
  GetClientsByQuery,
} from '@myko/core'
import { Injectable, OnModuleInit } from '@nestjs/common'
import { MykoEventBus, MykoQueryBus } from '../busses'
import type { WebSocket } from 'ws'
import { LoggerService } from '@rship/logging'

@Injectable()
export class SocketRegistry extends Map<ID, WebSocket> implements OnModuleInit {
  constructor(
    private events: MykoEventBus,
    private query: MykoQueryBus,
    private logger: LoggerService,
  ) {
    super()
  }
  onModuleInit() {
    setInterval(async () => {
      const server = await this.query
        .execute(new GetConnectedServer())
        .then((x) => x.shift())
      const previousIncarnations = (
        await this.query.execute(
          new GetServersByQuery({ address: server.address, port: server.port }),
        )
      ).filter((x) => x.id !== server.id)

      if (!server) {
        return
      }

      const connectedClients = await this.query
        .execute(new GetClientsByIds(Array.from(this.keys())))
        .then((x) => x.filter((c) => c.connected === false))

      const allClients = await this.query.execute(new GetClientsByQuery({}))

      const oldClients = allClients
        .filter((client) =>
          previousIncarnations.find((s) => client.serverId === s.id),
        )
        .filter((x) => x.connected)

      this.events.publishAll(
        oldClients.map((c) =>
          makeSet(
            new Client({ ...c, connected: false }),
            'socket-registry-update',
          ),
        ),
      )

      this.events.publishAll(
        connectedClients.map((c) =>
          makeSet(
            new Client({ ...c, connected: true }),
            'socket-registry-update',
          ),
        ),
      )
    }, 1000)
  }

  register(id: ID, socket: WebSocket, serverId: ID) {
    if (this.has(id) && socket === this.get(id)) {
      return
    }

    if (this.has(id)) {
      throw new Error('duplicate-connection')
    }
    socket.on('close', () => {
      this.events.publishSet(
        new Client({ connected: false, id, serverId }),
        'client-disconnected',
      )
      this.delete(id)
    })
    const c = new Client({ id, connected: true, serverId })
    this.logger.getLogger('SocketRegistry').dev.info(`Client Connected`)
    this.events.publishSet(c, 'client-connected')
    this.set(id, socket)
  }
}
