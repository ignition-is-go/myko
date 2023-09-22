import {
  ID,
  Client,
  GetClientsByIds,
  GetConnectedServer,
  GetServersByQuery,
  makeSet,
  GetClientsByQuery,
  makeDel,
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

      const allClients = await this.query.execute(new GetClientsByQuery({}))

      const oldClients = allClients.filter((client) =>
        previousIncarnations.find((s) => client.serverId === s.id),
      )

      this.events.publishAll(
        oldClients.map((c) => makeDel(c, 'socket-registry-update')),
      )
      ;[...this.entries()].forEach(([id, socket]) => {
        this.events.publishSet(
          new Client({ id, serverId: server.id }),
          'socket-registry-update',
        )
      })
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
      this.events.publishDel(
        new Client({ id, serverId }),
        'client-disconnected',
      )
      this.delete(id)
    })
    const c = new Client({ id, serverId })
    this.logger.getLogger('SocketRegistry').dev.info(`Client Connected`)
    this.events.publishSet(c, 'client-connected')
    this.set(id, socket)
  }
}
