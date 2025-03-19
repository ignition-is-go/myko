import {
  Client,
  eventBus,
  GetClientsByIds,
  getHostId,
  isAllInit,
  makeDel,
  queryBus,
  type ID,
} from '@myko/core'
import type { ServerWebSocket } from 'bun'
import { randomUUID } from 'crypto'
import { v4 as uuid } from 'uuid'
import { parse, serialize } from '../compression/client.protocols'
import type {
  MykoWsAdapter,
  MykoWsAdapterOptions,
  MykoWsAdapterResult,
} from './types'

type BunWSClientData = {
  clientId: ID
}

export const bunAdapter: MykoWsAdapter = ({
  port,
  rx,
  tx,
  serverId,
}: MykoWsAdapterOptions): MykoWsAdapterResult => {
  const s = Bun.serve({
    port,
    fetch(req, server) {
      const url = new URL(req.url)

      if (url.pathname === '/server') {
        return new Response(getHostId(), { status: 200 })
      }

      const isInit = isAllInit()

      if (!isInit.done) {
        return new Response('Not Ready', { status: 500 })
      }

      if (url.pathname === '/myko') {
        const clientId = randomUUID()

        if (
          server.upgrade(req, { data: { clientId } satisfies BunWSClientData })
        ) {
          return new Response('Upgrade Success', { status: 101 })
        }
      }

      return new Response('Upgrade Failed', { status: 500 })
    },
    websocket: {
      message(ws: ServerWebSocket<BunWSClientData>, message) {
        try {
          const parsed = parse(ws.data.clientId, message)
          rx.next({ clientId: ws.data.clientId, data: parsed })
        } catch (e) {
          ws.send('Error parsing message')
        }
      },
      open(ws: ServerWebSocket<BunWSClientData>) {
        ws.subscribe(ws.data.clientId)

        eventBus.publishSet(
          new Client({ id: ws.data.clientId, serverId }),
          randomUUID(),
        )

        ws.ping()
      },
      drain(_ws) {},
      close(ws, _code, _message) {
        ws.unsubscribe(ws.data.clientId)
        queryBus
          .execute(
            new GetClientsByIds([ws.data.clientId]).withContext({
              commandClientId: getHostId(),
              tx: uuid(),
            }),
          )
          .then((clients) => {
            if (clients.length === 0) {
              return
            }
            eventBus.publishAll(clients.map((c) => makeDel(c, randomUUID())))
          })
      },
    },
  })

  tx.subscribe({
    next: (m) => {
      s.publish(m.clientId, serialize(m.clientId, m.data))
    },
    error: (e) => {
      console.error('Error in tx', e.message)
    },
  })

  return {
    clientHealthCheck: (id: ID) => {
      return s.subscriberCount(id) > 0
    },
  }
}
