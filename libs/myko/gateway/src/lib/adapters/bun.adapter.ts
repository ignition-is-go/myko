import {
  Client,
  eventBus,
  GetClientsByIds,
  isAllInit,
  makeDel,
  queryBus,
  type ID,
} from '@myko/core'
import type { ServerWebSocket } from 'bun'
import { randomUUID } from 'crypto'
import { parse, serialize } from '../compression/client.protocols'
import type { MykoWsAdapter, MykoWsAdapterOptions } from './types'

type BunWSClientData = {
  clientId: ID
}

export const bunAdapter: MykoWsAdapter = ({
  port,
  rx,
  tx,
  clients,
  serverId,
}: MykoWsAdapterOptions) => {
  let clientSet = new Set<ID>()

  const s = Bun.serve({
    port,

    fetch(req, server) {
      const isInit = isAllInit()

      if (!isInit.done) {
        return new Response('Not Ready', { status: 500 })
      }

      const url = new URL(req.url)

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

        clientSet.add(ws.data.clientId)
        clients.next([...clientSet])
        eventBus.publishSet(
          new Client({ id: ws.data.clientId, serverId }),
          randomUUID(),
        )
      },
      drain(ws) {},
      close(ws, code, message) {
        ws.unsubscribe(ws.data.clientId)

        clientSet.delete(ws.data.clientId)
        clients.next([...clientSet])

        const existing = queryBus
          .execute(new GetClientsByIds([ws.data.clientId]))
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
}
