import {
  Client,
  eventBus,
  GetClientsByIds,
  getHostId,
  isAllInit,
  makeDel,
  MykoLogger,
  queryBus,
  type ID,
} from '@myko/core'
import {
  MCOMMAND_ERROR_EVENT,
  MCOMMAND_EVENT,
  MCOMMAND_RESPONSE_EVENT,
} from '@myko/ws'
import type { ServerWebSocket } from 'bun'
import { randomUUID } from 'crypto'
import { filter, firstValueFrom } from 'rxjs'
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
  const logger = new MykoLogger('BunAdapter')

  const s = Bun.serve({
    port,
    fetch: async (req, server) => {
      const url = new URL(req.url)

      if (url.pathname === '/server') {
        return new Response(getHostId(), { status: 200 })
      }

      const isInit = isAllInit()

      if (!isInit.done) {
        return new Response('Not Ready', { status: 500 })
      }

      if (url.pathname === '/myko/api') {
        const clientId = randomUUID()

        if (req.method === 'POST') {
          const body = await req.text().catch((e) => {
            logger.error(e.message, e)
            return undefined
          })

          if (!body) {
            return new Response('Invalid Body', { status: 400 })
          }

          const parsed = parse(clientId, body)

          if (!parsed) {
            return new Response('Invalid Body', { status: 400 })
          }

          if (parsed.event !== MCOMMAND_EVENT) {
            return new Response('Only Commands are supported over REST', {
              status: 400,
            })
          }

          // we don't expect txIds over REST so we generate a new one
          const txId: ID = randomUUID()

          Reflect.set(parsed.data.command, 'tx', txId)

          const successPromise = firstValueFrom(
            tx.pipe(
              filter((x) => x.clientId === clientId),
              filter((x) =>
                parsed.event === MCOMMAND_EVENT
                  ? x.data.event === MCOMMAND_RESPONSE_EVENT &&
                    x.data.data.tx === parsed.data.command.tx
                  : false,
              ),
            ),
          )
          const errorPromise = firstValueFrom(
            tx.pipe(
              filter((x) => x.clientId === clientId),
              filter((x) =>
                parsed.event === MCOMMAND_EVENT
                  ? x.data.event === MCOMMAND_ERROR_EVENT &&
                    x.data.data.tx === parsed.data.command.tx
                  : false,
              ),
            ),
          )

          rx.next({ clientId, data: parsed })
          const response = await Promise.race([successPromise, errorPromise])
          return new Response(JSON.stringify(response.data), {
            status: response.data.event === MCOMMAND_ERROR_EVENT ? 400 : 200,
            statusText:
              response.data.event === MCOMMAND_ERROR_EVENT
                ? response.data.data.message
                : 'OK',
          })
        }
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

        // Send initial ping
        ws.ping()

        // Set up periodic pings to keep connection alive (every 25 seconds)
        const keepAliveInterval = setInterval(() => {
          try {
            ws.ping()
          } catch (error) {
            console.warn(`Failed to ping client ${ws.data.clientId}:`, error)
            clearInterval(keepAliveInterval)
          }
        }, 1000)
        // Store the interval so we can clean it up on close
        ;(ws as any).__keepAliveInterval = keepAliveInterval
      },
      drain(_ws) {},
      close(ws, _code, _message) {
        logger.warn(`Client ${ws.data.clientId} closed, ${_message}, ${_code}`)

        // Clean up keep-alive interval
        const keepAliveInterval = (ws as any).__keepAliveInterval
        if (keepAliveInterval) {
          clearInterval(keepAliveInterval)
        }

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
      logger.error('Error in tx', e.message)
    },
  })

  return {
    clientHealthCheck: (id: ID) => {
      return s.subscriberCount(id) > 0
    },
  }
}
