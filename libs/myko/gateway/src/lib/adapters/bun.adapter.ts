import {
  Client,
  commandBus,
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
import { randomUUID } from 'node:crypto'
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
  tls,
}: MykoWsAdapterOptions): MykoWsAdapterResult => {
  const logger = new MykoLogger('BunAdapter')

  // Build TLS config if provided
  const tlsConfig = tls
    ? {
        key: Bun.file(tls.key),
        cert: Bun.file(tls.cert),
        ...(tls.ca && { ca: Bun.file(tls.ca) }),
        ...(tls.passphrase && { passphrase: tls.passphrase }),
      }
    : undefined

  if (tls) {
    logger.info(`TLS enabled on port ${port}`)
  }

  const s = Bun.serve({
    port,
    hostname: '0.0.0.0', // Explicitly bind to all interfaces
    tls: tlsConfig,
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

      // Feedback frustration endpoint - accepts anonymous feedback
      if (
        url.pathname === '/api/feedback/frustration' &&
        req.method === 'POST'
      ) {
        try {
          const body = await req.json().catch(() => null)

          if (
            !body ||
            typeof body.rawText !== 'string' ||
            typeof body.route !== 'string'
          ) {
            return new Response(
              JSON.stringify({
                success: false,
                error:
                  'Missing required fields: rawText and route are required',
              }),
              {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
              },
            )
          }

          // Dynamically import to avoid circular dependencies
          const { CreateFeedbackEvent } = await import('@rship/entities-core')

          const txId = randomUUID()
          const correlationId = randomUUID()

          // Build the command with server-side enrichment
          const command = new CreateFeedbackEvent(body.rawText, body.route, {
            userId: body.userId,
            anonymousId: body.anonymousId || randomUUID(),
            userContext: body.userContext,
            screenshotUrl: body.screenshotUrl,
            metadata: body.metadata,
            browserInfo:
              body.browserInfo || req.headers.get('user-agent') || undefined,
            environment: body.environment,
            viewport: body.viewport,
            featureFlags: body.featureFlags,
            recentErrors: body.recentErrors,
            performanceMetrics: body.performanceMetrics,
            projectId: body.projectId,
            sessionId: body.sessionId,
            // Server-side enrichment
            serverTimestamp: new Date().toISOString(),
            serverVersion: process.env.SERVER_VERSION || 'unknown',
            correlationId,
          })

          command.tx = txId

          // Execute the command
          const feedbackId = await commandBus.execute(command)

          return new Response(
            JSON.stringify({
              success: true,
              feedbackId,
              correlationId,
              message:
                'Thank you for your feedback. We take every report seriously.',
            }),
            {
              status: 201,
              headers: { 'Content-Type': 'application/json' },
            },
          )
        } catch (error) {
          logger.error('Failed to process feedback', error)
          return new Response(
            JSON.stringify({
              success: false,
              error: 'Failed to process feedback. Please try again.',
            }),
            {
              status: 500,
              headers: { 'Content-Type': 'application/json' },
            },
          )
        }
      }

      if (url.pathname === '/myko') {
        const isInit = isAllInit()

        if (!isInit.done) {
          return new Response('Not Ready', { status: 503 })
        }

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
      // Enable per-message deflate compression for clients that use it (like Unreal Engine)
      perMessageDeflate: true,
      message(ws: ServerWebSocket<BunWSClientData>, message) {
        try {
          const parsed = parse(ws.data.clientId, message)
          rx.next({ clientId: ws.data.clientId, data: parsed })
        } catch (e) {
          console.error('[WS] Parse error:', e)
          ws.send('Error parsing message')
        }
      },
      open(ws: ServerWebSocket<BunWSClientData>) {
        ws.subscribe(ws.data.clientId)

        eventBus.publishSet(
          new Client({ id: ws.data.clientId, serverId }),
          randomUUID(),
        )

        // Send SetClientId command to trigger executor's SendAll()
        // This also helps flush IXWebSocket's buffer by sending data immediately
        const setClientIdCmd = JSON.stringify({
          event: 'ws:m:command',
          data: {
            commandId: 'SetClientId',
            command: {
              tx: randomUUID(),
              clientId: ws.data.clientId,
            },
          },
        })
        ws.send(setClientIdCmd)

        // Send additional pings to ensure IXWebSocket buffer flushes
        const pingMsg = () =>
          JSON.stringify({ event: 'ws:m:ping', data: { ts: Date.now() } })
        setTimeout(() => {
          try {
            ws.send(pingMsg())
          } catch {}
        }, 100)
        setTimeout(() => {
          try {
            ws.send(pingMsg())
          } catch {}
        }, 500)
        setTimeout(() => {
          try {
            ws.send(pingMsg())
          } catch {}
        }, 1000)

        // Set up periodic pings to keep connection alive (every 25 seconds)
        const keepAliveInterval = setInterval(() => {
          try {
            ws.ping()
          } catch (error) {
            console.warn(`Failed to ping client ${ws.data.clientId}:`, error)
            clearInterval(keepAliveInterval)
          }
        }, 25000) // 25 seconds - well before typical 30s timeout

        // Store the interval so we can clean it up on close
        ;(ws as any).__keepAliveInterval = keepAliveInterval
      },
      drain(_ws) {
        // WebSocket buffer drained - ready for more data
      },
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

  logger.info(`Server listening on 0.0.0.0:${port}`)

  /**
   * Performance: Message batching for WebSocket
   *
   * High event throughput can generate 1000s of messages/sec per client.
   * Batching reduces syscall overhead and improves network efficiency.
   *
   * Strategy:
   * - Queue messages for each client
   * - Flush every 16ms (60fps) or when queue reaches 100 messages
   * - Preserves message order within each client
   * - Independent batches per client (no head-of-line blocking)
   */

  const messageQueues = new Map<ID, any[]>()
  const flushTimeouts = new Map<ID, ReturnType<typeof setTimeout>>()

  const BATCH_INTERVAL_MS = 16 // ~60fps
  const MAX_BATCH_SIZE = 100 // Flush if queue exceeds this

  const flushQueue = (clientId: ID) => {
    const queue = messageQueues.get(clientId)
    if (!queue || queue.length === 0) return

    // Send all messages in batch as array
    const batch = queue.splice(0, queue.length)

    // For now, send individually (future: send as single array message)
    // TODO: Update client protocol to handle batched messages
    batch.forEach((msg) => {
      s.publish(clientId, serialize(clientId, msg))
    })

    // Clear flush timeout
    const timeoutId = flushTimeouts.get(clientId)
    if (timeoutId) {
      clearTimeout(timeoutId)
      flushTimeouts.delete(clientId)
    }
  }

  const queueMessage = (clientId: ID, data: any) => {
    // Get or create queue for this client
    if (!messageQueues.has(clientId)) {
      messageQueues.set(clientId, [])
    }

    const queue = messageQueues.get(clientId)!
    queue.push(data)

    // Flush immediately if batch size exceeded
    if (queue.length >= MAX_BATCH_SIZE) {
      flushQueue(clientId)
      return
    }

    // Schedule flush if not already scheduled
    if (!flushTimeouts.has(clientId)) {
      const timeoutId = setTimeout(() => {
        flushQueue(clientId)
      }, BATCH_INTERVAL_MS)
      flushTimeouts.set(clientId, timeoutId)
    }
  }

  tx.subscribe({
    next: (m) => {
      queueMessage(m.clientId, m.data)
    },
    error: (e) => {
      logger.error('Error in tx', e.message)
    },
  })

  // Graceful shutdown handler
  const shutdown = () => {
    logger.info('Shutting down server...')

    // Clear all pending flush timeouts
    for (const timeoutId of flushTimeouts.values()) {
      clearTimeout(timeoutId)
    }
    flushTimeouts.clear()
    messageQueues.clear()

    // Stop the server
    s.stop()

    logger.info('Server stopped')
    process.exit(0)
  }

  // Handle termination signals
  process.on('SIGINT', shutdown)
  process.on('SIGTERM', shutdown)

  return {
    clientHealthCheck: (id: ID) => {
      return s.subscriberCount(id) > 0
    },
  }
}
