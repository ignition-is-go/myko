import { expect, test } from 'bun:test'

import {
  MykoClient,
  type Query,
  type Report,
  type View,
} from '../src/client.js'

test('same-named subscriptions keep service identity over the socket', async () => {
  const messages: unknown[] = []
  let frameReceived = () => {}
  const received = new Promise<void>((resolve) => {
    frameReceived = () => {
      if (messages.length === 7) resolve()
    }
  })
  const server = Bun.serve({
    hostname: '127.0.0.1',
    port: 0,
    fetch(request, server) {
      if (server.upgrade(request)) return
      return new Response('WebSocket required', { status: 400 })
    },
    websocket: {
      message(_socket, message) {
        messages.push(JSON.parse(message.toString()))
        frameReceived()
      },
    },
  })
  const client = new MykoClient()
  client.setConnectionLogLevel('silent')
  const subscriptions: Array<{ unsubscribe(): void }> = []
  const disposers: Array<() => void> = []
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    for (const serviceId of ['left', 'right']) {
      const query: Query<string> = {
        serviceId,
        queryId: 'Records',
        queryItemType: 'Record',
        query: {},
      }
      const view: View<string> = {
        serviceId,
        viewId: 'Records',
        viewItemType: 'Record',
        view: {},
      }
      const report: Report<string> = {
        serviceId,
        reportId: 'RecordLabel',
        report: {},
      }
      const queries = client.watchQuery(query)
      const views = client.watchView(view)
      const reports = client.watchReport(report)
      expect(client.watchQuery(query)).toBe(queries)
      expect(client.watchView(view)).toBe(views)
      expect(client.watchReport(report)).toBe(reports)
      subscriptions.push(
        queries.subscribe(),
        views.subscribe(),
        reports.subscribe(),
      )
    }
    disposers.push(
      client.subscribeReport(
        { serviceId: 'direct', reportId: 'RecordLabel', report: {} },
        () => {},
      ),
    )
    client.setAddress(`ws://127.0.0.1:${server.port}`)
    await Promise.race([
      received,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () => reject(new Error('missing service-qualified frames')),
          2000,
        )
      }),
    ])
    for (const serviceId of ['left', 'right']) {
      for (const identity of [
        { queryId: 'Records' },
        { viewId: 'Records' },
        { reportId: 'RecordLabel' },
      ]) {
        expect(messages).toContainEqual(
          expect.objectContaining({
            data: expect.objectContaining({ serviceId, ...identity }),
          }),
        )
      }
    }
    expect(messages).toContainEqual(
      expect.objectContaining({
        data: expect.objectContaining({
          serviceId: 'direct',
          reportId: 'RecordLabel',
        }),
      }),
    )
  } finally {
    clearTimeout(timer)
    for (const dispose of disposers) dispose()
    for (const subscription of subscriptions) subscription.unsubscribe()
    client.disconnect()
    server.stop(true)
  }
})
