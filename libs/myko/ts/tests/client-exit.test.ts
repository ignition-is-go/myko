import { describe, expect, test } from 'bun:test'

// A short-lived process (a CLI, a headless agent harness) that connected once
// must be able to exit after `disconnect()`. The module-global ws_timing
// interval used to keep the event loop alive forever.
describe('MykoClient process exit', () => {
  test('process exits after disconnect()', async () => {
    const server = Bun.serve({
      port: 0,
      fetch(req, srv) {
        return srv.upgrade(req)
          ? undefined
          : new Response('upgrade required', { status: 426 })
      },
      websocket: {
        open(ws) {
          ws.send(JSON.stringify({ event: 'ws:m:report', data: {} }))
        },
        message() {},
      },
    })
    try {
      const child = Bun.spawn(
        [
          'bun',
          `${import.meta.dir}/fixtures/ws-timing-exit.ts`,
          String(server.port),
        ],
        { stdout: 'ignore', stderr: 'pipe' },
      )
      const outcome = await Promise.race([
        child.exited,
        new Promise<'hung'>((resolve) =>
          setTimeout(() => resolve('hung'), 5_000),
        ),
      ])
      if (outcome === 'hung') child.kill()
      if (outcome !== 0) console.error(await new Response(child.stderr).text())
      expect(outcome).toBe(0)
    } finally {
      server.stop(true)
    }
  })
})
