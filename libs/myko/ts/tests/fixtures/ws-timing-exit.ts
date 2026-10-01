// Child process for client-exit.test.ts: connect, exchange one message so the
// ws_timing logger starts, disconnect, then let the event loop drain. The
// parent asserts this process exits on its own.
import { filter, firstValueFrom } from 'rxjs'

import { ConnectionStatus, MykoClient } from '../../src/client.js'

const port = Number(process.argv[2])
const client = new MykoClient()
client.setAddress(`ws://127.0.0.1:${port}`)
await firstValueFrom(
  client.connectionStatus$.pipe(
    filter((s) => s === ConnectionStatus.Connected),
  ),
)
// Give the server's greeting time to land so the ws_timing logger has started.
await new Promise((resolve) => setTimeout(resolve, 200))
client.disconnect()
