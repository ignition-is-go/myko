import {
  ID,
  MykoLogger,
  Server,
  eventBus,
  setDefaultRepoOptions,
  setServer,
  watchInit,
} from '@myko/core'
import type { WSMMessage } from '@myko/ws'
import { randomUUID } from 'crypto'
import { DateTime } from 'luxon'
import { groupBy } from 'ramda'
import { Subject, bufferTime, filter, map } from 'rxjs'
import { setAdapterBusses, setAuth } from '../registry'
import { handleMessage } from './message.handler'
import type { MykoGatewayBootstrapOptions } from './types'

export const bootstrap = (args: MykoGatewayBootstrapOptions) => {
  const {
    defaultPersister,
    port,
    wsAdapter,
    authService,
    version,
    address,
    groupId,
  } = args

  const startString = `Listening: ${address}:${port} @ ${version}`
  const border = ''.padEnd(startString.length, '=')

  console.log(border)
  console.log(startString)
  console.log(border)

  const serverId = randomUUID()

  const server = new Server({
    address,
    groupId,
    id: serverId,
    port: port,
    startedAt: DateTime.utc().toISO(),
    version: args.version,
  })

  setServer(server)

  setAuth(authService)

  setDefaultRepoOptions({
    persisterFactory: defaultPersister,
  })

  const tx = new Subject<{ clientId: ID; data: WSMMessage }>()
  const rx = new Subject<{ clientId: ID; data: WSMMessage }>()

  rx.subscribe(handleMessage(tx))

  setAdapterBusses({ tx, rx })

  wsAdapter({
    port,
    rx,
    tx,
    serverId,
  })

  watchInit((ent, all, init, uninit) => {
    const logger = new MykoLogger(ent)

    const allStr = `${all.length}`
    const initStr = `${init.length}`.padStart(allStr.length, ' ')

    logger.info('Module Init', `${initStr}/${all.length} (${uninit.length})`)
  })

  eventBus.subject$
    .pipe(
      bufferTime(500),
      filter((x) => x.length > 0),
      map((x) => groupBy((y) => `${y.itemType}:${y.changeType}`, x)),
    )
    .subscribe((x) => {
      Object.entries(x).forEach(([k, v]) => {
        const [itemName, changeType] = k.split(':')
        const logger = new MykoLogger(itemName)

        logger.info(changeType, v.length)
      })
    })
}
