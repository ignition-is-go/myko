import {
  MykoLogger,
  Server,
  commandHandlers,
  commands,
  getHostId,
  onAllInit,
  queries,
  queryHandlers,
  reportHandlers,
  reports,
  setDefaultRepoOptions,
  setServer,
  watchInit,
  type ID,
} from '@myko/core'
import { MykoDocsService } from '@myko/core/src/lib/docs/myko.docs.service'
import type { WSMMessage } from '@myko/ws'
import { DateTime } from 'luxon'
import { Subject } from 'rxjs'
import { setAdapterBusses, setAuth } from '../registry'
import { handleMessage } from './message.handler'
import type { MykoGatewayBootstrapOptions } from './types'

export const bootstrap = (args: MykoGatewayBootstrapOptions) => {
  const { version, groupId } = args

  const serverId = getHostId()

  if (args.ws) {
    const { host, port, wsAdapter } = args.ws
    const startString = `Listening: ${host}:${port} @ ${version}`
    const serverInfo = `Server ID: ${serverId} | Group ID: ${groupId}`

    const maxLen = Math.max(startString.length, serverInfo.length)
    const border = ''.padEnd(maxLen, '=')

    console.log('\n' + border)
    console.log(startString)
    console.log(serverInfo)
    console.log(border + '\n')

    const doc = new MykoDocsService()

    if (args.docPath) {
      doc.writeDocs(args.docPath)
    }

    const server = new Server({
      address: host,
      groupId,
      id: serverId,
      port: port,
      startedAt: DateTime.utc().toISO(),
      version: args.version,
    })

    setServer(server)
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

    if (args.ws.authService) {
      setAuth(args.ws.authService)
    }
  }

  setDefaultRepoOptions({
    persisterFactory: args.defaultPersister,
    persisterOverrides: args.persisterOverrides,
    repoFactory: args.defaultRepo,
    repoOverrides: args.repoOverrides,
  })

  onAllInit(() => {
    const unhandledQueries = queries.difference(queryHandlers)
    const unhandledReports = reports.difference(reportHandlers)
    const unhandledCommands = commands.difference(commandHandlers)

    if (unhandledQueries.size > 0) {
      console.error(
        [
          'Unhandled Queries',
          ...[...unhandledQueries.values()].map((x) => ` - ${x}`),
          '',
        ].join('\n'),
      )
    }

    if (unhandledReports.size > 0) {
      console.error(
        [
          'Unhandled Reports',
          ...[...unhandledReports.values()].map((x) => ` - ${x}`),
          '',
        ].join('\n'),
      )
    }

    if (unhandledCommands.size > 0) {
      console.error(
        [
          'Unhandled Commands',
          ...[...unhandledCommands.values()].map((x) => ` - ${x}`),
          '',
        ].join('\n'),
      )
    }
  })

  watchInit((ent, all, init, uninit) => {
    const logger = new MykoLogger(ent)

    const allStr = `${all.length}`
    const initStr = `${init.length}`.padStart(allStr.length, ' ')

    logger.info(`Module Init ${initStr}/${all.length} (${uninit.length})`)
  })
}
