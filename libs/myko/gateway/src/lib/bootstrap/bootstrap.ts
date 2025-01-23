import {
  MykoLogger,
  Server,
  commandHandlers,
  commands,
  eventBus,
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
import { dockerAddress } from '../identity'
import { setAdapterBusses, setAuth } from '../registry'
import { handleMessage } from './message.handler'
import type { MykoGatewayBootstrapOptions } from './types'

export const bootstrap = async (args: MykoGatewayBootstrapOptions) => {
  const { version } = args

  const serverId = getHostId()

  if (args.ws) {
    const { host, port, wsAdapter } = args.ws

    const doc = new MykoDocsService()

    if (args.docPath) {
      doc.writeDocs(args.docPath)
    }

    const tx = new Subject<{ clientId: ID; data: WSMMessage }>()
    const rx = new Subject<{ clientId: ID; data: WSMMessage }>()

    rx.subscribe({
      next: handleMessage(tx),
      error: (e) => {
        console.error('Error in rx', e)
      },
    })

    setAdapterBusses({ tx, rx })

    wsAdapter({
      port,
      rx,
      tx,
      serverId,
    })

    const ENV_MYKO_PRIVATE_HOST = process.env['MYKO_PRIVATE_HOST']

    const MYKO_PRIVATE_HOST = ENV_MYKO_PRIVATE_HOST ?? (await dockerAddress())

    if (!MYKO_PRIVATE_HOST) {
      throw new Error('MYKO_PRIVATE_HOST must be set')
    }

    const server = new Server({
      address: host,
      id: serverId,
      port: port,
      startedAt: DateTime.utc().toISO(),
      version: args.version,
      privateAddress: MYKO_PRIVATE_HOST,
    })
    const publicHost = `Listening: ${host}:${port} @ ${version}`
    const privateHost = `Private: ${MYKO_PRIVATE_HOST}`
    const serverInfo = `Server ID: ${serverId}`
    const maxLen = Math.max(publicHost.length, serverInfo.length)
    const border = ''.padEnd(maxLen, '=')

    console.log('\n' + border)
    console.log(publicHost)
    console.log(privateHost)
    console.log(serverInfo)
    console.log(border + '\n')

    setServer(server)

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

    const leftStr = uninit.slice(0, 3).join(', ')

    logger.info(`Module Init ${initStr}/${all.length} [ ${leftStr} ]`)
  })

  onAllInit(() => {
    eventBus.subject$.subscribe((x) => {
      if (x.itemType === 'Log') {
        return
      }

      new MykoLogger(x.itemType).info(`${x.changeType} - ${x.item.id}`)
    })
  })
}
