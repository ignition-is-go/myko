import {
  commandHandlers,
  commands,
  eventBus,
  getHistoryProvider,
  getHostId,
  MykoLogger,
  onAllInit,
  queries,
  queryHandlers,
  reportHandlers,
  reports,
  Server,
  setDefaultRepoOptions,
  setHistoryProvider,
  setServer,
  watchInit,
  type ID,
} from '@myko/core'
import { MykoDocsService } from '@myko/core/src/lib/docs/myko.docs.service'

import { logsPreventedEntities } from '@myko/core/src/lib/logger/registry'
import type { WSMMessage } from '@myko/ws'
import { DateTime } from 'luxon'
import { groupBy } from 'ramda'
import { bufferTime, filter, Subject } from 'rxjs'
import { setAdapterBusses, setAdapterResult, setAuth } from '../registry'
import { initCallRateMonitor } from '../telemetry/call_rate.monitor'
import { initTracing } from '../telemetry/instrumentation'
import { handleMessage } from './message.handler'
import type { MykoGatewayBootstrapOptions, StartupReportEntry } from './types'

export const bootstrap = async (args: MykoGatewayBootstrapOptions) => {
  const logger = new MykoLogger('Bootstrap')
  const { version } = args

  const startupReport: StartupReportEntry[] = []
  const serverId = getHostId()

  if (args.historyProvider) {
    setHistoryProvider(args.historyProvider)
    await getHistoryProvider().init()

    startupReport.push({
      lines: [`History Provider Initialized`],
      module: 'History',
    })
  }

  startupReport.push({
    lines: [`Starting: ${version}`, `Host ID: ${serverId}`],
    module: 'Bootstrap',
  })

  if (args.tracing?.enabled) {
    const report = initTracing(args)
    startupReport.push(report)
  }

  // Initialize call rate monitoring (enabled via MYKO_CALL_RATE_LOGGING=true)
  initCallRateMonitor()

  if (args.ws) {
    const { host, port, wsAdapter } = args.ws

    const doc = new MykoDocsService()

    if (args.docPath) {
      doc.writeDocs(args.docPath)
      startupReport.push({
        lines: [`Docs written to ${args.docPath}`],
        module: 'Docs',
      })
    }

    const tx = new Subject<{ clientId: ID; data: WSMMessage }>()
    const rx = new Subject<{ clientId: ID; data: WSMMessage }>()

    rx.subscribe({
      next: handleMessage(tx),
      error: (e) => {
        logger.error('Error in rx', e)
      },
    })

    setAdapterBusses({ tx, rx })

    const res = wsAdapter({
      port,
      rx,
      tx,
      serverId,
      tls: args.ws.tls,
    })

    setAdapterResult(res)

    const server = new Server({
      address: host,
      id: serverId,
      port: port,
      startedAt: DateTime.utc().toISO(),
      version: args.version,
    })

    const protocol = args.ws.tls ? 'wss' : 'ws'
    const lines = [`Listening: ${protocol}://${host}:${port}`]

    startupReport.push({
      lines,
      module: 'WebSocket Adapter',
    })

    setServer(server)

    if (args.ws.authService) {
      setAuth(args.ws.authService)
    }

    const maxLineLength = Math.max(
      ...startupReport.flatMap((x) => x.lines.map((y) => y.length)),
    )

    for (const entry of startupReport) {
      console.log('='.repeat(maxLineLength))
      console.log(`[ ${entry.module} ] `)
      console.log(entry.lines.join('\n'))
    }
    console.log(`${'='.repeat(maxLineLength)}`)
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
    eventBus.subject$
      .pipe(
        filter((x) => logsPreventedEntities.has(x.itemType) === false),
        bufferTime(1000),
      )
      .subscribe((events) => {
        const groupedByType = groupBy(
          (x) => `${x.changeType} - ${x.itemType}`,
          events,
        )

        for (const [itemType, items] of Object.entries(groupedByType)) {
          if (!items?.length) {
            continue
          }
          const logger = new MykoLogger(itemType)
          logger.info(`${items.length}`)
        }
      })
  })
}
