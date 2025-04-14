import {
  MykoLogger,
  Server,
  commandBus,
  commandHandlers,
  commands,
  eventBus,
  getHistoryProvider,
  getHostId,
  onAllInit,
  queries,
  queryBus,
  queryHandlers,
  reportBus,
  reportHandlers,
  reports,
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
import { Subject } from 'rxjs'
import { setAdapterBusses, setAdapterResult, setAuth } from '../registry'
import {
  beginTraceTransaction,
  countResults,
  endTraceTransaction,
  processEvent,
} from '../telemetry/helpers'
import { initTracing } from '../telemetry/instrumentation'
import { exportMetrics } from '../telemetry/metrics'
import { handleMessage } from './message.handler'
import type { MykoGatewayBootstrapOptions, StartupReportEntry } from './types'

export const bootstrap = async (args: MykoGatewayBootstrapOptions) => {
  const { version } = args

  const startupReport: StartupReportEntry[] = []
  const serverId = getHostId()

  if (args.historyProvider) {
    setHistoryProvider(args.historyProvider)
  }

  await getHistoryProvider().init()

  startupReport.push({
    lines: [`Starting: ${version}`, `Host ID: ${serverId}`],
    module: 'Bootstrap',
  })

  if (args.tracing?.enabled) {
    const report = initTracing(args.tracing)
    startupReport.push(report)

    queryBus.onPrepare((tx, tag) => beginTraceTransaction(tx, tag, 'm:query'))
    queryBus.onResult((tx, tag) => endTraceTransaction(tx, tag, 'm:query'))
    queryBus.onFollowUp((tx, tag) => countResults(tx, tag, 'm:query'))

    reportBus.onPrepare((tx, tag) => beginTraceTransaction(tx, tag, 'm:report'))
    reportBus.onResult((tx, tag) => endTraceTransaction(tx, tag, 'm:report'))
    reportBus.onFollowUp((tx, tag) => countResults(tx, tag, 'm:report'))

    commandBus.onPrepare((tx, tag) =>
      beginTraceTransaction(tx, tag, 'm:command'),
    )
    commandBus.onResult((tx, tag) => endTraceTransaction(tx, tag, 'm:command'))

    eventBus.subject$.subscribe((x) => {
      processEvent(x)
    })

    exportMetrics(args)
  }

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
        console.error('Error in rx', e)
      },
    })

    setAdapterBusses({ tx, rx })

    const res = wsAdapter({
      port,
      rx,
      tx,
      serverId,
    })

    setAdapterResult(res)

    const server = new Server({
      address: host,
      id: serverId,
      port: port,
      startedAt: DateTime.utc().toISO(),
      version: args.version,
    })

    const lines = [`Listening: ${host}:${port}`]

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
    eventBus.subject$.subscribe((x) => {
      if (logsPreventedEntities.has(x.itemType)) {
        return
      }

      new MykoLogger(x.itemType).info(`${x.changeType} - ${x.item.id}`)
    })
  })
}
