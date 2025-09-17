/*instrumentation.ts*/
// Require dependencies
import { resourceFromAttributes } from '@opentelemetry/resources'
import { NodeSDK } from '@opentelemetry/sdk-node'
import {
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
} from '@opentelemetry/semantic-conventions'

import {
  commandBus,
  eventBus,
  Log,
  ofItems,
  queryBus,
  reportBus,
} from '@myko/core'
import { OTLPLogExporter } from '@opentelemetry/exporter-logs-otlp-http'
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-proto'
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-proto'

import {
  LoggerProvider,
  SimpleLogRecordProcessor,
} from '@opentelemetry/sdk-logs'
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics'
import type {
  MykoGatewayBootstrapOptions,
  StartupReportEntry,
} from '../bootstrap'
import {
  beginTraceTransaction,
  countEvent,
  countResults,
  endTraceTransaction,
} from './helpers'
import { exportMetrics } from './metrics'

export const initTracing = (
  opts: MykoGatewayBootstrapOptions,
): StartupReportEntry => {
  if (!opts?.tracing?.enabled) {
    throw new Error('Tracing is not enabled')
  }

  const traceExporter = new OTLPTraceExporter({
    url: `http://${opts.tracing.endpoint}/v1/traces`,
    timeoutMillis: 1000,
  })
  const metricExporter = new OTLPMetricExporter({
    url: `http://${opts.tracing.endpoint}/v1/metrics`,
  })

  const logExporter = new OTLPLogExporter({
    url: `http://${opts.tracing.endpoint}/v1/logs`,
    timeoutMillis: 5000,
  })

  const resource = resourceFromAttributes({
    [ATTR_SERVICE_NAME]: opts.tracing.serviceName,
    [ATTR_SERVICE_VERSION]: opts.tracing.serviceVersion,
  })

  const sdk = new NodeSDK({
    resource,
    traceExporter: traceExporter,
    metricReader: new PeriodicExportingMetricReader({
      exporter: metricExporter,
      exportIntervalMillis: 1000,
      exportTimeoutMillis: 1000,
    }),
  })

  const loggerProvider = new LoggerProvider({
    processors: [new SimpleLogRecordProcessor(logExporter)],
    resource,
  })

  const logger = loggerProvider.getLogger('myko-gateway')

  sdk.start()

  queryBus.onPrepare((ctx, callId) =>
    beginTraceTransaction(ctx, 'm:query', callId),
  )
  queryBus.onResult((ctx, callId) =>
    endTraceTransaction(ctx, 'm:query', callId),
  )
  queryBus.onFollowUp((ctx, callId) => countResults(ctx, 'm:query', callId))

  reportBus.onPrepare((ctx, callId) =>
    beginTraceTransaction(ctx, 'm:report', callId),
  )
  reportBus.onResult((ctx, callId) =>
    endTraceTransaction(ctx, 'm:report', callId),
  )
  reportBus.onFollowUp((ctx, callId) => countResults(ctx, 'm:report', callId))

  commandBus.onPrepare((ctx, callId) =>
    beginTraceTransaction(ctx, 'm:command', callId),
  )
  commandBus.onResult((ctx, callId) =>
    endTraceTransaction(ctx, 'm:command', callId),
  )

  eventBus.subject$.subscribe((x) => {
    countEvent(x)
  })

  eventBus.subject$.pipe(ofItems(Log)).subscribe((logEvent) => {
    const log = logEvent.item

    logger.emit({
      severityText: log.level,
      attributes: {
        loggerName: log.loggerName,
        data: log.data,
        serverId: log.serverId,
      },
      body: log.text,
    })
  })

  exportMetrics(opts)

  const lines = [
    `Tracing started`,
    `Service name: ${opts.tracing.serviceName}`,
    `Service version: ${opts.tracing.serviceVersion}`,
    `Endpoint: ${opts.tracing.endpoint}`,
  ]

  return {
    module: 'Tracing',
    lines,
  }
}
