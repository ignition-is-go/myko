/*instrumentation.ts*/
// Require dependencies
import { resourceFromAttributes } from '@opentelemetry/resources'
import { NodeSDK } from '@opentelemetry/sdk-node'
import {
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
} from '@opentelemetry/semantic-conventions'

import { commandBus, eventBus, queryBus, reportBus } from '@myko/core'
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-proto'
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-proto'
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
  const sdk = new NodeSDK({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: opts.tracing.serviceName,
      [ATTR_SERVICE_VERSION]: opts.tracing.serviceVersion,
    }),

    traceExporter: traceExporter,
    metricReader: new PeriodicExportingMetricReader({
      exporter: metricExporter,
      exportIntervalMillis: 1000,
      exportTimeoutMillis: 1000,
    }),
  })

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
