/*instrumentation.ts*/
// Require dependencies
import { resourceFromAttributes } from '@opentelemetry/resources'
import { NodeSDK } from '@opentelemetry/sdk-node'
import {
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
} from '@opentelemetry/semantic-conventions'

import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-proto'
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-proto'
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics'
import type {
  MykoGatewayBootstrapOptions,
  StartupReportEntry,
} from '../bootstrap'

export const initTracing = (
  opts: MykoGatewayBootstrapOptions['tracing'],
): StartupReportEntry => {
  if (!opts?.enabled) {
    throw new Error('Tracing is not enabled')
  }

  const traceExporter = new OTLPTraceExporter({
    url: `http://${opts.endpoint}/v1/traces`,
    timeoutMillis: 1000,
  })
  const metricExporter = new OTLPMetricExporter({
    url: `http://${opts.endpoint}/v1/metrics`,
  })
  const sdk = new NodeSDK({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: opts.serviceName,
      [ATTR_SERVICE_VERSION]: opts.serviceVersion,
    }),

    traceExporter: traceExporter,
    metricReader: new PeriodicExportingMetricReader({
      exporter: metricExporter,
      exportIntervalMillis: 1000,
      exportTimeoutMillis: 1000,
    }),
  })

  sdk.start()

  const lines = [
    `Tracing started`,
    `Service name: ${opts.serviceName}`,
    `Service version: ${opts.serviceVersion}`,
    `Endpoint: ${opts.endpoint}`,
  ]

  return {
    module: 'Tracing',
    lines,
  }
}
