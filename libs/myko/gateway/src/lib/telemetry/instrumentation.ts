/*instrumentation.ts*/
// Require dependencies
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node'
import { resourceFromAttributes } from '@opentelemetry/resources'
import { NodeSDK } from '@opentelemetry/sdk-node'
import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions'

import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-proto'
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-proto'
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics'

export const initTracing = () => {
  const traceExporter = new OTLPTraceExporter({
    url: `http://b.rship.io:4318/v1/traces`,
  })
  const metricExporter = new OTLPMetricExporter({
    url: `http://b.rship.io:4318/v1/metrics`,
  })
  const sdk = new NodeSDK({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: 'rship-server',
      // [ATTR_SERVICE_VERSION]: '',
    }),

    instrumentations: [getNodeAutoInstrumentations()],
    traceExporter: traceExporter,
    metricReader: new PeriodicExportingMetricReader({
      exporter: metricExporter,
    }),
  })

  console.log('START TRACING')
  sdk.start()

  setInterval(() => {
    traceExporter.forceFlush()
    metricExporter.forceFlush()
  }, 1000)
}
