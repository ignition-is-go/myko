/*instrumentation.ts*/
// Require dependencies

// export const initTracing = ({
//   appName,
//   appVersion,
//   collectorUrl,
// }: {
//   appName: string
//   appVersion: string
//   collectorUrl: string
// }) => {
//   const runner = new NodeSDK({
//     resource: resourceFromAttributes({
//       [ATTR_SERVICE_NAME]: appName,
//       [ATTR_SERVICE_VERSION]: appVersion,
//     }),

//     instrumentations: [getNodeAutoInstrumentations()],
//     traceExporter: new OTLPTraceExporter({
//       url: `http://b.rship.io/v1/traces`,
//     }),
//     metricReader: new PeriodicExportingMetricReader({
//       exporter: new OTLPMetricExporter({
//         url: `http://b.rship.io/v1/metrics`,
//       }),
//     }),
//   })

//   runner.start()
//   console.log('')
//   console.log(`Initializing tracing for ${appName} v${appVersion}`)
//   console.log(`Collector URL: ${collectorUrl}`)
// }
