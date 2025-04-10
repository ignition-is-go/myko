import {
  metrics,
  type Gauge,
  type Histogram,
  type Meter,
} from '@opentelemetry/api'

let _meter: Meter | null

export const meter = () => {
  if (_meter) {
    return _meter
  }
  _meter = metrics.getMeter('MykoGateway')
  return _meter
}

let _transactionDurationHist: Histogram | null = null
export const transactionDurationHist = () => {
  if (!_transactionDurationHist) {
    _transactionDurationHist = meter().createHistogram(
      'myko.gateway.client.transaction.duration.hist',
    )
  }
  return _transactionDurationHist
}

let _transactionDurationGauge: Gauge | null = null
export const transactionDurationGauge = () => {
  if (!_transactionDurationGauge) {
    _transactionDurationGauge = meter().createGauge(
      'myko.gateway.client.transaction.duration',
    )
  }
  return _transactionDurationGauge
}

let _transactioncountGuage: Gauge | null = null
export const transactioncountGuage = () => {
  if (!_transactioncountGuage) {
    _transactioncountGuage = meter().createGauge(
      'myko.gateway.client.transaction.rate',
    )
  }
  return _transactioncountGuage
}

let _transactionTotalTime: Gauge | null = null
export const transactionTotalTime = () => {
  if (!_transactionTotalTime) {
    _transactionTotalTime = meter().createGauge(
      'myko.gateway.client.transaction.duration.total',
    )
  }
  return _transactionTotalTime
}

let _eventCountGuage: Gauge | null = null
export const eventCountGuage = () => {
  if (!_eventCountGuage) {
    _eventCountGuage = meter().createGauge('myko.gateway.client.event.rate')
  }
  return _eventCountGuage
}

let _transactionResultGuage: Gauge | null = null
export const transactionResultGuage = () => {
  if (!_transactionResultGuage) {
    _transactionResultGuage = meter().createGauge(
      'myko.gateway.client.transaction.result.total',
    )
  }
  return _transactionResultGuage
}

let _serverGuage: Gauge | null = null
export const serverGuage = () => {
  if (!_serverGuage) {
    _serverGuage = meter().createGauge('myko.gateway.server')
  }
  return _serverGuage
}

// let _tracer: Tracer | null = null
// export const tracer = () => {
//   if (!_tracer) {
//     _tracer = trace.getTracer('MykoGateway')
//   }
//   return _tracer
// }
