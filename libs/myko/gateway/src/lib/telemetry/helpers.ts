import { getHostId, type ID, type MEvent } from '@myko/core'
import {
  getTx,
  MCOMMAND_EVENT,
  MQUERY_EVENT,
  MREPORT_EVENT,
  type WSMMessage,
} from '@myko/ws'
import { metrics, trace } from '@opentelemetry/api'

export const transactionStartTimes = new Map<ID, number>()
export const transactionAttributes = new Map<
  ID,
  { [key: string]: string | number }
>()

const spans = new Map<ID, any>()

const eventCounts = new Map<string, number>()

const txCounts = new Map<ID, number>()

const txTimes = new Map<ID, number>()

const tracer = trace.getTracer('MykoGateway')

const meter = metrics.getMeter('MykoGateway')

const transactionDurationHist = meter.createHistogram(
  'myko.gateway.client.transaction.duration.hist',
)
const transactionDurationGauge = meter.createGauge(
  'myko.gateway.client.transaction.duration',
)

const transactioncountGuage = meter.createGauge(
  'myko.gateway.client.transaction.rate',
)

const transactionTotalTime = meter.createGauge(
  'myko.gateway.client.transaction.duration.total',
)

const eventCountGuage = meter.createGauge('myko.gateway.client.event.rate')

export const beginTraceTransaction = (
  tx: ID,
  tag: string,
  event: 'm:query' | 'm:command' | 'm:report',
) => {
  const startTime = performance.now()
  transactionStartTimes.set(tx, startTime)
  tracer.startActiveSpan(tag, (span) => {
    spans.set(tx, span)
  })

  const count = txCounts.get(tag) ?? 0
  const newCount = count + 1
  txCounts.set(tag, newCount)
}

export const endTraceTransaction = (
  tx: ID,
  tag: string,
  event: 'm:query' | 'm:command' | 'm:report',
) => {
  const startTime = transactionStartTimes.get(tx)
  const endTime = performance.now()
  const duration = startTime ? endTime - startTime : undefined

  if (duration) {
    transactionDurationHist.record(duration, {
      event,
      tag,
      hostId: getHostId(),
    })
    transactionDurationGauge.record(duration, {
      event,
      tag,
      hostId: getHostId(),
    })

    const totalTime = txTimes.get(tag) ?? 0
    const newTotalTime = totalTime + duration
    txTimes.set(tag, newTotalTime)
  }

  transactionStartTimes.delete(tx)
  const span = spans.get(tx)
  if (span) {
    span.end()
    spans.delete(tx)
  }
}

export const addTransactionAttribute = (
  txId: ID,
  key: string,
  value: string | number,
) => {
  const attributes = transactionAttributes.get(txId) ?? {}
  attributes[key] = value
  transactionAttributes.set(txId, attributes)
}

export const getAttributes = (
  m: WSMMessage,
): {
  [key: string]: string | number
} => {
  switch (m.event) {
    case MCOMMAND_EVENT: {
      const data = m.data
      return {
        tag: data.commandId,
      }
    }
    case MQUERY_EVENT: {
      const data = m.data
      return {
        tag: data.queryId,
      }
    }
    case MREPORT_EVENT: {
      const data = m.data
      return {
        tag: data.reportId,
      }
    }
    default: {
      return {}
    }
  }
}

export const initIncomingMessage = (m: WSMMessage) => {
  const tx = getTx(m)
  const startTime = performance.now()
  transactionStartTimes.set(tx, startTime)

  const attributes = getAttributes(m)
  transactionAttributes.set(tx, attributes)
}

export const completeOutgoingMessage = (m: WSMMessage) => {
  const tx = getTx(m)

  const endTime = performance.now()

  const txStartTime = transactionStartTimes.get(tx)
  if (txStartTime) {
    transactionStartTimes.delete(tx)
    const duration = endTime - txStartTime
    const attributes = transactionAttributes.get(tx)

    transactionDurationHist.record(duration, {
      event: m.event,
      ...attributes,
      hostId: getHostId(),
    })
    transactionDurationGauge.record(duration, {
      event: m.event,
      ...attributes,
      hostId: getHostId(),
    })
  }
}

export const processEvent = (e: MEvent) => {
  const count = eventCounts.get(e.itemType) ?? 0
  const newCount = count + 1
  eventCounts.set(e.itemType, newCount)
}

setInterval(() => {
  for (const [key, value] of txCounts.entries()) {
    transactioncountGuage.record(value, {
      tag: key,
      hostId: getHostId(),
    })
    txCounts.set(key, 0)
  }
  for (const [key, value] of txTimes.entries()) {
    transactionTotalTime.record(value, {
      tag: key,
      hostId: getHostId(),
    })
    txTimes.set(key, 0)
  }

  for (const [key, value] of eventCounts.entries()) {
    eventCountGuage.record(value, {
      event: key,
      hostId: getHostId(),
    })
    eventCounts.set(key, 0)
  }
}, 1000)
