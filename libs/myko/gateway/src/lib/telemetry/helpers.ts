import { getHostId, type ID, type MEvent, type WithContext } from '@myko/core'
import {
  MCOMMAND_EVENT,
  MQUERY_EVENT,
  MREPORT_EVENT,
  type WSMMessage,
} from '@myko/ws'
import {
  // tracer,
  // tracer,
  transactionDurationGauge,
  transactionDurationHist,
} from './meters'
import {
  callLineageCounts,
  callStartTimes,
  eventCounts,
  lineageResults,
  spansByCall,
  spansByTx,
  tagCounts,
  tagResults,
  tagTimes,
} from './state'

export const beginTraceTransaction = (
  mContext: WithContext,
  event: 'm:query' | 'm:command' | 'm:report',
  callId: ID,
) => {
  const tag = mContext.getTag()
  const tx = mContext.tx

  // console.log(tag, mContext.lineage.join('>'))
  const t = mContext.lineage.join('>')
  callLineageCounts.set(t, (callLineageCounts.get(t) ?? 0) + 1)

  const startTime = performance.now()
  callStartTimes.set(callId, startTime)

  const existingTx = spansByTx.get(tx)

  if (existingTx) {
    // let ctx = trace.setSpan(context.active(), existingTx)
    // const childSpan = tracer().startSpan(
    //   tag,
    //   {
    //     attributes: {
    //       event,
    //       tag,
    //       hostId: getHostId(),
    //       lineage: mContext.lineage,
    //     },
    //   },
    //   ctx,
    // )
    // spansByCall.set(callId, childSpan)
  } else {
    // const span = tracer().startSpan(`Entry: ${tag}`, {
    //   attributes: {
    //     event,
    //     tag,
    //     hostId: getHostId(),
    //   },
    // })
    // spansByTx.set(tx, span)
    // spansByCall.set(callId, span)
  }

  const count = tagCounts.get(tag) ?? 0
  const newCount = count + 1
  tagCounts.set(tag, newCount)
}

export const endTraceTransaction = (
  mContext: WithContext,
  event: 'm:query' | 'm:command' | 'm:report',
  callId: ID,
) => {
  const startTime = callStartTimes.get(callId)
  const endTime = performance.now()
  const duration = startTime ? endTime - startTime : undefined
  callStartTimes.delete(callId)

  const tag = mContext.getTag()

  if (duration) {
    transactionDurationHist().record(duration, {
      event,
      tag,
      hostId: getHostId(),
    })
    transactionDurationGauge().record(duration, {
      event,
      tag,
      hostId: getHostId(),
    })

    const totalTime = tagTimes.get(tag) ?? 0
    const newTotalTime = totalTime + duration
    tagTimes.set(tag, newTotalTime)
  }

  spansByCall.get(callId)?.end()
  spansByCall.delete(callId)
}

export const countResults = (
  mContext: WithContext,
  _event: 'm:query' | 'm:report',
  _callId: ID,
) => {
  const tag = mContext.getTag()
  const count = tagResults.get(tag) ?? 0
  const newCount = count + 1
  tagResults.set(tag, newCount)

  const lineageTag = mContext.lineage.join('>')
  lineageResults.set(lineageTag, (lineageResults.get(lineageTag) ?? 0) + 1)
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

// export const initIncomingMessage = (m: WSMMessage) => {
//   const tx = getTx(m)
//   const startTime = performance.now()
//   transactionStartTimes.set(tx, startTime)

//   const attributes = getAttributes(m)
//   transactionAttributes.set(tx, attributes)
// }

// export const completeOutgoingMessage = (m: WSMMessage) => {
//   const tx = getTx(m)

//   const endTime = performance.now()

//   const txStartTime = transactionStartTimes.get(tx)
//   if (txStartTime) {
//     transactionStartTimes.delete(tx)
//     const duration = endTime - txStartTime
//     const attributes = transactionAttributes.get(tx)

//     transactionDurationHist().record(duration, {
//       event: m.event,
//       ...attributes,
//       hostId: getHostId(),
//     })
//     transactionDurationGauge().record(duration, {
//       event: m.event,
//       ...attributes,
//       hostId: getHostId(),
//     })
//   }
// }

// export const addTransactionAttribute = (
//   txId: ID,
//   key: string,
//   value: string | number,
// ) => {
//   const attributes = transactionAttributes.get(txId) ?? {}
//   attributes[key] = value
//   transactionAttributes.set(txId, attributes)
// }

export const countEvent = (e: MEvent) => {
  const count = eventCounts.get(e.itemType) ?? 0
  const newCount = count + 1
  eventCounts.set(e.itemType, newCount)
}
