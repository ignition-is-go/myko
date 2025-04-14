import { getHostId } from '@myko/core'
import { DateTime } from 'luxon'
import type { MykoGatewayBootstrapOptions } from '../bootstrap'
import {
  eventCountGuage,
  serverGuage,
  transactioncountGuage,
  transactionResultGuage,
  transactionTotalTime,
} from './meters'
import { eventCounts, txCounts, txResults, txTimes } from './state'

export const exportMetrics = (args: MykoGatewayBootstrapOptions) => {
  setInterval(() => {
    for (const [key, value] of txCounts.entries()) {
      transactioncountGuage().record(value, {
        tag: key,
        hostId: getHostId(),
      })
      txCounts.set(key, 0)
    }
    for (const [key, value] of txTimes.entries()) {
      transactionTotalTime().record(value, {
        tag: key,
        hostId: getHostId(),
      })
      txTimes.set(key, 0)
    }

    for (const [key, value] of eventCounts.entries()) {
      eventCountGuage().record(value, {
        event: key,
        hostId: getHostId(),
      })
      eventCounts.set(key, 0)
    }

    for (const [key, value] of txResults.entries()) {
      transactionResultGuage().record(value, {
        tag: key,
        hostId: getHostId(),
      })
      txResults.set(key, 0)
    }

    if (args.ws) {
      const time = DateTime.now().toMillis()
      serverGuage().record(time, {
        hostId: getHostId(),
        version: args.version,
        host: args.ws.host,
        port: args.ws.port,
      })
    }
  }, 1000)
}
