import { getAllRepos, getHostId } from '@myko/core'
import { DateTime } from 'luxon'
import type { MykoGatewayBootstrapOptions } from '../bootstrap'
import {
  eventCountGuage,
  itemCountsGuage,
  lagGuage,
  serverGuage,
  transactioncountGuage,
  transactionLineageGuage,
  transactionLineageResultsGuage,
  transactionResultGuage,
  transactionTotalTime,
} from './meters'
import {
  callLineageCounts,
  eventCounts,
  lineageResults,
  tagCounts,
  tagResults,
  tagTimes,
} from './state'

export const exportMetrics = (args: MykoGatewayBootstrapOptions) => {
  let lastTime = performance.now()

  setInterval(async () => {
    for (const [key, value] of tagCounts.entries()) {
      transactioncountGuage().record(value, {
        tag: key,
        hostId: getHostId(),
      })
      tagCounts.set(key, 0)
    }
    for (const [key, value] of tagTimes.entries()) {
      transactionTotalTime().record(value, {
        tag: key,
        hostId: getHostId(),
      })
      tagTimes.set(key, 0)
    }

    for (const [key, value] of eventCounts.entries()) {
      eventCountGuage().record(value, {
        event: key,
        hostId: getHostId(),
      })
      eventCounts.set(key, 0)
    }

    for (const [key, value] of tagResults.entries()) {
      transactionResultGuage().record(value, {
        tag: key,
        hostId: getHostId(),
      })
      tagResults.set(key, 0)
    }

    for (const [key, value] of callLineageCounts.entries()) {
      transactionLineageGuage().record(value, {
        tag: key,
        hostId: getHostId(),
      })
      callLineageCounts.set(key, 0)
    }

    for (const [key, value] of lineageResults.entries()) {
      transactionLineageResultsGuage().record(value, {
        tag: key,
        hostId: getHostId(),
      })
      lineageResults.set(key, 0)
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

    let now = performance.now()
    let diff = now - lastTime
    lastTime = now
    lagGuage().record(diff, {
      hostId: getHostId(),
    })

    const repos = getAllRepos()

    for (const repo of repos) {
      const name = repo.entity

      const count = await repo.getItemCount()

      itemCountsGuage().record(count, {
        hostId: getHostId(),
        tag: name,
      })
    }
  }, 1000)
}
