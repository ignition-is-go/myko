/**
 * Call Rate Monitor
 *
 * Tracks query and report call rates to identify performance bottlenecks.
 * Enable with MYKO_CALL_RATE_LOGGING=true environment variable.
 *
 * Logs a summary every LOG_INTERVAL_MS showing the top callers.
 */

import { MykoLogger, queryBus, reportBus, type WithContext } from '@myko/core'

const log = new MykoLogger('CallRateMonitor')

// Configuration
const LOG_INTERVAL_MS = 5000 // Log every 5 seconds
const TOP_N = 15 // Show top N callers
const MIN_CALLS_TO_LOG = 10 // Only log if at least this many calls in the window
const MAX_LINEAGE_DEPTH = 3 // Show last N items in lineage chain

// State for tracking call counts in current window
const queryCallCounts = new Map<string, number>()
const reportCallCounts = new Map<string, number>()
const queryFollowUpCounts = new Map<string, number>()
const reportFollowUpCounts = new Map<string, number>()

// Track lineage chains (tag -> lineage chain -> count)
const queryLineageCounts = new Map<string, Map<string, number>>()
const reportLineageCounts = new Map<string, Map<string, number>>()

let isEnabled = false
let intervalHandle: ReturnType<typeof setInterval> | null = null

/**
 * Get a shortened lineage string for display
 */
function getLineageKey(ctx: WithContext): string {
  const lineage = (ctx as unknown as { lineage?: string[] }).lineage ?? []
  if (lineage.length === 0) return '(root)'
  // Take last N items to show the immediate call chain
  const recent = lineage.slice(-MAX_LINEAGE_DEPTH)
  return recent.join(' → ')
}

/**
 * Record a query call
 */
function recordQueryCall(ctx: WithContext) {
  if (!isEnabled) return
  const tag = ctx.getTag()
  queryCallCounts.set(tag, (queryCallCounts.get(tag) ?? 0) + 1)

  // Track lineage
  const lineageKey = getLineageKey(ctx)
  if (!queryLineageCounts.has(tag)) {
    queryLineageCounts.set(tag, new Map())
  }
  const tagLineage = queryLineageCounts.get(tag)!
  tagLineage.set(lineageKey, (tagLineage.get(lineageKey) ?? 0) + 1)
}

/**
 * Record a report call
 */
function recordReportCall(ctx: WithContext) {
  if (!isEnabled) return
  const tag = ctx.getTag()
  reportCallCounts.set(tag, (reportCallCounts.get(tag) ?? 0) + 1)

  // Track lineage
  const lineageKey = getLineageKey(ctx)
  if (!reportLineageCounts.has(tag)) {
    reportLineageCounts.set(tag, new Map())
  }
  const tagLineage = reportLineageCounts.get(tag)!
  tagLineage.set(lineageKey, (tagLineage.get(lineageKey) ?? 0) + 1)
}

/**
 * Record a query follow-up (subscription emission)
 */
function recordQueryFollowUp(ctx: WithContext) {
  if (!isEnabled) return
  const tag = ctx.getTag()
  queryFollowUpCounts.set(tag, (queryFollowUpCounts.get(tag) ?? 0) + 1)
}

/**
 * Record a report follow-up (subscription emission)
 */
function recordReportFollowUp(ctx: WithContext) {
  if (!isEnabled) return
  const tag = ctx.getTag()
  reportFollowUpCounts.set(tag, (reportFollowUpCounts.get(tag) ?? 0) + 1)
}

/**
 * Get top N entries from a map sorted by count
 */
function getTopEntries(
  map: Map<string, number>,
  n: number,
): Array<{ tag: string; count: number }> {
  return Array.from(map.entries())
    .map(([tag, count]) => ({ tag, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, n)
}

/**
 * Get top lineage chains for a tag
 */
function getTopLineages(
  lineageMap: Map<string, Map<string, number>>,
  tag: string,
  n: number,
): Array<{ chain: string; count: number }> {
  const tagLineage = lineageMap.get(tag)
  if (!tagLineage) return []
  return Array.from(tagLineage.entries())
    .map(([chain, count]) => ({ chain, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, n)
}

/**
 * Log the current call rate summary and reset counters
 */
function logAndReset() {
  const queryTop = getTopEntries(queryCallCounts, TOP_N)
  const reportTop = getTopEntries(reportCallCounts, TOP_N)
  const queryFollowUpTop = getTopEntries(queryFollowUpCounts, TOP_N)
  const reportFollowUpTop = getTopEntries(reportFollowUpCounts, TOP_N)

  const totalQueryCalls = Array.from(queryCallCounts.values()).reduce(
    (a, b) => a + b,
    0,
  )
  const totalReportCalls = Array.from(reportCallCounts.values()).reduce(
    (a, b) => a + b,
    0,
  )
  const totalQueryFollowUps = Array.from(queryFollowUpCounts.values()).reduce(
    (a, b) => a + b,
    0,
  )
  const totalReportFollowUps = Array.from(reportFollowUpCounts.values()).reduce(
    (a, b) => a + b,
    0,
  )

  const hasActivity =
    totalQueryCalls >= MIN_CALLS_TO_LOG ||
    totalReportCalls >= MIN_CALLS_TO_LOG ||
    totalQueryFollowUps >= MIN_CALLS_TO_LOG ||
    totalReportFollowUps >= MIN_CALLS_TO_LOG

  if (hasActivity) {
    const ratePerSec = (count: number) =>
      ((count / LOG_INTERVAL_MS) * 1000).toFixed(1)

    log.info('=== Call Rate Summary (last 5s) ===')

    if (totalQueryCalls >= MIN_CALLS_TO_LOG) {
      log.info(`Query Calls: ${totalQueryCalls} (${ratePerSec(totalQueryCalls)}/s)`)
      queryTop
        .filter((e) => e.count >= 5)
        .forEach((e) => {
          log.info(`  ${e.tag}: ${e.count} (${ratePerSec(e.count)}/s)`)
          // Show top lineage chains for high-volume queries
          if (e.count >= 100) {
            const topLineages = getTopLineages(queryLineageCounts, e.tag, 3)
            topLineages.forEach((l) => {
              log.info(`    ← ${l.chain}: ${l.count}`)
            })
          }
        })
    }

    if (totalReportCalls >= MIN_CALLS_TO_LOG) {
      log.info(`Report Calls: ${totalReportCalls} (${ratePerSec(totalReportCalls)}/s)`)
      reportTop
        .filter((e) => e.count >= 5)
        .forEach((e) => {
          log.info(`  ${e.tag}: ${e.count} (${ratePerSec(e.count)}/s)`)
          // Show top lineage chains for high-volume reports
          if (e.count >= 100) {
            const topLineages = getTopLineages(reportLineageCounts, e.tag, 3)
            topLineages.forEach((l) => {
              log.info(`    ← ${l.chain}: ${l.count}`)
            })
          }
        })
    }

    if (totalQueryFollowUps >= MIN_CALLS_TO_LOG) {
      log.info(
        `Query Follow-ups (emissions): ${totalQueryFollowUps} (${ratePerSec(totalQueryFollowUps)}/s)`,
      )
      queryFollowUpTop
        .filter((e) => e.count >= 5)
        .forEach((e) => {
          log.info(`  ${e.tag}: ${e.count} (${ratePerSec(e.count)}/s)`)
        })
    }

    if (totalReportFollowUps >= MIN_CALLS_TO_LOG) {
      log.info(
        `Report Follow-ups (emissions): ${totalReportFollowUps} (${ratePerSec(totalReportFollowUps)}/s)`,
      )
      reportFollowUpTop
        .filter((e) => e.count >= 5)
        .forEach((e) => {
          log.info(`  ${e.tag}: ${e.count} (${ratePerSec(e.count)}/s)`)
        })
    }

    log.info('===================================')
  }

  // Reset counters
  queryCallCounts.clear()
  reportCallCounts.clear()
  queryFollowUpCounts.clear()
  reportFollowUpCounts.clear()
  queryLineageCounts.clear()
  reportLineageCounts.clear()
}

/**
 * Initialize call rate monitoring
 * Enabled via MYKO_CALL_RATE_LOGGING=true environment variable
 */
export function initCallRateMonitor(): void {
  isEnabled = process.env.MYKO_CALL_RATE_LOGGING === 'true'

  if (!isEnabled) {
    return
  }

  log.info('Call rate monitoring enabled (MYKO_CALL_RATE_LOGGING=true)')

  // Hook into query bus
  queryBus.onPrepare((ctx) => recordQueryCall(ctx))
  queryBus.onFollowUp((ctx) => recordQueryFollowUp(ctx))

  // Hook into report bus
  reportBus.onPrepare((ctx) => recordReportCall(ctx))
  reportBus.onFollowUp((ctx) => recordReportFollowUp(ctx))

  // Start periodic logging
  intervalHandle = setInterval(logAndReset, LOG_INTERVAL_MS)
}

/**
 * Stop call rate monitoring
 */
export function stopCallRateMonitor(): void {
  if (intervalHandle) {
    clearInterval(intervalHandle)
    intervalHandle = null
  }
  isEnabled = false
}
