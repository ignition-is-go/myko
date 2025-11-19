/**
 * Aggregated Client Command Monitor
 *
 * Purpose:
 * - Provide high-signal, low-noise, periodic diagnostics for client command round-trips
 * - Minimal per-event overhead; single-line periodic reports instead of per-command logs
 *
 * Enable via env:
 *   MYKO_CCMD_MONITOR=1
 * Optional env:
 *   MYKO_CCMD_MONITOR_INTERVAL_MS=2000
 *   MYKO_CCMD_MONITOR_TOP=8                // number of top tags to show per interval
 *   MYKO_CCMD_MONITOR_TAGS=true|false      // enable per-tag aggregation (default true)
 *
 * Usage from handlers:
 *   import { clientCommandMonitor as ccm } from './telemetry/client_command.monitor'
 *
 *   ccm.begin(tag, tx, clientId)           // when dispatching command to a client
 *   ccm.ok(tx)                             // when a client response is received (success)
 *   ccm.err(tx, message?)                  // when client responds with error
 *   ccm.disconnected(tx)                   // when the client disconnects before response
 *   ccm.timeout(tx)                        // when awaiting response times out (if you add a timeout)
 *   ccm.forward(tag, tx, fromServerId, toServerId) // when forwarded to a peer
 *
 * You can call ccm.start() to force start, otherwise it auto-starts if MYKO_CCMD_MONITOR is enabled.
 */

import type { ID } from '@myko/core'

type TxMeta = {
  tag: string
  clientId?: ID
  startedAt: number
}

type PeriodCounters = {
  dispatched: number
  ok: number
  err: number
  timeout: number
  disconnected: number
  forwarded: number
  orphanEnd: number
}

type TotalsCounters = PeriodCounters & {
  everPendingMax: number
}

type Hist = {
  buckets: number[] // counts per threshold
  // thresholds define the upper bound for a bucket, last bucket is ">= last"
  thresholds: number[]
  count: number
  sum: number
  min: number
  max: number
}

type TagPeriod = {
  dispatched: number
  ok: number
  err: number
  timeout: number
  disconnected: number
  avgMsSum: number
  avgMsCount: number
}

const DEFAULT_THRESHOLDS_MS = [5, 10, 25, 50, 100, 250, 500, 1000, 2000]

const makeHist = (thresholds = DEFAULT_THRESHOLDS_MS): Hist => ({
  buckets: new Array(thresholds.length + 1).fill(0),
  thresholds,
  count: 0,
  sum: 0,
  min: Number.POSITIVE_INFINITY,
  max: 0,
})

const addLatency = (h: Hist, ms: number) => {
  h.count++
  h.sum += ms
  if (ms < h.min) h.min = ms
  if (ms > h.max) h.max = ms
  const idx =
    h.thresholds.findIndex((t) => ms < t) === -1
      ? h.buckets.length - 1
      : h.thresholds.findIndex((t) => ms < t)
  h.buckets[idx]++
}

const approxPercentile = (h: Hist, p: number): number | null => {
  if (h.count === 0) return null
  const target = Math.ceil(h.count * p)
  let acc = 0
  for (let i = 0; i < h.buckets.length; i++) {
    acc += h.buckets[i]
    if (acc >= target) {
      // return upper bound of bucket as a coarse estimate
      if (i < h.thresholds.length) return h.thresholds[i]
      return h.max
    }
  }
  return h.max
}

const fmtMs = (ms: number | null | undefined) =>
  ms == null ? '-' : `${Math.round(ms)}ms`

const safeNowIso = () => {
  try {
    return new Date().toISOString()
  } catch {
    return `${Date.now()}`
  }
}

class ClientCommandMonitor {
  private enabled = false
  private intervalMs: number
  private topN: number
  private enableTags: boolean

  private tick: ReturnType<typeof setInterval> | null = null

  private pending = new Map<ID, TxMeta>() // tx -> meta

  private totals: TotalsCounters = {
    dispatched: 0,
    ok: 0,
    err: 0,
    timeout: 0,
    disconnected: 0,
    forwarded: 0,
    orphanEnd: 0,
    everPendingMax: 0,
  }

  private period: PeriodCounters = {
    dispatched: 0,
    ok: 0,
    err: 0,
    timeout: 0,
    disconnected: 0,
    forwarded: 0,
    orphanEnd: 0,
  }

  private periodHist: Hist = makeHist()
  private totalsHist: Hist = makeHist()

  // per-tag aggregation for this period
  private tagPeriod = new Map<string, TagPeriod>()

  constructor() {
    const e = (name: string) =>
      (typeof process !== 'undefined' && process.env?.[name]) || undefined

    this.enabled =
      (e('MYKO_CCMD_MONITOR') || '').toString().toLowerCase() === '1' ||
      (e('MYKO_CCMD_MONITOR') || '').toString().toLowerCase() === 'true'
    this.intervalMs =
      Number(e('MYKO_CCMD_MONITOR_INTERVAL_MS')) || /* default */ 2000
    this.topN = Number(e('MYKO_CCMD_MONITOR_TOP')) || 8
    const tagsEnv = (e('MYKO_CCMD_MONITOR_TAGS') || '').toLowerCase()
    this.enableTags = tagsEnv ? tagsEnv === '1' || tagsEnv === 'true' : true

    if (this.enabled) this.start()
  }

  start() {
    if (this.enabled && this.tick) return
    this.enabled = true
    this.tick = setInterval(() => this.report(), this.intervalMs)
  }

  stop() {
    if (!this.enabled) return
    this.enabled = false
    if (this.tick) {
      clearInterval(this.tick as any)
      this.tick = null
    }
  }

  begin(tag: string, tx: ID, clientId?: ID) {
    if (!this.enabled) return
    const meta: TxMeta = { tag, clientId, startedAt: performance.now() }
    this.pending.set(tx, meta)
    this.period.dispatched++
    this.totals.dispatched++
    if (this.pending.size > this.totals.everPendingMax) {
      this.totals.everPendingMax = this.pending.size
    }
    if (this.enableTags) {
      const p = this.tagPeriod.get(tag) || {
        dispatched: 0,
        ok: 0,
        err: 0,
        timeout: 0,
        disconnected: 0,
        avgMsSum: 0,
        avgMsCount: 0,
      }
      p.dispatched++
      this.tagPeriod.set(tag, p)
    }
  }

  ok(tx: ID) {
    if (!this.enabled) return
    const meta = this.pending.get(tx)
    if (!meta) {
      this.period.orphanEnd++
      this.totals.orphanEnd++
      return
    }
    this.pending.delete(tx)
    const ms = performance.now() - meta.startedAt
    this.period.ok++
    this.totals.ok++
    addLatency(this.periodHist, ms)
    addLatency(this.totalsHist, ms)
    if (this.enableTags) {
      const p = this.tagPeriod.get(meta.tag)
      if (p) {
        p.ok++
        p.avgMsSum += ms
        p.avgMsCount++
      }
    }
  }

  err(tx: ID, _message?: string) {
    if (!this.enabled) return
    const meta = this.pending.get(tx)
    if (!meta) {
      this.period.orphanEnd++
      this.totals.orphanEnd++
      return
    }
    this.pending.delete(tx)
    const ms = performance.now() - meta.startedAt
    this.period.err++
    this.totals.err++
    addLatency(this.periodHist, ms)
    addLatency(this.totalsHist, ms)
    if (this.enableTags) {
      const p = this.tagPeriod.get(meta.tag)
      if (p) {
        p.err++
        p.avgMsSum += ms
        p.avgMsCount++
      }
    }
  }

  timeout(tx: ID) {
    if (!this.enabled) return
    const meta = this.pending.get(tx)
    if (!meta) {
      this.period.orphanEnd++
      this.totals.orphanEnd++
      return
    }
    this.pending.delete(tx)
    const ms = performance.now() - meta.startedAt
    this.period.timeout++
    this.totals.timeout++
    addLatency(this.periodHist, ms)
    addLatency(this.totalsHist, ms)
    if (this.enableTags) {
      const p = this.tagPeriod.get(meta.tag)
      if (p) {
        p.timeout++
        p.avgMsSum += ms
        p.avgMsCount++
      }
    }
  }

  disconnected(tx: ID) {
    if (!this.enabled) return
    const meta = this.pending.get(tx)
    if (!meta) {
      this.period.orphanEnd++
      this.totals.orphanEnd++
      return
    }
    this.pending.delete(tx)
    const ms = performance.now() - meta.startedAt
    this.period.disconnected++
    this.totals.disconnected++
    addLatency(this.periodHist, ms)
    addLatency(this.totalsHist, ms)
    if (this.enableTags) {
      const p = this.tagPeriod.get(meta.tag)
      if (p) {
        p.disconnected++
        p.avgMsSum += ms
        p.avgMsCount++
      }
    }
  }

  forward(_tag: string, _tx: ID, _fromServerId?: ID, _toServerId?: ID) {
    if (!this.enabled) return
    this.period.forwarded++
    this.totals.forwarded++
  }

  private report() {
    // Snapshot and reset period counters/histogram/tag map
    const pend = this.pending.size

    const p = this.period
    this.period = {
      dispatched: 0,
      ok: 0,
      err: 0,
      timeout: 0,
      disconnected: 0,
      forwarded: 0,
      orphanEnd: 0,
    }

    const h = this.periodHist
    this.periodHist = makeHist()

    // Compute throughput per sec for this period
    const perSec = (n: number) =>
      (n / (this.intervalMs / 1000)).toFixed(1)

    const p50 = approxPercentile(h, 0.5)
    const p95 = approxPercentile(h, 0.95)
    const p99 = approxPercentile(h, 0.99)
    const avg = h.count ? h.sum / h.count : null

    // Single compact line
    const head = `[CCMD] ${safeNowIso()}`
    const totalsStr = `dispatched=${p.dispatched} ok=${p.ok} err=${p.err} timeout=${p.timeout} disc=${p.disconnected} fwd=${p.forwarded} orphan=${p.orphanEnd}`
    const rateStr = `rate/s=${perSec(p.dispatched)} pend=${pend} maxPend=${this.totals.everPendingMax}`
    const latStr = `lat(ms): avg=${fmtMs(avg)} p50=${fmtMs(p50)} p95=${fmtMs(p95)} p99=${fmtMs(p99)}`
    // eslint-disable-next-line no-console
    console.log(`${head} ${totalsStr} | ${rateStr} | ${latStr}`)

    // Optional per-tag breakdown (top N)
    if (this.enableTags && this.tagPeriod.size > 0) {
      const entries = Array.from(this.tagPeriod.entries()).map(([tag, v]) => {
        const avgMs =
          v.avgMsCount > 0 ? Math.round(v.avgMsSum / v.avgMsCount) : null
        const score = v.dispatched // sort by dispatched in this period
        return {
          tag,
          score,
          d: v.dispatched,
          ok: v.ok,
          err: v.err,
          to: v.timeout,
          dc: v.disconnected,
          avg: avgMs,
        }
      })
      entries.sort((a, b) => b.score - a.score)
      const top = entries.slice(0, this.topN)
      if (top.length > 0) {
        const parts = top.map((e) => {
          return `${e.tag}{d=${e.d},ok=${e.ok},err=${e.err},to=${e.to},dc=${e.dc},avg=${e.avg ?? '-' }ms}`
        })
        // eslint-disable-next-line no-console
        console.log(`[CCMD] top=${parts.join(' | ')}`)
      }
      this.tagPeriod.clear()
    }
  }
}

// Export singleton with safe no-op API when disabled
const monitor = new ClientCommandMonitor()

export const clientCommandMonitor = {
  start: () => monitor.start(),
  stop: () => monitor.stop(),
  begin: (tag: string, tx: ID, clientId?: ID) => monitor.begin(tag, tx, clientId),
  ok: (tx: ID) => monitor.ok(tx),
  err: (tx: ID, message?: string) => monitor.err(tx, message),
  timeout: (tx: ID) => monitor.timeout(tx),
  disconnected: (tx: ID) => monitor.disconnected(tx),
  forward: (tag: string, tx: ID, fromServerId?: ID, toServerId?: ID) =>
    monitor.forward(tag, tx, fromServerId, toServerId),
}
