#!/usr/bin/env node
import { createReadStream } from 'node:fs'
import { createInterface } from 'node:readline'

const [path, ...extra] = process.argv.slice(2)
if (!path || extra.length) {
  console.error('Usage: node scripts/summarize-authority-trace.mjs TRACE.log')
  process.exit(2)
}

const requests = new Map()
const timings = new Map()
const transfers = { count: 0, applied: 0, duplicates: 0 }
const record = (name, elapsed) => {
  const values = timings.get(name) ?? []
  values.push(elapsed)
  timings.set(name, values)
}

for await (const line of createInterface({
  input: createReadStream(path),
  crlfDelay: Infinity,
})) {
  if (/^native |^test result:|^Error:/.test(line)) {
    console.log(
      line.replace(
        /AuthorizationBlocked .* after /,
        'AuthorizationBlocked after ',
      ),
    )
  }
  const time = Date.parse(line.split(' ', 1)[0])
  const id = line.match(/admission_id=([\da-f-]+)/)?.[1]
  const kind = line.match(/request="(\w+)"/)?.[1]
  if (id && kind && !Number.isFinite(time))
    throw new Error(`Invalid request timestamp: ${line}`)
  if (id && kind && !requests.has(id))
    requests.set(id, { time, kind, complete: false })
  if (id && line.includes('session request completed')) {
    const request = requests.get(id)
    if (request && !request.complete) {
      record(`request ${request.kind}`, time - request.time)
      request.complete = true
    }
  }
  const elapsed = line.match(/elapsed_ms=(\d+)/)?.[1]
  if (elapsed !== undefined) {
    if (line.includes('scoped evidence refreshed')) {
      const applied = line.match(/\bapplied=(\d+)/)?.[1]
      const duplicates = line.match(/\bduplicates=(\d+)/)?.[1]
      if (applied === undefined || duplicates === undefined)
        throw new Error(`Incomplete scope transfer counters: ${line}`)
      transfers.count++
      transfers.applied += Number(applied)
      transfers.duplicates += Number(duplicates)
      record('scoped evidence refresh', Number(elapsed))
    }
    const stage = line.match(
      /authority (history refreshed|prepare completed|proposal completed|accepts completed|synchronization completed)/,
    )?.[1]
    if (stage) record(`authority ${stage}`, Number(elapsed))
    if (line.includes('redb journal replay decoded'))
      record('journal replay', Number(elapsed))
    const operation = line.match(
      /operation=(ReadItems|FollowItems|FollowHandler)/,
    )?.[1]
    if (operation) {
      const outcome =
        line.match(/decision=(Permit|Deny|Challenge)/)?.[1] ??
        (line.includes('authority unavailable') ? 'Unavailable' : undefined)
      if (outcome) record(`${operation} ${outcome}`, Number(elapsed))
    }
  }
}

console.log('stage\tcount\ttotal_ms\tp50_ms\tmax_ms')
for (const [name, values] of [...timings].sort(([a], [b]) =>
  a.localeCompare(b),
)) {
  values.sort((a, b) => a - b)
  console.log(
    [
      name,
      values.length,
      values.reduce((sum, n) => sum + n, 0),
      values[Math.floor(values.length / 2)],
      values.at(-1),
    ].join('\t'),
  )
}
const pending = [...requests.values()].filter((request) => !request.complete)
console.log(`Scoped transfers: ${JSON.stringify(transfers)}`)
console.log(
  `Requests without completion records: ${pending.length}. Durations overlap; totals are not wall time.`,
)
