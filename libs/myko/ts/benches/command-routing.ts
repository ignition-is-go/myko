import { filter, Subject } from 'rxjs'

const COMMAND_COUNT = 5_000
const SAMPLES = 7

type Response = { tx: string; value: number }
type Failure = { tx: string; message: string }

let consumed = 0

function legacyFilteredSubjects(): void {
  const responses = new Subject<Response>()
  const failures = new Subject<Failure>()

  for (let index = 0; index < COMMAND_COUNT; index += 1) {
    const tx = `command-${index}`
    const responseSub = responses
      .pipe(filter((response) => response.tx === tx))
      .subscribe((response) => {
        responseSub.unsubscribe()
        failureSub.unsubscribe()
        consumed += response.value
      })
    const failureSub = failures
      .pipe(filter((failure) => failure.tx === tx))
      .subscribe(() => {
        responseSub.unsubscribe()
        failureSub.unsubscribe()
      })
  }

  for (let index = 0; index < COMMAND_COUNT; index += 1) {
    responses.next({ tx: `command-${index}`, value: index })
  }
}

function directTransactionMap(): void {
  const pending = new Map<string, (response: Response) => void>()
  for (let index = 0; index < COMMAND_COUNT; index += 1) {
    const tx = `command-${index}`
    pending.set(tx, (response) => {
      pending.delete(tx)
      consumed += response.value
    })
  }

  for (let index = 0; index < COMMAND_COUNT; index += 1) {
    const response = { tx: `command-${index}`, value: index }
    pending.get(response.tx)?.(response)
  }
}

function median(samples: number[]): number {
  const sorted = samples.toSorted((a, b) => a - b)
  return sorted[Math.floor(sorted.length / 2)]
}

function measure(run: () => void): number {
  run()
  const samples = Array.from({ length: SAMPLES }, () => {
    const started = performance.now()
    run()
    return performance.now() - started
  })
  return median(samples)
}

const legacyMs = measure(legacyFilteredSubjects)
const directMs = measure(directTransactionMap)

console.table({
  'filtered RxJS subjects': {
    commands: COMMAND_COUNT,
    medianMs: legacyMs.toFixed(3),
    relative: '1.0x',
  },
  'direct transaction map': {
    commands: COMMAND_COUNT,
    medianMs: directMs.toFixed(3),
    relative: `${(legacyMs / directMs).toFixed(1)}x faster`,
  },
})

if (consumed === 0) throw new Error('benchmark result was not consumed')
