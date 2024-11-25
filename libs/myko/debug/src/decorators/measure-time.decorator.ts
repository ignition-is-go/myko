const timeCache = new Map<
  string,
  { totalTime: number; numCalls: number; avgTime: number }
>()

let interval

let enable = false
try {
  console.log(process.env['MYKO_MEASURE'])
  enable = process.env['MYKO_MEASURE'] === 'true'
  if (enable) {
    console.log('MYKO_MEASURE enabled')
  }
} catch (e) {
  console.log(e)
}

export const setStart = (key: string) => {
  performance.mark(`${key}-start`)
}

export const setEnd = (key: string) => {
  performance.mark(`${key}-end`)
  const a = performance.measure(key, `${key}-start`, `${key}-end`)
  const c = timeCache.get(key)

  const numCalls = (c?.numCalls ?? 0) + 1
  const totalTime = (c?.totalTime ?? 0) + a.duration

  timeCache.set(key, {
    avgTime: totalTime / numCalls,
    numCalls,
    totalTime,
  })
}

export function Measure() {
  return (target: any, propertyKey: string, descriptor: PropertyDescriptor) => {
    if (!enable) {
      return descriptor
    }
    const originalMethod = descriptor.value

    const idText = `${target.constructor.name}.${propertyKey}`

    const isAsync = originalMethod.constructor.name === 'AsyncFunction'

    if (isAsync) {
      descriptor.value = async function (...args: any[]) {
        const time = performance.now()
        const result = await originalMethod.apply(this, args)
        const timeEnd = performance.now()
        const total = timeEnd - time

        const cache = timeCache.get(idText)
        const numCalls = (cache?.numCalls ?? 0) + 1
        const totalTime = (cache?.totalTime ?? 0) + total

        timeCache.set(idText, {
          avgTime: totalTime / numCalls,
          numCalls,
          totalTime,
        })
        return result
      }
    } else {
      descriptor.value = function (...args: any[]) {
        const time = performance.now()
        const result = originalMethod.apply(this, args)
        const timeEnd = performance.now()
        const total = timeEnd - time

        const cache = timeCache.get(idText)
        const numCalls = (cache?.numCalls ?? 0) + 1
        const totalTime = (cache?.totalTime ?? 0) + total

        timeCache.set(idText, {
          avgTime: totalTime / numCalls,
          numCalls,
          totalTime,
        })
        return result
      }
    }

    if (!interval) {
      interval = setInterval(() => {
        try {
          console.log('MYKO_MEASURE', process.env['MYKO_MEASURE'])
          if (!process.env['MYKO_MEASURE'] === true) {
            return
          }

          console.log(
            [...timeCache.entries()]
              .sort(([_, val], [__, val2]) => val2.avgTime - val.avgTime)
              .map(
                ([id, val]) =>
                  `${id} - avg: ${val.avgTime} - numCalls: ${val.numCalls} - total: ${val.totalTime}`,
              )
              .slice(0, 10),
          )
        } catch (e) {
          console.warn(e)
        }
      }, 5 * 1000)
    }

    return descriptor
  }
}
