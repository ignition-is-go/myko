import { getHostId, type ID } from '@myko/core'
import * as dns from 'dns/promises'

export const dockerAddress = async (): Promise<string> => {
  const SERVICE_NAME = process.env['MYKO_SERVICE_NAME']
  const MYKO_PORT = process.env['MYKO_PORT']

  if (!MYKO_PORT) {
    throw new Error('MYKO_PORT not specified')
  }

  if (!SERVICE_NAME) {
    throw new Error('SERVICE_NAME Not Specified')
  }

  const records = await dns.resolve4(`tasks.${SERVICE_NAME}`)

  while (true) {
    const tasks = records.map((record) =>
      getServerIp(record, Number(MYKO_PORT), getHostId()),
    )

    const results = await Promise.allSettled(tasks)

    const ip = results
      .filter((r) => r.status === 'fulfilled')
      .map((r) => r.value)
      .find((f) => f !== undefined)

    if (ip) {
      return ip
    }

    await new Promise((res) => {
      setTimeout(res, 5000)
    })
  }
}

const getServerIp = async (
  record: string,
  port: number,
  searchId: ID,
): Promise<string | undefined> => {
  const url = `http://${record}:${port}/server`

  const serverId = await fetch(url)
    .catch((_) => {
      console.log("Couldn't fetch", url)
    })
    .then((res) => res?.text())
    .catch((_) => {
      console.log("Couldn't get text", url)
      return undefined
    })

  if (!serverId) {
    return undefined
  }

  if (serverId === searchId) {
    console.log('Connected to Self at', record)
    return record
  }
}
