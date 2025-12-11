import { Client, getHostId, repo, type ID } from '@myko/core'
import { Subject, filter, map, shareReplay } from 'rxjs'
import { v4 as uuid } from 'uuid'

export const unsub = new Subject<ID>()

// Cache disconnect observables per client to prevent subscription leak
const disconnectCache = new Map<ID, ReturnType<typeof createClientDisconnect>>()

const createClientDisconnect = (clientId: ID) =>
  repo(Client, { commandClientId: getHostId(), tx: uuid() })
    .watchId(clientId)
    .pipe(
      map((x) => !!x),
      filter((x) => !x),
      shareReplay(1), // Share single subscription across all queries/reports for this client
    )

export const clientDisconnect = (clientId: ID) => {
  if (!disconnectCache.has(clientId)) {
    const disconnect$ = createClientDisconnect(clientId)

    // Clean up cache when client disconnects
    disconnect$.subscribe(() => {
      disconnectCache.delete(clientId)
    })

    disconnectCache.set(clientId, disconnect$)
  }

  return disconnectCache.get(clientId)!
}
