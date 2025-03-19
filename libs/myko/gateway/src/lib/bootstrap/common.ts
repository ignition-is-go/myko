import { Client, getHostId, repo, type ID } from '@myko/core'
import { Subject, filter, map } from 'rxjs'
import { v4 as uuid } from 'uuid'

export const unsub = new Subject<ID>()
export const clientDisconnect = (clientId: ID) =>
  repo(Client, { commandClientId: getHostId(), tx: uuid() })
    .watchId(clientId)
    .pipe(
      map((x) => !!x),
      filter((x) => !x),
    )
