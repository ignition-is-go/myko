import { Client, repo, type ID } from '@myko/core'
import { Subject, filter, map } from 'rxjs'

export const unsub = new Subject<ID>()
export const clientDisconnect = (clientId: ID) =>
  repo(Client)
    .watchId(clientId)
    .pipe(
      map((x) => !!x),
      filter((x) => !x),
    )
