import {
  queryBus,
  unwrapQuery,
  wrapItem,
  type ID,
  type MWrappedQuery,
} from '@myko/core'
import {
  MQUERY_ERROR_EVENT,
  MQUERY_RESPONSE_EVENT,
  type WSMMessage,
  type WSMQueryError,
  type WSMQueryResponse,
} from '@myko/ws'
import {
  catchError,
  filter,
  map,
  of,
  takeUntil,
  type Observable,
  type Subject,
} from 'rxjs'
import { clientDisconnect, unsub } from './common'

export const handleQuery = (
  clientId: ID,
  query: MWrappedQuery,
  respond: Subject<{ clientId: ID; data: WSMMessage }>,
) => {
  const q = unwrapQuery(query)

  const tx = q.tx

  const asSent = new Map<ID, string>()

  let sequence = -1

  const response = queryBus.watch(q).pipe(
    map((x) => x.filter((x) => !!x)),
    map((curr) => {
      const currMap = new Map(curr.map((x) => [x.id, x]))

      const upserts = curr.filter(
        (x) =>
          x.hash == null ||
          x.hash === undefined ||
          !asSent.has(x.id) ||
          asSent.get(x.id) !== x.hash,
      )
      const deletes = Array.from(asSent.keys()).filter((x) => !currMap.has(x))

      upserts.forEach((x) => asSent.set(x.id, x.hash))

      deletes.forEach((x) => asSent.delete(x))

      sequence = sequence + 1

      return {
        data: {
          deletes: [...deletes],
          sequence: sequence,
          upserts: upserts.map((x) => wrapItem(x)),
          tx,
        },
        event: MQUERY_RESPONSE_EVENT,
      } satisfies WSMQueryResponse
    }),

    filter(
      (x) =>
        x.data.deletes.length > 0 ||
        x.data.upserts.length > 0 ||
        sequence === 0,
    ),
    catchError((e) => {
      console.error(e.message)
      return of({
        data: {
          message: e.message,
          tx: tx,
        },
        event: MQUERY_ERROR_EVENT,
      } as WSMQueryError)
    }),
    takeUntil(clientDisconnect(clientId)),
    takeUntil(unsub.pipe(filter((u) => u === q.tx))),
  ) as Observable<WSMQueryResponse>

  response.subscribe((x) => {
    respond.next({
      clientId: clientId,
      data: x,
    })
  })
}
