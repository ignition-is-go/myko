import {
  ClientRepo,
  EventContainer,
  GetClientsByIds,
  GetEventLog,
  GetItemsByTypeAndIds,
  MItem,
  MQueryHandler,
  MykoQueryHandler,
  getEvents,
} from '@myko/core'
import { Observable, combineLatest, debounceTime, map, startWith } from 'rxjs'
import { watchIds } from '@myko/core/src/lib/registry'

@MykoQueryHandler(GetEventLog)
export class GetEventLogHandler implements MQueryHandler<GetEventLog> {
  execute(query: GetEventLog): Observable<EventContainer[]> {
    const time = query.time

    const all = [...getEvents.values()]

    return combineLatest(
      all.map((fn) => fn(time).pipe(startWith([] as EventContainer[]))),
    ).pipe(
      map((x) =>
        x
          .flat()
          .filter((x) => x.event.tx !== undefined)
          .sort((a, b) => a.id.localeCompare(b.id)),
      ),
      debounceTime(50),
    )
  }
}

@MykoQueryHandler(GetItemsByTypeAndIds)
export class GetItemByTypeAndIdHandler
  implements MQueryHandler<GetItemsByTypeAndIds>
{
  constructor() {}
  execute(query: GetItemsByTypeAndIds): Observable<MItem[]> {
    const watchTypeByIds = watchIds.get(query.type)
    return watchTypeByIds(query.ids)
  }
}
