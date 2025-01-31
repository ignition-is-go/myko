import {
  EntitySearch,
  GetItemsByTypeAndIds,
  MItem,
  MykoQueryHandler,
  MykoReportHandler,
  repoName,
  type MQueryHandler,
  type MReportHandler,
} from '@myko/core'
import type { Observable } from 'rxjs'

@MykoQueryHandler(GetItemsByTypeAndIds)
export class GetItemByTypeAndIdHandler
  implements MQueryHandler<GetItemsByTypeAndIds>
{
  constructor() {}
  execute(query: GetItemsByTypeAndIds): Observable<MItem[]> {
    return repoName(query.type).watchIds(query.ids)
  }
}

@MykoReportHandler(EntitySearch)
export class EntitySearchHandler implements MReportHandler<EntitySearch<any>> {
  execute(report: EntitySearch<any>): Observable<any> {
    return repoName(report.entityType).watchSearch(
      report.query,
      {
        showAllOnEmpty: report.opts?.showAllOnEmpty,
      },
      {
        query: report.filter,
      },
    )
  }
}
