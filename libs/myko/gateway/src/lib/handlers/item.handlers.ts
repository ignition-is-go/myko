import {
  ChildEntities,
  EntitySearch,
  GetItemsByTypeAndIds,
  MItem,
  MykoQueryHandler,
  MykoReportHandler,
  relationRegistry,
  repoName,
  wrapItem,
  type MLiveReportResult,
  type MQueryHandler,
  type MReportHandler,
  type MWrappedItem,
} from '@myko/core'
import { uniqBy } from 'ramda'
import { combineLatest, map, of, switchMap, type Observable } from 'rxjs'

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

@MykoReportHandler(ChildEntities)
export class ChildEntitiesHandler implements MReportHandler<ChildEntities> {
  execute(report: ChildEntities): MLiveReportResult<ChildEntities> {
    const item = repoName(report.parentType).watchId(report.parentId)

    const relValues = [...relationRegistry.values()]

    const children = item.pipe(
      switchMap((item) => {
        if (!item) {
          return of([] as MWrappedItem[])
        }

        const ownsMany = relValues.map((x) =>
          x.type === 'owns-many' && x.localType === report.parentType
            ? repoName(x.foreignType).watch({
                [x.foreignKey]: item[x.localKey],
              })
            : of([]),
        )

        const ensuredFor = relValues.map((x) => {
          if (
            x.type !== 'ensure-for' ||
            !x.dependencies.some((y) => y.foreignType === report.parentType)
          ) {
            return of([])
          }

          const dep = x.dependencies.find(
            (y) => y.foreignType === report.parentType,
          )!

          return repoName(x.localType).watch({
            [dep.localKey]: item[dep.foreignKey],
          })
        })

        const belongsToThis = relValues.map((x) =>
          x.type === 'belongs-to' && x.foreignType === report.parentType
            ? repoName(x.localType).watch({
                [x.localKey]: item[x.foreignKey],
              })
            : of([]),
        )

        const streams = [...ownsMany, ...ensuredFor, ...belongsToThis]

        if (streams.length === 0) {
          return of([] as MWrappedItem[])
        }

        return combineLatest(streams).pipe(
          map((x) => x.flat()),
          map((x) => uniqBy((y) => y.id, x)),
          map((x) => x.map(wrapItem)),
        )
      }),
    )

    return children
  }
}
