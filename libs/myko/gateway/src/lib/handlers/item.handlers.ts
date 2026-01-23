import {
  ChildEntities,
  ChildEntitiesAllTime,
  EntitySearch,
  EntitySnapshotDifference,
  eventBus,
  FullChildEntities,
  getHistoryProvider,
  getHostId,
  GetItemsByTypeAndIds,
  ImportItems,
  liveRepoName,
  makeSet,
  MykoCommandHandler,
  MykoReportHandler,
  relationRegistry,
  repoName,
  reportBus,
  unwrapItem,
  wrapItem,
  type EntitySnapshotDifferenceData,
  type MCommandHandler,
  type MItem,
  type MItemStub,
  type MLiveReportResult,
  type MReportHandler,
  type MWrappedItem,
} from '@myko/core'
import { uniqBy } from 'ramda'
import {
  combineLatest,
  debounceTime,
  distinctUntilChanged,
  from,
  map,
  of,
  switchMap,
  type Observable,
} from 'rxjs'

// Debounce time for search queries - not in hot path
const SEARCH_DEBOUNCE_MS = 100

@MykoReportHandler(GetItemsByTypeAndIds)
export class GetItemByTypeAndIdHandler
  implements MReportHandler<GetItemsByTypeAndIds>
{
  execute(query: GetItemsByTypeAndIds) {
    return repoName(query.type, query)
      .watchIds(query.ids)
      .pipe(map((x) => x.map((item) => wrapItem(item))))
  }
}

@MykoReportHandler(EntitySearch)
export class EntitySearchHandler implements MReportHandler<EntitySearch<any>> {
  execute(report: EntitySearch<any>): Observable<any> {
    return repoName(report.entityType, report)
      .watchSearch(
        report.query,
        {
          showAllOnEmpty: report.opts?.showAllOnEmpty,
        },
        {
          query: report.filter,
        },
      )
      .pipe(debounceTime(SEARCH_DEBOUNCE_MS))
  }
}

@MykoReportHandler(FullChildEntities)
export class FullChildEntitiesHandler
  implements MReportHandler<FullChildEntities>
{
  execute(report: FullChildEntities): Observable<MItemStub[]> {
    const myChildren = reportBus.watch(
      new ChildEntities(report.parentType, report.parentId).withContext(report),
    )

    return myChildren.pipe(
      switchMap((children) =>
        children.length === 0
          ? of([] as MItemStub[])
          : combineLatest(
              children.map((child) =>
                reportBus
                  .watch(
                    new FullChildEntities(child.itemType, child.id).withContext(
                      report,
                    ),
                  )
                  .pipe(map((x) => [...x, child])),
              ),
            ).pipe(map((x) => x.flat())),
      ),
    )
  }
}

@MykoReportHandler(ChildEntities)
export class ChildEntitiesHandler implements MReportHandler<ChildEntities> {
  execute(report: ChildEntities): MLiveReportResult<ChildEntities> {
    const item = repoName(report.parentType, report).watchId(report.parentId)

    const relValues = [...relationRegistry.values()]

    const children = item.pipe(
      switchMap((item) => {
        if (!item) {
          return of([] as MWrappedItem[])
        }

        const ownsMany = relValues.map((x) =>
          x.type === 'owns-many' && x.localType === report.parentType
            ? repoName(x.foreignType, report).watch({
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

          return repoName(x.localType, report).watch({
            [dep.localKey]: item[dep.foreignKey],
          })
        })

        const belongsToThis = relValues.map((x) =>
          x.type === 'belongs-to' && x.foreignType === report.parentType
            ? repoName(x.localType, report).watch({
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

    return children.pipe(
      map((x) =>
        x.map(
          (y) =>
            ({
              id: y.item.id,
              itemType: y.itemType,
              name: 'name' in y.item ? (y.item.name as string) : undefined,
              hash: y.item.hash,
            }) satisfies MItemStub,
        ),
      ),
      distinctUntilChanged((prev, current) => {
        const p = new Set(prev.map((p) => `${p.id}-${p.itemType}-${p.name}`))
        const c = new Set(current.map((p) => `${p.id}-${p.itemType}-${p.name}`))

        const diff = p.symmetricDifference(c)

        if (diff.size > 0) {
          console.log('sym diff', diff, report.parentId, report.parentType)
        }

        return diff.size === 0
      }),
    )
  }
}

@MykoReportHandler(ChildEntitiesAllTime)
export class ChildEntitiesAllTimeHandler
  implements MReportHandler<ChildEntitiesAllTime>
{
  execute(
    report: ChildEntitiesAllTime,
  ): MLiveReportResult<ChildEntitiesAllTime> {
    const item = liveRepoName(report.parentType).watchId(report.parentId)

    const relValues = [...relationRegistry.values()]

    const children = item.pipe(
      switchMap((item) => {
        if (!item) {
          return of([] as MWrappedItem[])
        }

        const ownsMany = Promise.all(
          relValues.map((rel) => {
            return rel.type === 'owns-many' &&
              rel.localType === report.parentType
              ? getHistoryProvider()
                  .getAllEntitiesNoDeletes(rel.foreignType)
                  .then((items) =>
                    items.filter(
                      (y) => y[rel.foreignKey] === item[rel.localKey],
                    ),
                  )
              : []
          }),
        ).then((x) => x.flat())

        const ensuredFor = Promise.all(
          relValues.map((rel) => {
            if (
              rel.type !== 'ensure-for' ||
              !rel.dependencies.some(
                (dep) => dep.foreignType === report.parentType,
              )
            ) {
              return [] as MItem[]
            }

            const dep = rel.dependencies.find(
              (dep) => dep.foreignType === report.parentType,
            )!

            return getHistoryProvider()
              .getAllEntitiesNoDeletes(rel.localType)
              .then((items) =>
                items.filter((y) => y[dep.localKey] === item[dep.foreignKey]),
              )
          }),
        ).then((x) => x.flat())

        const belongsToThis = Promise.all(
          relValues.map((rel) => {
            return rel.type === 'belongs-to' &&
              rel.foreignType === report.parentType
              ? getHistoryProvider()
                  .getAllEntitiesNoDeletes(rel.localType)
                  .then((items) =>
                    items.filter(
                      (y) => y[rel.localKey] === item[rel.foreignKey],
                    ),
                  )
              : []
          }),
        ).then((x) => x.flat())

        const items = Promise.all([ownsMany, ensuredFor, belongsToThis])
          .then((x) => x.flat())
          .then(uniqBy((x) => x.id))
          .then((x) => x.map(wrapItem))

        return from(items)
      }),
    )

    return children.pipe(
      map((x) =>
        x.map(
          (y) =>
            ({
              id: y.item.id,
              itemType: y.itemType,
              name: 'name' in y.item ? (y.item.name as string) : undefined,
              hash: y.item.hash,
            }) satisfies MItemStub,
        ),
      ),
      distinctUntilChanged((prev, current) => {
        const p = new Set(prev.map((p) => `${p.id}-${p.itemType}-${p.name}`))
        const c = new Set(current.map((p) => `${p.id}-${p.itemType}-${p.name}`))

        const diff = p.symmetricDifference(c)

        if (diff.size > 0) {
          console.log('sym diff', diff, report.parentId, report.parentType)
        }

        return diff.size === 0
      }),
    )
  }
}

@MykoReportHandler(EntitySnapshotDifference)
export class EntitySnapshotDifferenceHandler
  implements MReportHandler<EntitySnapshotDifference>
{
  execute(
    report: EntitySnapshotDifference,
  ): Observable<EntitySnapshotDifferenceData> {
    const pinned = reportBus.watch(
      new ChildEntities(report.parentType, report.parentId).withContext(report),
    )

    const upToDate = reportBus.watch(
      new ChildEntities(report.parentType, report.parentId).withContext({
        commandClientId: getHostId(),
        tx: report.tx,
      }),
    )

    return combineLatest([pinned, upToDate]).pipe(
      map(([pinned, upToDate]) => {
        // Build O(1) lookup maps to avoid O(n²) find() calls
        const pinnedMap = new Map(pinned.map((x) => [x.id, x]))
        const upToDateMap = new Map(upToDate.map((x) => [x.id, x]))

        const pinnedIds = new Set(pinnedMap.keys())
        const upToDateIds = new Set(upToDateMap.keys())

        const addedIds = pinnedIds.difference(upToDateIds)
        const removedIds = upToDateIds.difference(pinnedIds)

        const potentiallyChanged = pinnedIds.intersection(upToDateIds)

        const changed = [...potentiallyChanged].filter((id) => {
          return pinnedMap.get(id)!.hash !== upToDateMap.get(id)!.hash
        })

        return {
          added: [...addedIds].map((id) => upToDateMap.get(id)!),
          removed: [...removedIds].map((id) => pinnedMap.get(id)!),
          changed: changed.map((id) => pinnedMap.get(id)!),
        } satisfies EntitySnapshotDifferenceData
      }),
    )
  }
}

@MykoCommandHandler(ImportItems)
export class ImportItemsHandler implements MCommandHandler<ImportItems> {
  async execute(command: ImportItems) {
    const events = command.items
      .map((item) => unwrapItem(item))
      .map((item) =>
        makeSet(item, command.tx, { preventRelationshipUpdates: true }),
      )

    eventBus.publishAll(events)
  }
}
