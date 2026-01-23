import {
  autoballanceRegistry,
  commandBus,
  constructorRegistry,
  eventBus,
  getHostId,
  liveRepoName,
  MEventType,
  MykoCommandHandler,
  MykoLogger,
  MykoSaga,
  onAllInit,
  ReballanceItem,
  type MCommandHandler,
  type MEventStream,
  type MSagaHandler,
} from '@myko/core'
import { from, merge, mergeMap, of, switchMap } from 'rxjs'
import { v4 as uuid } from 'uuid'

@MykoSaga()
export class AutoBallanceSaga implements MSagaHandler {
  execute(stream: MEventStream<MItem>) {
    return stream.pipe(
      mergeMap((event) => {
        const localReballance = autoballanceRegistry.get(event.itemType)

        if (localReballance && event.changeType === MEventType.SET) {
          // Use withContext to set the context from the event
          return of(
            new ReballanceItem(event.itemType, event.item.id).withContext({
              commandClientId: getHostId(),
              tx: event.tx,
            }),
          )
        }

        const foreignReballance = Array.from(autoballanceRegistry.entries())
          .filter(([_, info]) => {
            return info.foreignType === event.itemType
          })
          .map(([local, info]) => ({ local, ...info }))

        return merge(
          ...foreignReballance.map((info) => {
            return from(
              liveRepoName(info.local).get({ [info.propName]: event.item.id }),
            ).pipe(
              switchMap((items) => {
                return of(
                  ...items.map((item) =>
                    new ReballanceItem(info.local, item.id).withContext({
                      commandClientId: getHostId(),
                      tx: event.tx,
                    }),
                  ),
                )
              }),
            )
          }),
        )
      }),
    )
  }
}

@MykoCommandHandler(ReballanceItem)
export class RebalanceItemHandler implements MCommandHandler<ReballanceItem> {
  private logger = new MykoLogger(RebalanceItemHandler.name)
  async execute(command: ReballanceItem): Promise<void> {
    const localItems = liveRepoName(command.entityType)

    if (!localItems) {
      throw new Error(`Cannot find repo for ${command.entityType}`)
    }

    const reballanceInfo = autoballanceRegistry.get(command.entityType)

    if (!reballanceInfo) {
      throw new Error(`Cannot find reballance info for ${command.entityType}`)
    }

    const foreignItems = liveRepoName(reballanceInfo.foreignType)

    if (!foreignItems) {
      throw new Error(`Cannot find repo for ${reballanceInfo.foreignType}`)
    }

    const localItem = await localItems.getId(command.id)

    if (!localItem) {
      throw new Error(`Cannot find item ${command.id}`)
    }

    const foreigns = await foreignItems.get({})

    if (!foreigns) {
      throw new Error(`Cannot find any foreign items`)
    }

    if (foreigns.length === 0) {
      this.logger.warn(
        `No ${reballanceInfo.foreignType}s found durring reballance`,
      )
      return
    }

    const ranked = rankForeigns(localItem, foreigns)

    const newOwner = ranked[0]

    if (newOwner.id === localItem[reballanceInfo.propName]) {
      return
    }

    const localCtor = constructorRegistry.get(command.entityType)

    if (!localCtor) {
      throw new Error(`Cannot find constructor for ${command.entityType}`)
    }

    const newLocal = new localCtor({
      ...localItem,
      [reballanceInfo.propName]: newOwner.id,
    })

    eventBus.publishSet(newLocal, command.tx)
  }
}

const rankForeigns = (local: MItem, foreigns: MItem[]): MItem[] => {
  foreigns.sort((a, b) => {
    const localHash = keyspace(local.id)

    const aHash = keyspace(a.id)

    const bHash = keyspace(b.id)

    const aDist = Math.abs(localHash - aHash)
    const bDist = Math.abs(localHash - bHash)

    return aDist - bDist
  })

  return foreigns
}

const keyspace = (key: string): number => {
  let hash = 0
  for (let i = 0; i < key.length; i++) {
    const char = key.charCodeAt(i)
    hash = (hash << 5) - hash + char
    hash |= 0 // Convert to 32bit integer
  }
  return hash >>> 0 // Ensure non-negative
}

onAllInit(async () => {
  for (const [key, value] of autoballanceRegistry.entries()) {
    console.log('Reballance', key, value)

    const localItems = liveRepoName(key)

    if (!localItems) {
      throw new Error(`Cannot find repo for ${key}`)
    }

    const allLocals = await localItems.get({})

    if (!allLocals) {
      throw new Error(`Cannot find any local items`)
    }

    // Generate a transaction ID for this initialization
    const initTx = uuid()
    for (const local of allLocals) {
      const context = {
        commandClientId: getHostId(),
        tx: initTx,
      }
      commandBus.execute(new ReballanceItem(key, local.id).withContext(context))
    }
  }
})
