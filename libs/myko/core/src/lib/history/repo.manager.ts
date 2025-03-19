import { firstValueFrom, switchMap, type Observable } from 'rxjs'
import type { IRepo } from '../aggregates/repo.abstract'
import { getHistoryProvider } from '../registry/history.registry'
import type { DeepPartial, IContext, ID, IMItem, MItem } from '../types'
import { HistoryRepo } from './history.repo'

export const wrapWithHistory = <T extends MItem>(
  entity: string,
  fallback: IRepo<T>,
  context: IContext,
  getClient: (clientId: ID) => Observable<{ windback?: string }>,
) => {
  const historyRepo = new HistoryRepo<T>(entity)

  return new RepoWithHistory<T>(
    context,
    historyRepo as any,
    fallback,
    getClient,
  )
}

export class RepoWithHistory<T extends IMItem> implements IRepo<T> {
  constructor(
    private ctx: IContext,
    private historyRepo: HistoryRepo<T>,
    private baseRepo: IRepo<T>,
    private getClient: (
      clientId: ID,
    ) => Observable<{ windback?: string } | undefined>,
  ) {}

  watchId(id: ID): Observable<T | null> {
    return this.getClient(this.ctx.commandClientId).pipe(
      switchMap((client) => {
        if (client?.windback) {
          return this.historyRepo
            .withContext(client.windback, getHistoryProvider())
            .watchId(id)
        } else {
          return this.baseRepo.watchId(id)
        }
      }),
    )
  }

  watchIds(ids: ID[]): Observable<T[]> {
    return this.getClient(this.ctx.commandClientId).pipe(
      switchMap((client) => {
        if (client?.windback) {
          return this.historyRepo
            .withContext(client.windback, getHistoryProvider())
            .watchIds(ids)
        } else {
          return this.baseRepo.watchIds(ids)
        }
      }),
    )
  }

  watchFilter(filterFunc: (ent: T) => boolean): Observable<T[]> {
    return this.getClient(this.ctx.commandClientId).pipe(
      switchMap((client) => {
        if (client?.windback) {
          return this.historyRepo
            .withContext(client.windback, getHistoryProvider())
            .watchFilter(filterFunc)
        } else {
          return this.baseRepo.watchFilter(filterFunc)
        }
      }),
    )
  }

  watch(query: DeepPartial<T>): Observable<T[]> {
    return this.getClient(this.ctx.commandClientId).pipe(
      switchMap((client) => {
        if (client?.windback) {
          return this.historyRepo
            .withContext(client.windback, getHistoryProvider())
            .watch(query)
        } else {
          return this.baseRepo.watch(query)
        }
      }),
    )
  }

  getSearch(query: string): Promise<T[]> {
    return firstValueFrom(this.getClient(this.ctx.commandClientId)).then(
      (client) => {
        if (client?.windback) {
          return this.historyRepo
            .withContext(client.windback, getHistoryProvider())
            .getSearch(query)
        } else {
          return this.baseRepo.getSearch(query)
        }
      },
    )
  }

  watchSearch(
    query: string,
    opts?: { showAllOnEmpty?: boolean },
    filters?: {
      func?: ((ent: T) => boolean) | undefined
      query?: DeepPartial<T> | undefined
    },
  ): Observable<T[]> {
    return this.getClient(this.ctx.commandClientId).pipe(
      switchMap((client) => {
        if (client?.windback) {
          return this.historyRepo
            .withContext(client.windback, getHistoryProvider())
            .watchSearch(query, opts, filters)
        } else {
          return this.baseRepo.watchSearch(query, opts, filters)
        }
      }),
    )

    throw new Error('Method not implemented.')
  }
  getId(id: ID): Promise<T | null> {
    return firstValueFrom(this.getClient(this.ctx.commandClientId)).then(
      (client) => {
        if (client?.windback) {
          return this.historyRepo
            .withContext(client.windback, getHistoryProvider())
            .getId(id)
        } else {
          return this.baseRepo.getId(id)
        }
      },
    )
  }

  getIds(ids: ID[]): Promise<T[]> {
    return firstValueFrom(this.getClient(this.ctx.commandClientId)).then(
      (client) => {
        if (client?.windback) {
          return this.historyRepo
            .withContext(client.windback, getHistoryProvider())
            .getIds(ids)
        } else {
          return this.baseRepo.getIds(ids)
        }
      },
    )
  }

  getIndex(index: keyof T, value: any): Promise<T[]> {
    return firstValueFrom(this.getClient(this.ctx.commandClientId)).then(
      (client) => {
        if (client?.windback) {
          return this.historyRepo
            .withContext(client.windback, getHistoryProvider())
            .getIndex(index, value)
        } else {
          return this.baseRepo.getIndex(index, value)
        }
      },
    )
  }

  get(query: DeepPartial<T>): Promise<T[]> {
    return firstValueFrom(this.getClient(this.ctx.commandClientId)).then(
      (client) => {
        if (client?.windback) {
          return this.historyRepo
            .withContext(client.windback, getHistoryProvider())
            .get(query)
        } else {
          return this.baseRepo.get(query)
        }
      },
    )
  }

  getFilter(filterFunc: (ent: T) => boolean): Promise<T[]> {
    return firstValueFrom(this.getClient(this.ctx.commandClientId)).then(
      (client) => {
        if (client?.windback) {
          return this.historyRepo
            .withContext(client.windback, getHistoryProvider())
            .getFilter(filterFunc)
        } else {
          return this.baseRepo.getFilter(filterFunc)
        }
      },
    )
  }
}
