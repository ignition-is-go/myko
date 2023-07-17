import {
  MykoQueryHandler,
  GetServers,
  MQueryHandler,
  ServerRepo,
  MLiveQueryResult,
  GetConnectedServer,
  GetServersByQuery,
  Server,
} from '@myko/core'
import { Inject, Injectable } from '@nestjs/common'
import { ConfigService } from '@nestjs/config'
import { map, tap } from 'rxjs'
import { SERVER_TOKEN } from '../types'

@MykoQueryHandler(GetServers)
export class GetServersHandler implements MQueryHandler<GetServers> {
  constructor(private repo: ServerRepo) {}

  execute(query: GetServers): MLiveQueryResult<GetServers> {
    return this.repo.watch({})
  }
}

@MykoQueryHandler(GetConnectedServer)
export class GetConnectedServerHandler
  implements MQueryHandler<GetConnectedServer>
{
  constructor(
    private repo: ServerRepo,
    private config: ConfigService,
    @Inject(SERVER_TOKEN) private server: Server,
  ) {}

  execute(query: GetConnectedServer): MLiveQueryResult<GetConnectedServer> {
    return this.repo.watchId(this.server.id).pipe(map((x) => [x]))
  }
}

@MykoQueryHandler(GetServersByQuery)
export class GetServersByQueryHandler
  implements MQueryHandler<GetServersByQuery>
{
  constructor(private repo: ServerRepo) {}

  execute(query: GetServersByQuery): MLiveQueryResult<GetServersByQuery> {
    return this.repo.watch(query.query)
  }
}

@Injectable()
export class ServerSagas {
  constructor(private repo: ServerRepo) {}
}
