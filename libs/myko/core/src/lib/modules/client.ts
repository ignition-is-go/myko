import { MykoCommand, MykoItem, MykoQuery, MykoReport, belongsTo } from '../decorators'
import { MCommand, MReport, type ID } from '../types'
import { MItem } from '../types/item'
import { MQuery } from '../types/query'
import type { MWrappedCommand } from '../wrappers'
import { Server } from './server'

@MykoItem({
  doc: 'A Myko Client connected to a Server',
})
export class Client extends MItem<Client> {
  @belongsTo(Server)
  readonly serverId!: string

  readonly windback?: string
}

@MykoQuery(Client)
export class GetClientsByIds extends MQuery<Client> {
  constructor(public ids: string[]) {
    super()
  }
}

@MykoQuery(Client)
export class GetClientsByQuery extends MQuery<Client> {
  constructor(public partial: Partial<Client>) {
    super()
  }
}

@MykoCommand()
export class DeleteClientsByServerId extends MCommand {
  constructor(public serverId: string) {
    super()
  }
}

@MykoCommand()
export class ClientCommand extends MCommand<unknown> {
  constructor(
    readonly command: MWrappedCommand,
    readonly client: Client,
  ) {
    super()
  }
}

@MykoReport()
export class ClientStatus extends MReport<{ online: boolean }> {
  constructor(readonly clientId: ID) {
    super()
  }
}

@MykoCommand({
  allowDuringWindback: true,
})
export class SetClientWindbackTime extends MCommand<boolean> {
  constructor(readonly windback: string) {
    super()
  }
}

@MykoCommand({
  allowDuringWindback: true,
})
export class ClearClientWindbackTime extends MCommand {}

@MykoReport()
export class WindbackStatus extends MReport<string | undefined> {}
