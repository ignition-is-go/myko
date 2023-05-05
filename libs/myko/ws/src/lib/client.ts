import {
  filter,
  finalize,
  firstValueFrom,
  map,
  Observable,
  shareReplay,
  Subject,
  tap,
} from 'rxjs'
import {
  MCommand,
  MQuery,
  unwrapCommand,
  unwrapQuery,
  unwrapItem,
  type MEvent,
  type MLiveQueryResult,
  type ID,
  type CommandResponse,
} from '@myko/core'
import {
  wrapCommandWS,
  wrapEventWS,
  wrapQueryCancel,
  wrapQueryWS,
} from './wrappers'
import {
  type WSMMessage,
  MCOMMAND_EVENT,
  MQUERY_EVENT,
  MEVENT_EVENT,
  MQUERY_RESPONSE_EVENT,
  type WSMQueryResponse,
  type WSMCommandResponse,
  MCOMMAND_RESPONSE_EVENT,
  MCOMMAND_ERROR_EVENT,
  type WSMCommandError,
  WSMQuery,
} from './types'

export class WSMClient {
  private q = new Set<WSMMessage>()
  private ws: WebSocket | null = null

  get commands(): Observable<MCommand> {
    return this.commandSubject.pipe()
  }

  get queries(): Observable<MQuery> {
    return this.querySubject.pipe()
  }

  get events(): Observable<MEvent> {
    return this.eventSubject.pipe()
  }

  get errors() {
    return this.errorsSubject.pipe()
  }

  get successes() {
    return this.successSubject.pipe()
  }

  private commandSubject: Subject<MCommand>
  private querySubject: Subject<MQuery>
  private eventSubject: Subject<MEvent>
  private queryResponses: Subject<WSMQueryResponse>
  private commandResponses: Subject<WSMCommandResponse | WSMCommandError>
  private errorsSubject: Subject<WSMCommandError>
  private successSubject: Subject<string>
  private userToken: ID | null = null
  private resendQueries: Map<ID, WSMQuery>

  constructor(
    private host: string,
    private port: number,
    private clientId: string,
    private makeSocket: (url: string) => any,
    private hooks?: {
      onStartConnect?: (url: string) => void
      onConnect?: (url: string) => void
      onDisconnect?: (c: CloseEvent) => void
      onError?: (e?: string) => void
    },
    private secure: boolean = false,
  ) {
    this.commandSubject = new Subject()
    this.querySubject = new Subject()
    this.eventSubject = new Subject()
    this.queryResponses = new Subject()
    this.commandResponses = new Subject()
    this.errorsSubject = new Subject()
    this.successSubject = new Subject()
    this.resendQueries = new Map()
    this.connect()
  }

  sendCommand<T extends MCommand<unknown>>(
    command: T,
  ): Promise<CommandResponse<T>> {
    if (this.userToken) {
      command.userToken = this.userToken
    }
    const wrapped = wrapCommandWS(command, this.clientId)
    this.send(wrapped)
    return firstValueFrom(
      this.commandResponses.pipe(
        filter((c) => c.tx === command.tx),
        map((e) => {
          if (e.event === MCOMMAND_ERROR_EVENT) {
            throw e
          }
          return e.data as CommandResponse<T>
        }),
        tap((x) =>
          this.successSubject.next(
            wrapped.data.commandId.split(':').reverse().join(' '),
          ),
        ),
      ),
    )
  }

  watchQuery<T extends MQuery>(query: T): MLiveQueryResult<T> {
    const wrappedQuery = wrapQueryWS(query, this.clientId)
    this.resendQueries.set(query.tx, wrappedQuery)
    this.send(wrappedQuery)
    return this.queryResponses.pipe(
      filter((r) => r.tx === query.tx),
      map((r) => r.data.map((rr) => unwrapItem(rr))),

      shareReplay(1),
      finalize(() => {
        this.resendQueries.delete(query.tx)
        this.send(wrapQueryCancel(query.tx))
      }),
    ) as MLiveQueryResult<T>
  }

  sendEvent(event: MEvent) {
    this.send(wrapEventWS(event, this.clientId))
  }

  setUser(token: ID) {
    this.userToken = token
  }

  private connect() {
    const protocol = this.secure ? 'wss' : 'ws'
    const port = this.secure ? '' : `:${this.port}`
    const path = `${protocol}://${this.host}${port}/myko`
    this.hooks?.onStartConnect && this.hooks?.onStartConnect(path)
    this.ws = this.makeSocket(path)

    if (!this.ws) {
      this.hooks?.onError && this.hooks?.onError('No Socket')
      return
    }

    this.ws.onopen = () => {
      this.hooks?.onConnect && this.hooks?.onConnect(path)
      ;[...this.q.values()].forEach((v) => {
        this.q.delete(v)
        this.send(v)
      })
      ;[...this.resendQueries.values()].forEach((q) => {
        this.resendQueries.delete(q.data.query.tx)
        this.send(q)
      })
    }

    this.ws.onmessage = (e) => {
      const body = e.data
      const message: WSMMessage = JSON.parse(body.toString())

      switch (message.event) {
        case MCOMMAND_EVENT:
          const cmd = unwrapCommand(message.data)
          this.commandSubject.next(cmd)
          break

        case MQUERY_EVENT:
          const query = unwrapQuery(message.data)
          this.querySubject.next(query)
          break

        case MEVENT_EVENT:
          const evt = message.data
          this.eventSubject.next(evt)
          break
        case MQUERY_RESPONSE_EVENT:
          this.queryResponses.next(message)
          break
        case MCOMMAND_RESPONSE_EVENT:
          this.commandResponses.next(message)
          break

        case MCOMMAND_ERROR_EVENT:
          this.commandResponses.next(message)
          this.errorsSubject.next(message)
          break
        default:
          console.warn('no idea what to do with this', message)
      }
    }

    this.ws.onclose = (e) => {
      this.hooks?.onDisconnect && this.hooks?.onDisconnect(e)
      setTimeout(() => {
        this.connect()
      }, 1000)
    }

    this.ws.onerror = (err) => {
      this.hooks?.onError && this.hooks?.onError()
    }
  }

  private send(item: WSMMessage) {
    if (!this.ws || this.ws.readyState !== this.ws.OPEN) {
      this.q.add(item)
      return
    }

    this.ws.send(JSON.stringify(item))
  }
}
