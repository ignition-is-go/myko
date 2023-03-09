import {
  filter,
  finalize,
  firstValueFrom,
  map,
  Observable,
  of,
  shareReplay,
  Subject,
  tap,
} from 'rxjs'
import {
  MCommand,
  MQuery,
  MEvent,
  unwrapCommand,
  unwrapQuery,
  unwrapItem,
  MLiveQueryResult,
  ID,
  CommandResponse,
} from '@myko/core'
import {
  wrapCommandWS,
  wrapEventWS,
  wrapQueryCancel,
  wrapQueryWS,
} from './wrappers'
import {
  WSMMessage,
  MCOMMAND_EVENT,
  MQUERY_EVENT,
  MEVENT_EVENT,
  MQUERY_RESPONSE_EVENT,
  WSMQueryResponse,
  WSMCommandResponse,
  MCOMMAND_RESPONSE_EVENT,
  MCOMMAND_ERROR_EVENT,
  WSMCommandError,
} from './types'

export class WSMClient {
  private q = new Set<WSMMessage>()
  private ws: WebSocket

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
  private userToken: ID

  constructor(
    private host: string,
    private port: number,
    private clientId: string,
    private argReconnect: () => void,
    private makeSocket: (url: string) => any,
  ) {
    this.commandSubject = new Subject()
    this.querySubject = new Subject()
    this.eventSubject = new Subject()
    this.queryResponses = new Subject()
    this.commandResponses = new Subject()
    this.errorsSubject = new Subject()
    this.successSubject = new Subject()
    this.connect()
  }

  sendCommand<T extends MCommand<unknown>>(
    command: T,
  ): Promise<CommandResponse<T>> {
    if (this.userToken) {
      command.userToken = this.userToken
    }
    const wrapped = wrapCommandWS(command)
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
    const wrappedQuery = wrapQueryWS(query)
    this.send(wrappedQuery)
    return this.queryResponses.pipe(
      filter((r) => r.tx === query.tx),
      map((r) => r.data.map((rr) => unwrapItem(rr))),

      shareReplay(1),
      finalize(() => {
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
    this.ws = this.makeSocket(`ws://${this.host}:${this.port}`)
    if (!this.ws) {
      return
    }
    this.ws.onopen = () => {
      console.log('Connected')
      this
      this.onReconnect()
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
      console.log(
        'Socket Closed: Reconnecting',
        `ws://${this.host}:${this.port}`,
      )
      setTimeout(() => {
        this.connect()
      }, 1000)
    }

    this.ws.onerror = (err) => {
      console.error('MykoWebSocketError', JSON.stringify(err))
    }
  }

  onReconnect() {
    ;[...this.q.values()].forEach((v) => {
      this.q.delete(v)
      this.send(v)
    })
  }

  private send(item: WSMMessage) {
    if (!this.ws || this.ws.readyState !== this.ws.OPEN) {
      this.q.add(item)
      return
    }

    this.ws.send(JSON.stringify(item))
  }
}
