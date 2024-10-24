import {
  filter,
  first,
  firstValueFrom,
  pairwise,
  ReplaySubject,
  startWith,
} from 'rxjs'
import { ReconnectSocket } from './socket.reconnect'
export enum SocketSendMode {
  Single = 'Single',
  Broadcast = 'Broadcast',
}

export type SocketGroupOpts = {
  onConnected: (url: string) => void
  onClosed: () => void
  onError: (error: string) => void
  onLog: (...log: any[]) => void
  onMessage: (data: MessageEvent) => void
  onMainServerChange: (url: string) => void
  onMainSocketReconnecting: (url: string) => void
  socketSendMode: SocketSendMode
  reconnect: boolean
  secure: boolean
}

export class SocketGroup {
  private sockets = new Map<string, ReconnectSocket>()

  private badSockets = new Set<string>()

  private openSockets = new ReplaySubject<string[]>(1)

  private currentSocket: string

  get goodSockets() {
    return Array.from(this.sockets.keys()).filter(
      (key) => !this.badSockets.has(key),
    )
  }

  get ready() {
    return this.goodSockets.length > 0
  }

  constructor(
    private makeSocket: (socketUrl: string) => WebSocket,
    private groupOpts: SocketGroupOpts,
  ) {
    this.openSockets.pipe(startWith([]), pairwise()).subscribe((keys) => {
      if (keys[0].length === 0 && keys[1].length === 1) {
        this.onFirstClientConnected(keys[1][0])
      }

      if (keys[0].length > 0 && keys[1].length === 0) {
        this.groupOpts.onClosed()
        this.currentSocket = undefined
      }
    })
  }

  private onFirstClientConnected(key: string) {
    const mainSocket = this.sockets.get(key)

    if (!mainSocket) {
      this.groupOpts.onError('Cannot Establish Main Socket')
      return
    }

    this.currentSocket = key

    this.groupOpts.onConnected(key)
  }

  private tickSocketKeys() {
    this.openSockets.next(this.goodSockets)

    if (this.goodSockets.length === 0) {
      this.groupOpts.onClosed()
    }
  }

  private onSocketOpen(key: string, socket: ReconnectSocket) {
    const emptyAtStart = this.goodSockets.length === 0

    this.sockets.set(key, socket)
    this.badSockets.delete(key)

    const hasOneNow = this.goodSockets.length === 1

    if (emptyAtStart && hasOneNow) {
      this.currentSocket = key
      this.groupOpts.onMainServerChange(key)
      this.groupOpts.onConnected(key)
    }

    this.tickSocketKeys()
  }

  private onSocketClosed(key: string) {
    this.badSockets.add(key)

    if (this.goodSockets.length === 0) {
      this.groupOpts.onClosed()
      return
    }

    if (key === this.currentSocket) {
      this.currentSocket = this.goodSockets[0]
      this.groupOpts.onMainServerChange(this.currentSocket)
    }

    this.tickSocketKeys()
  }

  private removeSocket(key: string) {
    this.sockets.delete(key)
    this.tickSocketKeys()
  }

  async teardown() {
    if (this.sockets.size === 0) {
      return
    }

    this.sockets.forEach((socket) => {
      socket.teardown()
    })

    await firstValueFrom(
      this.openSockets.pipe(
        filter((keys) => keys.length === 0),
        first(),
      ),
    )

    this.groupOpts.onClosed()

    return
  }

  private createSocket(host: string, port: number) {
    const socketUrl = this.groupOpts.secure
      ? `wss://${host}:${port}/myko`
      : `ws://${host}/myko`

    const socket = new ReconnectSocket(socketUrl, this.makeSocket, {
      onClosed: () => {
        this.onSocketClosed(socketUrl)
      },
      onConnected: (url) => {
        this.onSocketOpen(url, socket)
      },
      onMessage: (data) => {
        this.groupOpts.onMessage(data)
      },
      onError: (error) => {
        this.groupOpts.onError(error)
      },
      onReconnecting: (url) => {
        if (!this.currentSocket || this.currentSocket === url) {
          this.groupOpts.onMainSocketReconnecting(url)
        }
      },
      onTerminated: () => {
        this.removeSocket(socketUrl)
      },
      reconnect: this.groupOpts.reconnect
        ? {
            interval: 1000,
            maxAttempts: Infinity,
          }
        : undefined,
    })
  }

  async bootstrap(host: string, port: number) {
    this.groupOpts.onLog('Bootstrapping Socket Group', host, port)
    await this.teardown()

    this.createSocket(host, port)
  }

  send(event: string | ArrayBufferLike | Blob) {
    if (this.groupOpts.socketSendMode === SocketSendMode.Single) {
      const socket = this.sockets.get(this.currentSocket)

      if (!socket) {
        this.groupOpts.onError('No current socket for group')
        return
      }

      socket.send(event)
    }

    if (this.groupOpts.socketSendMode === SocketSendMode.Broadcast) {
      this.sockets.forEach((socket) => {
        socket.send(event)
      })
    }
  }

  addServers(servers: { host: string; port: number }[]) {
    servers.forEach((server) => {
      this.createSocket(server.host, server.port)
    })
  }
}
