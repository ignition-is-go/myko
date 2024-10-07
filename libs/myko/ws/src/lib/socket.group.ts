import {
  filter,
  first,
  firstValueFrom,
  pairwise,
  ReplaySubject,
  startWith,
} from 'rxjs'
import { ReconnectSocket, type ReconnectSocketOpts } from './socket.reconnect'
export enum SocketSendMode {
  Single = 'Single',
  Broadcast = 'Broadcast',
}

export type SocketGroupOpts = {
  onGroupConnected: (url: string) => void
  onGroupClosed: () => void
  onGroupError: (error: string) => void
  onGroupLog: (...log: any[]) => void
  onGroupMessage: (data: MessageEvent) => void
  onGroupMainServerChange: (url: string) => void
  onMainSocketReconnecting: (url: string) => void
  socketSendMode: SocketSendMode
}

export class SocketGroup {
  private sockets = new Map<string, ReconnectSocket>()

  private socketKeys = new ReplaySubject<string[]>(1)

  private currentSocket: string

  get ready() {
    return this.sockets.size > 0
  }

  constructor(
    private makeSocket: (socketUrl: string) => WebSocket,
    private socketOpts: ReconnectSocketOpts,
    private groupOpts: SocketGroupOpts,
  ) {
    this.socketKeys.pipe(startWith([]), pairwise()).subscribe((keys) => {
      if (keys[0].length === 0 && keys[1].length === 1) {
        this.onFirstClientConnected(keys[1][0])
      }

      if (keys[0].length > 0 && keys[1].length === 0) {
        this.groupOpts.onGroupClosed()
        this.currentSocket = undefined
      }
    })
  }

  private onFirstClientConnected(key: string) {
    const mainSocket = this.sockets.get(key)

    if (!mainSocket) {
      this.groupOpts.onGroupError('Cannot Establish Main Socket')
      return
    }

    this.currentSocket = key

    this.groupOpts.onGroupConnected(key)
  }

  private saveSocket(key: string, socket: ReconnectSocket) {
    this.sockets.set(key, socket)
    this.socketKeys.next(Array.from(this.sockets.keys()))
  }

  private removeSocket(key: string) {
    if (this.sockets.has(key)) {
      this.sockets.delete(key)
    }

    if (key === this.currentSocket) {
      this.currentSocket = this.sockets.keys().next().value

      if (this.currentSocket) {
        this.groupOpts.onGroupMainServerChange(this.currentSocket)
      }
    }

    this.socketKeys.next(Array.from(this.sockets.keys()))

    if (this.sockets.size === 0) {
      this.groupOpts.onGroupClosed()
    }
  }

  async teardown() {
    if (this.sockets.size === 0) {
      return
    }

    this.sockets.forEach((socket) => {
      socket.teardown()
    })

    await firstValueFrom(
      this.socketKeys.pipe(
        filter((keys) => keys.length === 0),
        first(),
      ),
    )

    this.groupOpts.onGroupClosed()

    return
  }

  private createSocket(host: string, port: number) {
    const socketUrl = `ws://${host}:${port}/myko`

    const socket = new ReconnectSocket(socketUrl, this.makeSocket, {
      onClosed: () => {
        this.socketOpts.onClosed()
      },
      onConnected: (url) => {
        this.saveSocket(url, socket)
      },
      onMessage: (data) => {
        this.socketOpts.onMessage(data)
      },
      onError: (error) => {
        this.groupOpts.onGroupError(error)
        this.socketOpts.onError(error)
      },
      onReconnecting: (url) => {
        if (!this.currentSocket || this.currentSocket === url) {
          this.groupOpts.onMainSocketReconnecting(url)
        }
        this.socketOpts.onReconnecting(url)
      },
      onTerminated: () => {
        this.removeSocket(socketUrl)
      },
      reconnect: this.socketOpts.reconnect,
    })
  }

  async bootstrap(host: string, port: number) {
    this.groupOpts.onGroupLog('Bootstrapping Socket Group', host, port)
    await this.teardown()

    this.createSocket(host, port)
  }

  send(event: string | ArrayBufferLike | Blob) {
    if (this.groupOpts.socketSendMode === SocketSendMode.Single) {
      const socket = this.sockets.get(this.currentSocket)

      if (!socket) {
        this.groupOpts.onGroupError('No current socket for group')
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
