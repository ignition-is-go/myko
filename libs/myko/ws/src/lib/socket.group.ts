import { ReconnectSocket } from './socket.reconnect'
export enum SocketSendMode {
  Single = 'Single',
  Broadcast = 'Broadcast',
}

export type SocketGroupOpts = {
  onConnected: (url: string) => void
  onClosed: () => void
  onError: (error: Event | string) => void
  onLog: (...log: unknown[]) => void
  onMessage: (data: MessageEvent) => void
  onMainServerChange: (url: string) => void
  onMainSocketReconnecting: (url: string) => void
  socketSendMode: SocketSendMode
  reconnect: boolean
  secure: boolean
}

export class SocketGroup {
  private openSockets = new Map<string, ReconnectSocket>()

  private allSockets = new Map<string, ReconnectSocket>()

  private currentSocket: string | undefined

  get openAddresses() {
    return Array.from(this.openSockets.keys())
  }

  get ready() {
    return this.openAddresses.length > 0
  }

  constructor(
    private makeSocket: (socketUrl: string) => WebSocket,
    private groupOpts: SocketGroupOpts,
  ) {}

  private onFirstClientConnected(key: string) {
    const mainSocket = this.openSockets.get(key)

    if (!mainSocket) {
      this.groupOpts.onError('Cannot Establish Main Socket')
      return
    }

    this.currentSocket = key

    this.groupOpts.onConnected(key)
  }

  private checkForGroupClose() {
    if (this.openAddresses.length === 0) {
      this.groupOpts.onClosed()
    }
  }

  private onSocketOpen(key: string, socket: ReconnectSocket) {
    const emptyAtStart = this.openAddresses.length === 0

    this.openSockets.set(key, socket)

    const hasOneNow = this.openAddresses.length === 1

    if (emptyAtStart && hasOneNow) {
      this.currentSocket = key
      this.onFirstClientConnected(key)
    }
  }

  private onSocketClosed(key: string) {
    this.openSockets.delete(key)

    if (key === this.currentSocket) {
      this.currentSocket = this.openAddresses[0]
      if (this.currentSocket) {
        this.groupOpts.onMainServerChange(this.currentSocket)
      }
    }

    this.checkForGroupClose()
  }

  private onSocketTerminated(key: string) {
    this.allSockets.delete(key)
  }

  async teardown() {
    if (this.openSockets.size === 0) {
      return
    }

    this.openSockets.forEach((socket) => {
      socket.teardown()
    })

    this.groupOpts.onClosed()

    return
  }

  private createSocket(host: string, port: number) {
    // When secure, omit port (reverse proxy on 443 handles routing)
    const socketUrl = this.groupOpts.secure
      ? `wss://${host}/myko`
      : `ws://${host}:${port}/myko`

    if (this.allSockets.has(socketUrl)) {
      return
    }

    const socket = new ReconnectSocket(this.makeSocket, {
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
        this.onSocketTerminated(socketUrl)
      },
      reconnect: this.groupOpts.reconnect
        ? {
            interval: 1000,
            maxAttempts: Infinity,
          }
        : undefined,
    })

    this.allSockets.set(socketUrl, socket)

    socket.connect(socketUrl)
  }

  async bootstrap(host: string, port: number) {
    this.groupOpts.onLog('Bootstrapping Socket Group', host, port)
    await this.teardown()

    this.createSocket(host, port)
  }

  send(event: string | ArrayBufferLike | Blob) {
    if (this.groupOpts.socketSendMode === SocketSendMode.Single) {
      if (!this.currentSocket) {
        this.groupOpts.onError('No current socket set for group')
        return
      }

      const socket = this.openSockets.get(this.currentSocket)

      if (!socket || !socket.ready()) {
        this.groupOpts.onError('No current socket available for group')
        return
      }

      socket.send(event)
    }

    if (this.groupOpts.socketSendMode === SocketSendMode.Broadcast) {
      this.openSockets.forEach((socket) => {
        if (socket.ready()) {
          socket.send(event)
        } else {
          this.groupOpts.onError('No socket ready for group')
        }
      })
    }
  }

  addServers(servers: { host: string; port: number }[]) {
    servers.forEach((server) => {
      this.createSocket(server.host, server.port)
    })
  }
}
