export type ReconnectSocketOpts = {
  onConnected: (url: string) => void
  onReconnecting: (url: string) => void
  onClosed: () => void
  onMessage: (data: MessageEvent) => void
  onError: (error: Event | string) => void
  onTerminated: () => void
  reconnect?: {
    interval: number
    maxAttempts: number
  }
}

export class ReconnectSocket {
  private socket!: WebSocket

  private attempts = 0

  private tornDown = false

  constructor(
    private makeSocket: (socketUrl: string) => WebSocket,
    private opts: ReconnectSocketOpts,
  ) {}

  connect(url: string) {
    this.teardown()
    this.tornDown = false
    this.build(url)
  }

  private build(url: string) {
    this.socket = this.makeSocket(url)

    this.socket.binaryType = 'arraybuffer'

    this.socket.onopen = () => {
      this.attempts = 0
      this.opts.onConnected(url)
    }

    this.socket.onclose = () => {
      this.opts.onClosed()

      if (this.tornDown) {
        this.opts.onTerminated()
        return
      }

      if (this.opts.reconnect && this.opts.reconnect.maxAttempts > this.attempts) {
        this.attempts++
        setTimeout(() => {
          this.opts.onReconnecting(url)
          this.build(url)
        }, this.opts.reconnect.interval)

        return
      } else {
        this.opts.onTerminated()
      }
    }

    this.socket.onmessage = (event) => {
      this.opts.onMessage(event)
    }

    this.socket.onerror = (err) => {
      this.opts.onError(err)
    }
  }

  send(data: string | ArrayBufferLike | Blob) {
    if (this.socket.readyState !== this.socket.OPEN) {
      console.log('socket not ready')
      throw new Error('Not Connected')
    }

    this.socket.send(data)
  }

  ready(): boolean {
    return this.socket.readyState === this.socket.OPEN
  }

  teardown() {
    this.tornDown = true
    this.socket?.close()
  }
}
