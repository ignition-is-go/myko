export type ReconnectSocketOpts = {
  onConnected: (url: string) => void
  onReconnecting: (url: string) => void
  onClosed: () => void
  onMessage: (data: MessageEvent) => void
  onError: (error) => void
  onTerminated: () => void
  reconnect: {
    interval: number
    maxAttempts: number
  }
}

export class ReconnectSocket {
  private socket: WebSocket

  private attempts = 0

  private tornDown = false

  constructor(
    private url: string,
    private makeSocket: (socketUrl: string) => WebSocket,
    private opts: ReconnectSocketOpts,
  ) {
    this.connect()
  }

  private connect() {
    this.socket = this.makeSocket(this.url)
    this.socket.binaryType = 'arraybuffer'

    this.socket.onopen = () => {
      this.opts.onConnected(this.url)
    }

    this.socket.onclose = () => {
      this.opts.onClosed()

      if (this.tornDown) {
        this.opts.onTerminated()
        return
      }

      if (
        this.opts.reconnect &&
        this.opts.reconnect.maxAttempts > this.attempts
      ) {
        this.attempts++
        setTimeout(() => {
          this.opts.onReconnecting(this.url)
          this.connect()
        }, this.opts.reconnect.interval)

        return
      } else {
        this.opts.onTerminated()
      }
    }

    this.socket.onmessage = (event) => {
      this.opts.onMessage(event)
    }
  }

  send(data: string | ArrayBufferLike | Blob) {
    if (this.socket.readyState !== this.socket.OPEN) {
      throw new Error('Not Connected')
    }

    this.socket.send(data)
  }

  ready() {
    return this.socket.readyState === this.socket.OPEN
  }

  teardown() {
    this.tornDown = true
    this.socket.close()
  }
}
