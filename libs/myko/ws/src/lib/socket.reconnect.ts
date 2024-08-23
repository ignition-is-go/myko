export class ReconnectSocket {
  private socket: WebSocket

  private attempts = 0

  private tornDown = false

  constructor(
    private url: string,
    private makeSocket: (socketUrl: string) => WebSocket,
    private opts: {
      onConnected: (url: string) => void
      onReconnecting: (url: string) => void
      onClosed: () => void
      onMessage: (data: MessageEvent) => void
      onError: (error) => void
      reconnect?: {
        interval: number
        maxAttempts?: number
      }
    },
  ) {
    console.log('ReconnectSocket')
    this.socket = this.makeSocket(this.url)
    this.socket.binaryType = 'arraybuffer'

    this.socket.onopen = () => {
      opts.onConnected(this.url)
    }

    this.socket.onclose = () => {
      if (this.tornDown) {
        opts.onClosed()
        return
      }

      if (
        this.opts.reconnect &&
        (!this.opts.reconnect.maxAttempts ||
          this.opts.reconnect.maxAttempts > this.attempts)
      ) {
        this.attempts++
        setTimeout(() => {
          opts.onReconnecting(this.url)
          this.socket = this.makeSocket(this.url)
        }, this.opts.reconnect.interval)

        return
      }

      opts.onClosed()
    }

    this.socket.onmessage = (event) => {
      opts.onMessage(event)
    }
  }

  send(data: any) {
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
