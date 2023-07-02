import * as WebSocket from 'ws'
import { WebSocketAdapter, INestApplicationContext } from '@nestjs/common'
import { MessageMappingProperties } from '@nestjs/websockets'
import { Observable, fromEvent, EMPTY } from 'rxjs'
import { mergeMap, filter, map } from 'rxjs/operators'
import { MykoProtocol } from '@myko/core'
import { Decoder, Encoder } from '@msgpack/msgpack'
import { clientProtocols } from './registry/client.protocols'

export class MykoAdapter implements WebSocketAdapter {
  private decoders = new Map<MykoProtocol, (data: any) => any>()
  private encoders = new Map<MykoProtocol, (data: any) => any>()

  constructor(private app: INestApplicationContext) {
    const decoder = new Decoder()
    const encoder = new Encoder()
    this.decoders.set(MykoProtocol.JSON, (data) => JSON.parse(data))
    this.decoders.set(MykoProtocol.MSGPACK, (data) => decoder.decode(data))

    this.encoders.set(MykoProtocol.JSON, (data) => JSON.stringify(data))
    this.encoders.set(MykoProtocol.MSGPACK, (data) => encoder.encode(data))
  }

  create(port: number, options: any = {}): any {
    return new WebSocket.Server({ port, ...options })
  }

  bindClientConnect(server, callback: Function) {
    server.on('connection', callback)
  }

  bindMessageHandlers(
    client: WebSocket,
    handlers: MessageMappingProperties[],
    process: (data: any) => Observable<any>,
  ) {
    fromEvent(client, 'message')
      .pipe(
        map((data: any) =>
          this.decoders.get(clientProtocols.get(client) ?? MykoProtocol.JSON)(
            data.data,
          ),
        ),
        mergeMap((data) => this.bindMessageHandler(data, handlers, process)),
        filter((result) => result),
      )
      .subscribe((response) => {
        const encode = this.encoders.get(
          clientProtocols.get(client) ?? MykoProtocol.JSON,
        )
        client.send(encode(response))
      })
  }

  bindMessageHandler(
    message: any,
    handlers: MessageMappingProperties[],
    process: (data: any) => Observable<any>,
  ): Observable<any> {
    const messageHandler = handlers.find(
      (handler) => handler.message === message.event,
    )
    if (!messageHandler) {
      return EMPTY
    }
    return process(messageHandler.callback(message.data))
  }

  close(server) {
    server.close()
  }
}
