import * as WebSocket from 'ws'
import { WebSocketAdapter, INestApplicationContext } from '@nestjs/common'
import { MessageMappingProperties } from '@nestjs/websockets'
import { Observable, fromEvent, EMPTY } from 'rxjs'
import { mergeMap, filter, map } from 'rxjs/operators'
import { MykoProtocol } from '@myko/core'
import {
  clientProtocols,
  decoders,
  encoders,
} from './registry/client.protocols'

export class MykoAdapter implements WebSocketAdapter {
  handlers = new Map<string, Function>()
  constructor(private app: INestApplicationContext) {}

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
    handlers.forEach(({ message, callback }) => {
      this.handlers.set(message, callback)
    })

    fromEvent(client, 'message')
      .pipe(
        map((data: any) =>
          decoders.get(clientProtocols.get(client) ?? MykoProtocol.JSON)(
            data.data,
          ),
        ),
        mergeMap((data) => this.bindMessageHandler(data, handlers, process)),
        filter((result) => result),
      )
      .subscribe((response) => {
        const encode = encoders.get(
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
    const callback = this.handlers.get(message.event)
    if (!callback) {
      return EMPTY
    }
    return process(callback(message.data))
  }

  close(server) {
    server.close()
  }
}
