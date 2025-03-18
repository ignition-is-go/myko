import { clientIdPropertyRegistry, eventBus, type ID } from '@myko/core'
import {
  MCOMMAND_EVENT,
  MEVENT_EVENT,
  MPING_EVENT,
  MQUERY_CANCEL,
  MQUERY_EVENT,
  MREPORT_CANCEL,
  MREPORT_EVENT,
  type WSMMessage,
} from '@myko/ws'
import type { Subject } from 'rxjs'
import { handleCommand } from './command.handler'
import { unsub } from './common'
import { handleQuery } from './query.handler'
import { handleReport } from './report.handler'

export const handleMessage =
  (tx: Subject<{ clientId: ID; data: WSMMessage }>) =>
  async (x: { clientId: ID; data: WSMMessage }) => {
    switch (x.data.event) {
      case MCOMMAND_EVENT: {
        await handleCommand(x.clientId, x.data.data, tx)
        break
      }

      case MQUERY_EVENT: {
        handleQuery(x.clientId, x.data.data, tx)
        break
      }

      case MQUERY_CANCEL: {
        unsub.next(x.data.tx)
        break
      }

      case MREPORT_EVENT: {
        handleReport(x.clientId, x.data.data, tx)
        break
      }

      case MREPORT_CANCEL: {
        unsub.next(x.data.tx)
        break
      }

      case MEVENT_EVENT: {
        if (clientIdPropertyRegistry.has(x.data.data.itemType)) {
          const propertyKey = clientIdPropertyRegistry.get(x.data.data.itemType)
          if (propertyKey) {
            Reflect.set(x.data.data.item, propertyKey, x.clientId)
          }
        }

        eventBus.publish(x.data.data)
        break
      }

      case MPING_EVENT: {
        tx.next({
          clientId: x.clientId,

          data: {
            event: MPING_EVENT,
            data: x.data.data,
          },
        })
        break
      }

      default: {
        console.log('Unknown message', x)
      }
    }
  }
