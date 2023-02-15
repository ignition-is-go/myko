import { Injectable, Type } from '@nestjs/common'
import { ModulesContainer } from '@nestjs/core'
import {
  IMykoCommand,
  IMykoCommandHandler,
  IMykoQueryHandler,
  MYKO_HANDLER_COMMAND_ID_KEY,
  MYKO_HANDLER_QUERY_ID_KEY,
  MYKO_SAGA_METADATA,
  MykoQueryable,
  IMykoQuery,
  IMykoSaga,
  IMykoEvent,
  IMykoItem,
  MykoEventType,
} from '@myko/core'
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper'
import { Module } from '@nestjs/core/injector/module'
@Injectable()
export class ExplorerService {
  constructor(private readonly modulesContainer: ModulesContainer) {}

  explore() {
    const modules = [...this.modulesContainer.values()]
    const commands = this.flatMap<IMykoCommandHandler<IMykoCommand>>(
      modules,
      (instance) => this.filterProvider(instance, MYKO_HANDLER_COMMAND_ID_KEY),
    )
    const queries = this.flatMap<IMykoQueryHandler<IMykoQuery<MykoQueryable>>>(
      modules,
      (instance) => this.filterProvider(instance, MYKO_HANDLER_QUERY_ID_KEY),
    )

    const sagas = this.flatMap<
      IMykoSaga<IMykoEvent<IMykoItem, MykoEventType>, IMykoCommand>
    >(modules, (instance) => this.filterProvider(instance, MYKO_SAGA_METADATA))
    return { commands, queries, sagas }
  }

  flatMap<T>(
    modules: Module[],
    callback: (instance: InstanceWrapper) => Type<any> | undefined,
  ): Type<T>[] {
    const items = modules
      .map((module) => [...module.providers.values()].map(callback))
      .reduce((a, b) => a.concat(b), [])
    return items.filter((element) => !!element) as Type<T>[]
  }

  filterProvider(
    wrapper: InstanceWrapper,
    metadataKey: string,
  ): Type<any> | undefined {
    const { instance } = wrapper
    if (!instance) {
      return undefined
    }
    return this.extractMetadata(instance, metadataKey)
  }

  extractMetadata(
    instance: Record<string, any>,
    metadataKey: string,
  ): Type<any> {
    if (!instance.constructor) {
      return
    }
    const metadata = Reflect.getMetadata(metadataKey, instance.constructor)
    return metadata ? (instance.constructor as Type<any>) : undefined
  }
}
