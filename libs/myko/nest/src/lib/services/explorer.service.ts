import { Injectable, Type } from '@nestjs/common'
import { ModulesContainer } from '@nestjs/core'
import {
  MCommand,
  MCommandHandler,
  MQueryHandler,
  MYKO_HANDLER_COMMAND_ID_KEY,
  MYKO_HANDLER_QUERY_ID_KEY,
  MYKO_SAGA_METADATA,
  MQuery,
  MSaga,
} from '@myko/core'
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper'
import { Module } from '@nestjs/core/injector/module'
@Injectable()
export class ExplorerService {
  constructor(private readonly modulesContainer: ModulesContainer) {}

  explore() {
    const modules = [...this.modulesContainer.values()]
    const commands = this.flatMap<MCommandHandler<MCommand>>(
      modules,
      (instance) => this.filterProvider(instance, MYKO_HANDLER_COMMAND_ID_KEY),
    )
    const queries = this.flatMap<MQueryHandler<MQuery>>(modules, (instance) =>
      this.filterProvider(instance, MYKO_HANDLER_QUERY_ID_KEY),
    )

    const sagas = this.flatMap<MSaga>(modules, (instance) =>
      this.filterProvider(instance, MYKO_SAGA_METADATA),
    )
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
