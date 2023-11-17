import { Module, OnModuleInit, forwardRef } from '@nestjs/common'
import { MykoCommandBus } from './busses/command.bus'
import { ExplorerService } from './services'
import { LoggerModule, LoggerService } from '@rship/logging'
import { MykoQueryBus } from './busses'
import { MykoEventBus } from './busses/event.bus'
import { RedisPersisterFactory } from './persisters/redis.persisterFactory'
import { bufferTime, filter, groupBy, mergeMap } from 'rxjs'
import { SocketRegistry } from './registry/socket.registry'
import { ConfigModule, ConfigService } from '@nestjs/config'
import { KafkaPersisterFactory } from './persisters'
import { MykoGatewayModule } from './myko.gateway.module'
import { PeerRegistry } from './registry/peer.registry'
import { MykoReportBus } from './busses/report.bus'
import { groupBy as arrayGroupBy } from 'ramda'

import {
  unwrapCommand,
  unwrapItem,
  unwrapQuery,
  unwrapReport,
} from '@myko/core'
import { mkdir, writeFile } from 'fs/promises'
import {
  ItemDocInfo,
  PropDocInfo,
  docRegistry,
} from '@myko/core/src/lib/registry'

@Module({
  imports: [
    LoggerModule.forModule({ moduleName: 'Myko' }),
    ConfigModule,
    forwardRef(() => MykoGatewayModule),
  ],
  providers: [
    SocketRegistry,
    PeerRegistry,
    ExplorerService,
    MykoCommandBus,
    MykoQueryBus,
    MykoEventBus,
    MykoReportBus,
    RedisPersisterFactory,
    KafkaPersisterFactory,
  ],
  exports: [
    MykoCommandBus,
    MykoQueryBus,
    MykoEventBus,
    MykoReportBus,
    RedisPersisterFactory,
    KafkaPersisterFactory,
    SocketRegistry,
    PeerRegistry,
  ],
})
export class MykoModule implements OnModuleInit {
  constructor(
    private explorer: ExplorerService,
    private readonly commandBus: MykoCommandBus,
    private readonly queryBus: MykoQueryBus,
    private readonly eventBus: MykoEventBus,
    private readonly reportBus: MykoReportBus,
    private logger: LoggerService,
    private config: ConfigService,
  ) {}

  onModuleInit() {
    const doc = this.config.get('MYKO_DOC')

    if (doc) {
      this.writeDocs('./docs')
    }

    const { commands, queries, sagas, reports } = this.explorer.explore()
    this.commandBus.register(commands)
    this.queryBus.register(queries)
    this.reportBus.register(reports)
    this.eventBus.registerSagas(sagas)

    if (process.env.LOG_LEVEL?.toLocaleLowerCase() === 'debug') {
      this.eventBus.subject$
        .pipe(
          groupBy((e) => `${e.itemType}:${e.changeType}`),
          mergeMap((obss) =>
            obss.pipe(
              bufferTime(1000),
              filter((x) => x.length > 0),
            ),
          ),
        )
        .subscribe((e) => {
          this.logger
            .getLogger(e[0].itemType)
            .dev.debug(`${e[0].changeType} [${e.length}]`)
        })
    }
  }

  writeDocs(path: string) {
    this.logger.getLogger('Myko').dev.info('Writing Myko Documentation')

    mkdir(path, { recursive: true })
    writeFile(`${path}/myko.md`, this.generateDocs())
    writeFile(`${path}/myko.json`, JSON.stringify(docRegistry, null, 2))
  }

  generateDocs() {
    this.logger.getLogger('Myko').dev.info('Generating Myko Documentation')
    const { commands, queries, sagas, reports } = this.explorer.explore()

    const commandDocs = commands.map((command) => {
      return command.name
    })

    const entityDocs = this.makeEntityDocs()

    return `# Entities\n\n${entityDocs.join(
      '\n\n',
    )}\n\n# Commands\n\n${commandDocs.join('\n\n')}`
  }

  private makeEntityDocs() {
    const entities = docRegistry.filter(
      (x) => x.type === 'item' && x.preventDocs !== true,
    )

    return entities
      .sort((a, b) => a.entityType.localeCompare(b.entityType))
      .map((entity) => {
        const entityType = entity.entityType

        const propEntries = docRegistry.filter(
          (x) => x.type === 'prop' && x.entityType === entityType,
        ) as PropDocInfo[]

        const itemEntries = docRegistry.filter(
          (x) => x.type === 'item' && x.entityType === entityType,
        ) as ItemDocInfo[]

        const byProp = arrayGroupBy((e) => e.propName, propEntries)

        const props = Object.keys(byProp).map((propName) => {
          const prop = byProp[propName][0]
          const docStrings = byProp[propName]
            .map((p) => p.docString)
            .filter((x) => x?.length > 0)
            .map(comment)

          const nameAndType = `${prop.propName}: ${prop.propType}`

          const dep = byProp[propName].some((x) => !!x.deprecated)

          if (dep) {
            docStrings.push('// WARNING: deprecated')
          }

          return [...docStrings, nameAndType].join('\n')
        })

        const docString = itemEntries
          .map((item) => {
            return item.docString
          })
          .join('\n')

        const notes = itemEntries.some((x) => !!x.deprecated)
          ? ['WARNING: deprecated']
          : []

        const parents = itemEntries
          .map((i) => i.extends)
          .filter((x) => !!x)
          .map((x) => `extends ${link(x, `#${x.toLocaleLowerCase()}`)}`)

        const extendsString = parents.length > 0 ? [parents.join(', '), ''] : []

        const children = docRegistry
          .filter((x) => x.type === 'item' && x.extends === entityType)
          .map(
            (x) =>
              `* ${link(x.entityType, `#${x.entityType.toLocaleLowerCase()}`)}`,
          )

        const childrenString =
          children.length > 0 ? ['Extended By: ', ...children, ''] : []

        return [
          h2(entityType),
          ...extendsString,
          ...childrenString,
          ...notes.map(endl).map(note),
          docString,
          code(object(props.join('\n\n'))),
        ].join('\n')
      })
  }
}

const code = (str: string) => `\`\`\`ts\n${str}\n\`\`\` `

const h2 = (str: string) => `## ${str}\n`

const indent = (str: string) =>
  str
    .split('\n')
    .map((x) => `\t${x}`)
    .join('\n')

const endl = (str: string) => `${str}\n`

const comment = (str: string) => `// ${str}`

const object = (str: string) => `{\n${indent(str.trimEnd())}\n}`

const note = (str: string) => `> ${str}`

const link = (str: string, url: string) => `[${str}](${url})`
