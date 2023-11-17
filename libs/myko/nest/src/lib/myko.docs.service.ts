import {
  CommandDocInfo,
  ItemDocInfo,
  PropDocInfo,
  QueryDocInfo,
  docRegistry,
} from '@myko/core/src/lib/registry'
import { Injectable } from '@nestjs/common'
import { LoggerService } from '@rship/logging'
import { mkdir, writeFile } from 'fs/promises'
import { groupBy } from 'ramda'

@Injectable()
export class MykoDocsService {
  constructor(private logger: LoggerService) {}

  async writeDocs(path: string) {
    this.logger.getLogger('Myko').dev.info('Writing Myko Documentation')

    await mkdir(path, { recursive: true })
    await writeFile(`${path}/README.md`, this.generateDocs())
    await writeFile(
      `${path}/docs/myko.json`,
      JSON.stringify(docRegistry, null, 2),
    )
  }

  private generateDocs() {
    this.logger.getLogger('Myko').dev.info('Generating Myko Documentation')

    const entityDocs = this.makeEntityDocs()

    const commandDocs = this.makeCommandDocs()

    return `# Entities\n\n${entityDocs.join(
      '\n\n',
    )}\n\n# Commands\n\n${commandDocs.join(
      '\n\n',
    )}\n\n# Queries\n\n${this.makeQueryDocs().join('\n\n')}`
  }

  private makeCommandDocs() {
    const commands = docRegistry.filter(
      (x) => x.type === 'command',
    ) as CommandDocInfo[]

    commands.map((command) => {
      return [h2(command.commandName)].join('\n')
    })

    return commands.map((command) => {
      const props = command.props
        .map((prop) => {
          return `${prop.propName}: ${prop.propType}`
        })
        .join('\n')

      return [
        h2(command.commandName),
        command.props.length > 0 ? code(object(props)) : 'No Props',
      ].join('\n')
    })
  }

  private makeQueryDocs() {
    const queries = docRegistry.filter(
      (x) => x.type === 'query',
    ) as QueryDocInfo[]

    queries.map((query) => {
      return [h2(query.queryName)].join('\n')
    })

    return queries.map((command) => {
      const props = command.props
        .map((prop) => {
          return `${prop.propName}: ${prop.propType}`
        })
        .join('\n')

      return [
        h2(command.queryName),
        command.props.length > 0 ? code(object(props)) : 'No Props',
      ].join('\n')
    })
  }

  private makeEntityDocs() {
    const entities = docRegistry.filter(
      (x) => x.type === 'item' && x.preventDocs !== true,
    ) as ItemDocInfo[]

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

        const byProp = groupBy((e) => e.propName, propEntries)

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
            (x: ItemDocInfo) =>
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
