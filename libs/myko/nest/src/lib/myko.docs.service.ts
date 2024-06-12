import {
  CommandDocInfo,
  ItemDocInfo,
  PropDocInfo,
  QueryDocInfo,
  docRegistry,
} from '@myko/core/src/lib/registry'
import { Injectable } from '@nestjs/common'
import { mkdir, writeFile } from 'fs/promises'
import { groupBy } from 'ramda'
import { MykoLogger } from './logger'

@Injectable()
export class MykoDocsService {
  constructor(private logger: MykoLogger) {}

  async writeDocs(path: string) {
    this.logger.log('Writing Myko Documentation')

    await mkdir(path, { recursive: true })
    await writeFile(`${path}/README.md`, this.generateDocs())
    await writeFile(
      `${path}/docs/myko.json`,
      JSON.stringify(docRegistry, null, 2),
    )
  }

  private generateDocs() {
    this.logger.log('Generating Myko Documentation')

    const entityDocs = this.makeEntityDocs()

    const commandDocs = this.makeCommandDocs()

    const queryDocs = this.makeQueryDocs()

    return [
      '### Table of Contents',
      list(
        link('Entities', '#entities'),
        link('Queries', '#queries'),
        link('Commands', '#commands'),
      ),
      '# Entities',
      ...entityDocs,
      '# Queries',
      ...queryDocs,
      '# Commands',
      ...commandDocs,
    ].join('\n\n')
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
        command.props.length > 0
          ? code(
              object(
                [`commandId: '${command.commandId}'`, object(props)].join('\n'),
              ),
            )
          : 'No Props',
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

    return queries.map((query) => {
      const props = query.props
        .map((prop) => {
          return `${prop.propName}: ${prop.propType}`
        })
        .join('\n')

      return [
        h2(query.queryName),
        query.queryReturnType &&
          `Returns: ${link(
            query.queryReturnType + '[]',
            `#${query.queryReturnType?.toLocaleLowerCase()}`,
          )}`,
        query.props.length > 0
          ? code(
              object(
                [`queryId: '${query.queryId}' `, object(props)].join('\n'),
              ),
            )
          : 'No Props',
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

        const queries = docRegistry.filter(
          (r) => r.type === 'query' && r.queryReturnType === entityType,
        ) as QueryDocInfo[]

        const queryStrings = list(
          ...queries.map((x) =>
            link(x.queryName, `#${x.queryName.toLocaleLowerCase()}`),
          ),
        )

        const queryString =
          queries.length > 0 ? [h3('Queries'), queryStrings, ''] : []

        const parents = itemEntries
          .map((i) => i.extends)
          .filter((x) => !!x)
          .map((x) => `extends ${link(x, `#${x.toLocaleLowerCase()}`)}`)

        const extendsString = parents.length > 0 ? [parents.join(', '), ''] : []

        const children = docRegistry
          .filter((x) => x.type === 'item' && x.extends === entityType)
          .map((x: ItemDocInfo) =>
            listItem(
              link(x.entityType, `#${x.entityType.toLocaleLowerCase()}`),
            ),
          )

        const childrenString =
          children.length > 0 ? [h3('Extended By'), ...children, ''] : []

        return [
          h2(entityType),
          ...extendsString,
          ...childrenString,
          ...notes.map(endl).map(note),
          docString,
          ...queryString,
          code(object(props.join('\n\n'))),
        ].join('\n')
      })
  }
}

const code = (str: string) => `\`\`\`ts\n${str}\n\`\`\` `

const h2 = (str: string) => `## ${str}\n`

const h3 = (str: string) => `### ${str}\n`

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

const listItem = (str: string) => `- ${str}`

const list = (...strings: string[]) => `${strings.map(listItem).join('\n')}`
