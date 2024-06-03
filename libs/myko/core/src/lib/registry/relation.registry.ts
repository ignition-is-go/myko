export const relationRegistry: Set<Relation> = new Set<Relation>()

export type Relation =
  | {
      type: 'belongs-to'
      foreignKey: 'id'
      foreignType: string
      localType: string
      localKey: string
    }
  | {
      type: 'owns-many'
      foreignKey: 'id'
      foreignType: string
      localKey: string
      localType: string
    }
  | {
      type: 'ensure-for'
      dependencies: {
        foreignType: string
        foreignKey: 'id'
        localKey: string
      }[]
      localType: string
      makeDefault: (...args: any) => void
    }
