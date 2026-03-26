import type { EntityDiff, ExportedEntity, DiffStatus, FieldDiff } from './entity-diff.js';

/**
 * Known parent FK fields. Maps child entity type to its parent FK field name.
 * This is Rship-specific — the framework doesn't expose relationship metadata to the client.
 *
 * TODO(ts): Generate this map from Rust relationship registrations.
 */
const PARENT_FK_FIELDS: Record<string, string> = {
  Scene: 'scopeId',
  Binding: 'scopeId',
  BindingNode: 'scopeId',
  // Add other entity types as needed
};

/**
 * Extract a display name from entity data.
 * Tries 'name' field first, falls back to 'id'.
 */
function entityName(data: Record<string, unknown>): string {
  return (data.name as string) ?? (data.id as string) ?? 'unknown';
}

/**
 * Build an EntityDiff tree from a flat diff map.
 *
 * @param rootType - The root entity type
 * @param rootId - The root entity ID
 * @param diffs - Flat diff map from diffEntityLists()
 * @returns Root EntityDiff node with nested children
 */
export function buildDiffTree(
  rootType: string,
  rootId: string,
  diffs: Map<string, { status: DiffStatus; fields?: FieldDiff[]; entity: ExportedEntity }>,
): EntityDiff {
  // Build flat EntityDiff nodes and index by ID for parent lookup
  const allNodes = new Map<string, EntityDiff>();
  const idToKey = new Map<string, string>();
  const childrenByParent = new Map<string, EntityDiff[]>();

  for (const [key, diff] of diffs) {
    const [type, id] = key.split(':');
    const node: EntityDiff = {
      type,
      id,
      status: diff.status,
      name: entityName(diff.entity.data),
      fields: diff.fields,
      children: [],
    };
    allNodes.set(key, node);
    idToKey.set(id, key);
  }

  // Build parent-child relationships
  for (const [key, diff] of diffs) {
    const [type] = key.split(':');
    const fkField = PARENT_FK_FIELDS[type];
    if (fkField) {
      const parentId = diff.entity.data[fkField] as string | undefined;
      if (parentId) {
        const parentKey = idToKey.get(parentId);
        if (parentKey) {
          if (!childrenByParent.has(parentKey)) {
            childrenByParent.set(parentKey, []);
          }
          childrenByParent.get(parentKey)!.push(allNodes.get(key)!);
        }
      }
    }
  }

  // Attach children
  for (const [key, node] of allNodes) {
    node.children = childrenByParent.get(key) ?? [];
  }

  // Return root node
  const rootKey = `${rootType}:${rootId}`;
  return allNodes.get(rootKey) ?? {
    type: rootType,
    id: rootId,
    status: 'unchanged',
    name: rootId,
    children: [],
  };
}
