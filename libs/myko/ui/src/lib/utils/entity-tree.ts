import type { EntityDiff, ExportedEntity, DiffStatus, FieldDiff } from './entity-diff.js';

/**
 * Maps child entity type to its parent FK field names (camelCase).
 * Provided by the consuming application — generated from Rust relationship registrations.
 */
export type ParentFkFields = Record<string, string[]>;

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
 * Entities with known parent FK fields are placed under their parent.
 * Entities without a parent mapping (or whose parent isn't in the diff)
 * are placed as direct children of the root so they remain visible.
 *
 * @param rootType - The root entity type
 * @param rootId - The root entity ID
 * @param diffs - Flat diff map from diffEntityLists()
 * @returns Root EntityDiff node with nested children
 */
export function buildDiffTree(
  rootType: string,
  rootId: string,
  diffs: Map<string, { status: DiffStatus; fields?: FieldDiff[]; entity: ExportedEntity; currentEntity?: ExportedEntity }>,
  parentFkFields: ParentFkFields,
): EntityDiff {
  // Build flat EntityDiff nodes and index by ID for parent lookup
  const allNodes = new Map<string, EntityDiff>();
  const idToKey = new Map<string, string>();
  const childrenByParent = new Map<string, EntityDiff[]>();
  const placed = new Set<string>();

  for (const [key, diff] of diffs) {
    const [type, id] = key.split(':');
    const node: EntityDiff = {
      type,
      id,
      status: diff.status,
      name: entityName(diff.entity.data),
      fields: diff.fields,
      currentData: diff.currentEntity?.data,
      incomingData: diff.status === 'modified' ? diff.entity.data : undefined,
      children: [],
    };
    allNodes.set(key, node);
    idToKey.set(id, key);
  }

  // Build parent-child relationships
  for (const [key, diff] of diffs) {
    const [type] = key.split(':');
    const fkFields = parentFkFields[type];
    if (fkFields) {
      let foundParent = false;
      for (const fkField of fkFields) {
        const parentId = diff.entity.data[fkField] as string | undefined;
        if (parentId) {
          const parentKey = idToKey.get(parentId);
          if (parentKey) {
            if (!childrenByParent.has(parentKey)) {
              childrenByParent.set(parentKey, []);
            }
            childrenByParent.get(parentKey)!.push(allNodes.get(key)!);
            placed.add(key);
            foundParent = true;
            break;
          }
        }
      }
    }
  }

  // Get or create root node
  const rootKey = `${rootType}:${rootId}`;
  const rootNode = allNodes.get(rootKey) ?? {
    type: rootType,
    id: rootId,
    status: 'unchanged' as DiffStatus,
    name: rootId,
    children: [],
  };
  placed.add(rootKey);

  // Attach children from FK relationships
  for (const [key, node] of allNodes) {
    node.children = childrenByParent.get(key) ?? [];
  }

  // Orphaned entities (not the root, not placed under any parent) become root children
  for (const [key, node] of allNodes) {
    if (!placed.has(key)) {
      rootNode.children.push(node);
    }
  }

  // Deduplicate all children arrays — entities reachable via multiple FK paths
  // (e.g., BindingNodeConnection with both start_node_id and end_node_id) could
  // appear more than once
  for (const node of allNodes.values()) {
    if (node.children.length > 0) {
      const seen = new Set<string>();
      node.children = node.children.filter((c) => {
        const k = `${c.type}:${c.id}`;
        if (seen.has(k)) return false;
        seen.add(k);
        return true;
      });
    }
  }
  // Also dedup root if it was created fresh (not from allNodes)
  if (rootNode.children.length > 0) {
    const seen = new Set<string>();
    rootNode.children = rootNode.children.filter((c) => {
      const k = `${c.type}:${c.id}`;
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
  }

  return rootNode;
}
