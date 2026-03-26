import type { EntityDiff, ExportedEntity, DiffStatus, FieldDiff } from './entity-diff.js';

/**
 * Known parent FK fields. Maps child entity type to its parent FK field name.
 * This is Rship-specific — the framework doesn't expose relationship metadata to the client.
 *
 * TODO(ts): Generate this map from Rust relationship registrations.
 */
const PARENT_FK_FIELDS: Record<string, string[]> = {
  // Project children
  Scene: ['scopeId'],
  ActiveScene: ['scopeId'],
  Appearance: ['scopeId'],
  Alert: ['scopeId'],
  Bundle: ['scopeId'],
  Feed: ['scopeId'],
  Space: ['scopeId'],
  Service: ['projectId'],
  Link: ['projectId'],
  Fixture: ['projectId'],
  Camera: ['projectId'],
  LedWall: ['projectId'],
  Screen: ['projectId'],
  Point: ['projectId'],
  EventTrack: ['scopeId'],
  // Scene children
  Binding: ['scopeId'],
  // Binding children
  BindingNode: ['scopeId'],
  // Other scoped entities
  BundleStatus: ['sessionId', 'bundleId'],
  Instance: ['serviceId'],
  Action: ['instanceId'],
  Emitter: ['targetId'],
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
  diffs: Map<string, { status: DiffStatus; fields?: FieldDiff[]; entity: ExportedEntity }>,
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
      children: [],
    };
    allNodes.set(key, node);
    idToKey.set(id, key);
  }

  // Build parent-child relationships
  for (const [key, diff] of diffs) {
    const [type] = key.split(':');
    const fkFields = PARENT_FK_FIELDS[type];
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

  // Attach children
  for (const [key, node] of allNodes) {
    node.children = childrenByParent.get(key) ?? [];
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

  // Orphaned entities (not the root, not placed under any parent) become root children
  for (const [key, node] of allNodes) {
    if (!placed.has(key)) {
      rootNode.children.push(node);
    }
  }

  return rootNode;
}
