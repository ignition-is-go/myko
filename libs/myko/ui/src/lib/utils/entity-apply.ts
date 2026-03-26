import type { ExportedEntity, DiffStatus, FieldDiff } from './entity-diff.js';

export interface ApplyPlan {
  toSet: ExportedEntity[];  // Added + modified, parents first
  toDel: ExportedEntity[];  // Shallowest removed entities only
}

/**
 * Compute the minimal SET/DEL operations to apply an import.
 *
 * @param diffs - Flat diff map from diffEntityLists()
 * @param allFkFields - All FK field names used in BelongsTo relationships (for cascade detection)
 */
export function computeApplyPlan(
  diffs: Map<string, { status: DiffStatus; fields?: FieldDiff[]; entity: ExportedEntity }>,
  allFkFields: string[],
): ApplyPlan {
  const toSet: ExportedEntity[] = [];
  const allRemoved = new Map<string, ExportedEntity>();

  for (const [_key, diff] of diffs) {
    if (diff.status === 'added' || diff.status === 'modified') {
      toSet.push(diff.entity);
    } else if (diff.status === 'removed') {
      allRemoved.set(diff.entity.data.id as string, diff.entity);
    }
  }

  // Filter removed to shallowest only — if a removed entity's parent is also
  // removed, skip it (the cascade will handle it)
  const removedIds = new Set(allRemoved.keys());
  const toDel: ExportedEntity[] = [];
  for (const [_id, entity] of allRemoved) {
    const hasRemovedParent = allFkFields.some((fk) => {
      const parentId = entity.data[fk] as string | undefined;
      return parentId && removedIds.has(parentId);
    });
    if (!hasRemovedParent) {
      toDel.push(entity);
    }
  }

  return { toSet, toDel };
}
