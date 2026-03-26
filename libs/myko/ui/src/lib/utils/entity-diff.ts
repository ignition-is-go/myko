/** Diff status for an entity or field. */
export type DiffStatus = 'added' | 'removed' | 'modified' | 'unchanged';

/** Field-level diff for a modified entity. */
export interface FieldDiff {
  field: string;
  oldValue?: unknown;
  newValue?: unknown;
}

/** Entity-level diff node with children for tree display. */
export interface EntityDiff {
  type: string;
  id: string;
  status: DiffStatus;
  /** Display label (entity name or id). */
  name?: string;
  /** Field-level changes — only populated for 'modified' status. */
  fields?: FieldDiff[];
  /** The current (server) entity data — only populated for 'modified' status. */
  currentData?: Record<string, unknown>;
  /** The incoming entity data — only populated for 'modified' status. */
  incomingData?: Record<string, unknown>;
  children: EntityDiff[];
}

/** A single exported entity from an EntityTreeExport. */
export interface ExportedEntity {
  entityType: string;
  data: Record<string, unknown>;
}

/**
 * Compare two flat entity lists and produce field-level diffs.
 *
 * @param current - Entities currently on the server
 * @param incoming - Entities from the import source (file or snapshot)
 * @returns Map of "type:id" -> { status, fields }
 */
export function diffEntityLists(
  current: ExportedEntity[],
  incoming: ExportedEntity[],
): Map<string, { status: DiffStatus; fields?: FieldDiff[]; entity: ExportedEntity }> {
  const currentMap = new Map<string, ExportedEntity>();
  for (const e of current) {
    currentMap.set(`${e.entityType}:${e.data.id}`, e);
  }

  const incomingMap = new Map<string, ExportedEntity>();
  for (const e of incoming) {
    incomingMap.set(`${e.entityType}:${e.data.id}`, e);
  }

  const result = new Map<
    string,
    { status: DiffStatus; fields?: FieldDiff[]; entity: ExportedEntity; currentEntity?: ExportedEntity }
  >();

  // Check incoming entities against current
  for (const [key, inc] of incomingMap) {
    const cur = currentMap.get(key);
    if (!cur) {
      result.set(key, { status: 'added', entity: inc });
    } else {
      const fields = diffFields(cur.data, inc.data);
      if (fields.length > 0) {
        result.set(key, { status: 'modified', fields, entity: inc, currentEntity: cur });
      } else {
        result.set(key, { status: 'unchanged', entity: inc });
      }
    }
  }

  // Check for removed entities (in current but not in incoming)
  for (const [key, cur] of currentMap) {
    if (!incomingMap.has(key)) {
      result.set(key, { status: 'removed', entity: cur });
    }
  }

  return result;
}

/** Compare two entity data objects field-by-field. */
function diffFields(
  oldData: Record<string, unknown>,
  newData: Record<string, unknown>,
): FieldDiff[] {
  const diffs: FieldDiff[] = [];
  const allKeys = new Set([...Object.keys(oldData), ...Object.keys(newData)]);

  for (const key of allKeys) {
    // Skip hash — it's derived and always changes when fields change
    if (key === 'hash') continue;

    const oldVal = oldData[key];
    const newVal = newData[key];

    if (JSON.stringify(oldVal) !== JSON.stringify(newVal)) {
      diffs.push({ field: key, oldValue: oldVal, newValue: newVal });
    }
  }

  return diffs;
}
