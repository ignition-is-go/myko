<script lang="ts">
  import type { ExportedEntity, EntityDiff, DiffStatus } from '../utils/entity-diff.js';
  import { diffEntityLists } from '../utils/entity-diff.js';
  import type { ParentFkFields } from '../utils/entity-tree.js';
  import { buildDiffTree } from '../utils/entity-tree.js';
  import EntityDiffBadge from './EntityDiffBadge.svelte';

  interface Props {
    /** Root entity type (e.g., "Project") */
    rootType: string;
    /** Root entity ID */
    rootId: string;
    /** Entities currently on the server */
    current: ExportedEntity[];
    /** Entities from the incoming source (file or snapshot) */
    incoming: ExportedEntity[];
    /** Maps child entity type to parent FK field names */
    parentFkFields: ParentFkFields;
    /** Set of "Type:id" keys the user chose to exclude from import */
    excluded?: Set<string>;
    /** Whether to show unchanged entities */
    showUnchanged?: boolean;
  }

  let { rootType, rootId, current, incoming, parentFkFields, excluded = $bindable(new Set()), showUnchanged = false }: Props = $props();

  const diffs = $derived(diffEntityLists(current, incoming));
  const tree = $derived(buildDiffTree(rootType, rootId, diffs, parentFkFields));

  const toggleExclude = (key: string) => {
    const next = new Set(excluded);
    if (next.has(key)) {
      next.delete(key);
    } else {
      next.add(key);
    }
    excluded = next;
  };

  /** Action labels per status. First = apply action, second = skip action. */
  function actionLabels(status: DiffStatus): [string, string] {
    switch (status) {
      case 'removed': return ['delete', 'keep'];
      case 'added': return ['add', 'skip'];
      case 'modified': return ['update', 'keep'];
      default: return ['apply', 'skip'];
    }
  }


  const stats = $derived.by(() => {
    let toDelete = 0, toUpdate = 0, toAdd = 0, toSkip = 0, unchanged = 0;
    for (const [key, d] of diffs) {
      if (d.status === 'unchanged') { unchanged++; continue; }
      if (excluded.has(key)) { toSkip++; continue; }
      if (d.status === 'removed') toDelete++;
      else if (d.status === 'modified') toUpdate++;
      else if (d.status === 'added') toAdd++;
    }
    return { toDelete, toUpdate, toAdd, toSkip, unchanged };
  });

  // --- Batch operations ---

  /** Keys grouped by change type (excludes unchanged). */
  const keysByStatus = $derived.by(() => {
    const groups: Record<string, string[]> = { added: [], removed: [], modified: [] };
    for (const [key, d] of diffs) {
      if (d.status !== 'unchanged') groups[d.status]?.push(key);
    }
    return groups;
  });

  /** Keys grouped by entity type (excludes unchanged). */
  const keysByEntityType = $derived.by(() => {
    const groups: Record<string, string[]> = {};
    for (const [key, d] of diffs) {
      if (d.status === 'unchanged') continue;
      const [type] = key.split(':');
      (groups[type] ??= []).push(key);
    }
    return groups;
  });

  function batchApply(keys: string[]) {
    const next = new Set(excluded);
    for (const k of keys) next.delete(k);
    excluded = next;
    saveExpanded(expandedSet);
  }

  function batchSkip(keys: string[]) {
    const next = new Set(excluded);
    for (const k of keys) next.add(k);
    excluded = next;
    saveExpanded(expandedSet);
  }

  // --- Flatten tree into grid rows ---

  type FlatRow =
    | { kind: 'entity'; node: EntityDiff; depth: number; isRoot: boolean }
    | { kind: 'field'; fieldName: string; existing: string; pending: string; result: string; status: 'added' | 'removed' | 'changed'; depth: number; parentKey: string };

  const STORAGE_KEY = `myko:diff-expanded:${rootType}:${rootId}`;

  let expandedSet = $state(loadExpanded());

  function loadExpanded(): Set<string> {
    try {
      const stored = sessionStorage.getItem(STORAGE_KEY);
      if (stored) return new Set(JSON.parse(stored));
    } catch { /* ignore */ }
    // Default: only root expanded
    return new Set([`${rootType}:${rootId}`]);
  }

  function saveExpanded(set: Set<string>) {
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify([...set]));
    } catch { /* ignore */ }
  }

  function hasChanges(node: EntityDiff): boolean {
    if (node.status !== 'unchanged') return true;
    return node.children.some(hasChanges);
  }

  function toggleExpand(key: string) {
    const next = new Set(expandedSet);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    expandedSet = next;
    saveExpanded(next);
  }

  function formatVal(v: unknown): string {
    if (v === undefined || v === null) return '';
    if (typeof v === 'object') return JSON.stringify(v, null, 2);
    return String(v);
  }

  const rows = $derived.by((): FlatRow[] => {
    const result: FlatRow[] = [];

    const walk = (node: EntityDiff, depth: number, isRoot: boolean) => {
      const key = `${node.type}:${node.id}`;
      const visible = isRoot || showUnchanged || node.status !== 'unchanged' || hasChanges(node);
      if (!visible) return;

      result.push({ kind: 'entity', node, depth, isRoot });

      const isExpanded = expandedSet.has(key);
      const isExcluded = excluded.has(key);

      if (isExpanded && !isExcluded) {
        // Emit field rows for modified entities
        if (node.fields && node.fields.length > 0) {
          for (const f of node.fields) {
            const oldStr = formatVal(f.oldValue);
            const newStr = formatVal(f.newValue);
            const status: 'added' | 'removed' | 'changed' =
              f.oldValue === undefined ? 'added' :
              f.newValue === undefined ? 'removed' : 'changed';
            result.push({
              kind: 'field',
              fieldName: f.field,
              existing: oldStr,
              pending: newStr,
              result: status === 'removed' ? '' : newStr,
              status,
              depth: depth + 1,
              parentKey: key,
            });
          }
        }

        // Emit fields for added/removed entities (all fields as a block)
        if (!node.fields || node.fields.length === 0) {
          const data = node.status === 'removed' ? node.currentData : node.status === 'added' ? node.incomingData : null;
          if (data) {
            for (const [k, v] of Object.entries(data)) {
              if (k === 'hash') continue;
              const valStr = formatVal(v);
              result.push({
                kind: 'field',
                fieldName: k,
                existing: node.status === 'removed' ? valStr : '',
                pending: node.status === 'added' ? valStr : '',
                result: node.status === 'added' ? valStr : '',
                status: node.status === 'removed' ? 'removed' : 'added',
                depth: depth + 1,
                parentKey: key,
              });
            }
          }
        }

        // Recurse into children
        for (const child of node.children) {
          walk(child, depth + 1, false);
        }
      }
    };

    walk(tree, 0, true);
    return result;
  });
</script>

<div class="diff-grid-wrapper">
  <!-- Summary + batch controls -->
  <div class="summary-panel">
    <div class="summary-heading">Apply will:</div>
    <div class="summary-rows">
      {#each Object.entries(keysByStatus) as [status, keys]}
        {#if keys.length > 0}
          {@const activeKeys = keys.filter(k => !excluded.has(k))}
          {@const skippedKeys = keys.filter(k => excluded.has(k))}
          {@const [applyLabel, skipLabel] = actionLabels(status as DiffStatus)}
          {@const allExcluded = activeKeys.length === 0}
          {@const noneExcluded = skippedKeys.length === 0}
          {@const statusColor = status === 'removed' ? 'text-error' : status === 'added' ? 'text-success' : 'text-warning'}
          <div class="summary-row">
            <span class="summary-action {statusColor}">{applyLabel} {activeKeys.length}</span>
            {#if skippedKeys.length > 0}
              <span class="summary-skip">{skipLabel} {skippedKeys.length}</span>
            {/if}
            <span class="summary-total">of {keys.length}</span>
            <div class="join summary-btns">
              <button class="join-item btn btn-xs" class:btn-active={noneExcluded} onclick={() => batchApply(keys)}>{applyLabel} all</button>
              <button class="join-item btn btn-xs" class:btn-active={allExcluded} onclick={() => batchSkip(keys)}>{skipLabel} all</button>
            </div>
            <div class="dropdown dropdown-bottom dropdown-end">
              <div tabindex="0" role="button" class="btn btn-xs btn-ghost opacity-50">by type</div>
              <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
              <div tabindex="0" class="dropdown-content menu bg-base-200 rounded-lg shadow-lg z-10 p-2 w-64">
                {#each Object.entries(keysByEntityType).sort(([a], [b]) => a.localeCompare(b)) as [entityType, etKeys]}
                  {@const matching = etKeys.filter(k => keys.includes(k))}
                  {#if matching.length > 0}
                    {@const etAllExcluded = matching.every(k => excluded.has(k))}
                    {@const etNoneExcluded = matching.every(k => !excluded.has(k))}
                    <div class="batch-type-row">
                      <span class="batch-type-name">{entityType} <span class="opacity-40">({matching.length})</span></span>
                      <div class="join">
                        <button class="join-item btn btn-xs" class:btn-active={etNoneExcluded} onclick={() => batchApply(matching)}>apply</button>
                        <button class="join-item btn btn-xs" class:btn-active={etAllExcluded} onclick={() => batchSkip(matching)}>skip</button>
                      </div>
                    </div>
                  {/if}
                {/each}
              </div>
            </div>
          </div>
        {/if}
      {/each}
    </div>
  </div>

  <!-- Column headers -->
  <div class="grid-header">
    <span class="col-head col-tree"></span>
    <span class="col-head">Existing</span>
    <span class="col-head">Pending</span>
    <span class="col-head">Result</span>
    <span class="col-head col-action"></span>
  </div>

  <!-- Grid rows -->
  <div class="grid-body">
    {#each rows as row, i (row.kind === 'entity' ? `${row.node.type}:${row.node.id}` : `${row.parentKey}:${row.fieldName}`)}
      {#if row.kind === 'entity'}
        {@const key = `${row.node.type}:${row.node.id}`}
        {@const isExpanded = expandedSet.has(key)}
        {@const isExcluded = excluded.has(key)}
        {@const canExclude = !row.isRoot && (row.node.status === 'modified' || row.node.status === 'added' || row.node.status === 'removed')}
        {@const hasDetails = (row.node.fields && row.node.fields.length > 0) || row.node.children.length > 0 || !!row.node.currentData || !!row.node.incomingData}

        {@const willDelete = !isExcluded && row.node.status === 'removed'}
        {@const willAdd = !isExcluded && row.node.status === 'added'}
        {@const willUpdate = !isExcluded && row.node.status === 'modified'}
        {@const willSkip = isExcluded && row.node.status !== 'unchanged'}
        <div
          class="grid-row entity-row"
          class:outcome-delete={willDelete}
          class:outcome-add={willAdd}
          class:outcome-update={willUpdate}
          class:outcome-skip={willSkip}
        >
          <!-- Tree column -->
          <div class="cell cell-tree" style:padding-left="{row.depth * 1.25}rem">
            <button
              class="tree-toggle"
              onclick={() => toggleExpand(key)}
              disabled={!hasDetails}
            >
              {#if hasDetails}
                <span class="chevron">{isExpanded ? '▼' : '▶'}</span>
              {:else}
                <span class="chevron-spacer"></span>
              {/if}
              <span class="entity-type">{row.node.type}</span>
              <span class="entity-name">{row.node.name ?? row.node.id}</span>
              {#if row.node.status === 'unchanged'}
                <EntityDiffBadge status={row.node.status} />
              {/if}
            </button>
          </div>

          <!-- Value columns — empty for entity header rows, fields show below -->
          <div class="cell cell-val"></div>
          <div class="cell cell-val"></div>
          <div class="cell cell-val"></div>

          <!-- Action column -->
          <div class="cell cell-action">
            {#if canExclude}
              {@const [applyLabel, skipLabel] = actionLabels(row.node.status)}
              <div class="join">
                <button
                  class="join-item btn btn-xs {!isExcluded ? (row.node.status === 'removed' ? 'btn-error' : row.node.status === 'added' ? 'btn-success' : row.node.status === 'modified' ? 'btn-warning' : '') : ''}"
                  class:btn-active={!isExcluded}
                  onclick={(e) => { e.stopPropagation(); if (isExcluded) toggleExclude(key); }}
                >{applyLabel}</button>
                <button
                  class="join-item btn btn-xs"
                  class:btn-active={isExcluded}
                  onclick={(e) => { e.stopPropagation(); if (!isExcluded) toggleExclude(key); }}
                >{skipLabel}</button>
              </div>
            {/if}
          </div>
        </div>

      {:else}
        <!-- Field row -->
        <div
          class="grid-row field-row"
          class:field-added={row.status === 'added'}
          class:field-removed={row.status === 'removed'}
          class:field-changed={row.status === 'changed'}
        >
          <div class="cell cell-tree cell-field-name" style:padding-left="{row.depth * 1.25 + 1.25}rem">
            {row.fieldName}
          </div>
          <div class="cell cell-val cell-existing"><pre class="val">{row.existing}</pre></div>
          <div class="cell cell-val cell-pending"><pre class="val">{row.pending}</pre></div>
          <div class="cell cell-val cell-result"><pre class="val">{row.result}</pre></div>
          <div class="cell cell-action"></div>
        </div>
      {/if}
    {/each}
  </div>
</div>

<style>
  .diff-grid-wrapper {
    font-size: var(--rs-fs-xs, 0.75rem);
  }

  .summary-panel {
    margin-bottom: 0.75rem;
    padding: 0.75rem;
    background: rgba(255, 255, 255, 0.03);
    border-radius: 0.5rem;
  }

  .summary-heading {
    font-size: 0.65rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    opacity: 0.4;
    margin-bottom: 0.5rem;
  }

  .summary-rows {
    display: flex;
    flex-direction: column;
    gap: 0.375rem;
  }

  .summary-row {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    font-size: var(--rs-fs-sm, 0.875rem);
  }

  .summary-action {
    font-weight: 600;
    min-width: 6rem;
  }

  .summary-skip {
    opacity: 0.4;
    min-width: 5rem;
  }

  .summary-total {
    opacity: 0.3;
    font-size: var(--rs-fs-xs, 0.75rem);
    min-width: 3rem;
  }

  .summary-btns {
    margin-left: auto;
  }

  .batch-type-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.5rem;
    padding: 0.25rem 0.5rem;
    border-radius: 0.25rem;
  }

  .batch-type-row:hover {
    background: rgba(255, 255, 255, 0.05);
  }

  .batch-type-name {
    font-size: var(--rs-fs-xs, 0.75rem);
    white-space: nowrap;
  }

  /* --- Grid layout --- */
  .grid-header {
    display: grid;
    grid-template-columns: minmax(250px, 1.5fr) 1fr 1fr 1fr auto;
    gap: 0;
    padding: 0 0.5rem;
    border-bottom: 1px solid oklch(var(--bc) / 0.1);
    margin-bottom: 0.25rem;
  }

  .col-head {
    font-size: 0.65rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    opacity: 0.4;
    padding: 0.25rem 0.75rem 0.375rem;
    white-space: nowrap;
  }

  .col-head.col-tree,
  .col-head.col-action {
    opacity: 0;
  }

  .grid-body {
    display: flex;
    flex-direction: column;
  }

  .grid-row {
    display: grid;
    grid-template-columns: minmax(250px, 1.5fr) 1fr 1fr 1fr auto;
    gap: 0;
    border-radius: 0.25rem;
    min-height: 1.75rem;
    align-items: start;
  }

  .entity-row {
    align-items: center;
    background: rgba(255, 255, 255, 0.03);
  }

  .field-row {
    background: rgba(255, 255, 255, 0.02);
  }

  .grid-row:hover {
    background: rgba(255, 255, 255, 0.06);
  }

  /* --- Outcome-based row colors (stoplight: green=add, yellow=update, red=delete) --- */
  .entity-row.outcome-add {
    background: oklch(var(--su) / 0.06);
  }

  .entity-row.outcome-update {
    background: oklch(var(--wa) / 0.04);
  }

  .entity-row.outcome-delete {
    background: oklch(var(--er) / 0.06);
  }

  .entity-row.outcome-skip {
    opacity: 0.4;
  }

  .field-row.field-added {
    background: oklch(var(--su) / 0.05);
  }

  .field-row.field-removed {
    background: oklch(var(--er) / 0.05);
  }

  .field-row.field-changed {
    background: oklch(var(--wa) / 0.05);
  }

  /* --- Cells --- */
  .cell {
    padding: 0.25rem 0.75rem;
    font-size: inherit;
  }

  .cell-tree {
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .cell-val {
    font-family: monospace;
    min-width: 0;
  }

  .val {
    margin: 0;
    font-family: inherit;
    font-size: inherit;
    white-space: pre-wrap;
    word-break: break-word;
  }

  .tree-toggle {
    display: inline-flex;
    align-items: center;
    gap: 0.375rem;
    background: none;
    border: none;
    padding: 0;
    cursor: pointer;
    text-align: left;
    color: inherit;
    font-size: inherit;
    width: 100%;
  }

  .tree-toggle:disabled {
    cursor: default;
  }

  .chevron {
    font-size: 0.6rem;
    width: 0.75rem;
    flex-shrink: 0;
    opacity: 0.5;
  }

  .chevron-spacer {
    width: 0.75rem;
    flex-shrink: 0;
  }

  .entity-type {
    opacity: 0.45;
    font-size: 0.65rem;
    flex-shrink: 0;
  }

  .entity-name {
    font-weight: 500;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .cell-field-name {
    opacity: 0.6;
    font-family: monospace;
  }

  /* --- Value column colors (field rows) --- */
  .field-removed .cell-existing {
    color: oklch(var(--er));
  }

  .field-added .cell-pending,
  .field-added .cell-result {
    color: oklch(var(--su));
  }

  .field-changed .cell-existing {
    color: oklch(var(--er) / 0.8);
  }

  .field-changed .cell-pending,
  .field-changed .cell-result {
    color: oklch(var(--wa));
  }

  /* --- Action button --- */
  .cell-action {
    font-family: inherit;
    padding: 0.25rem 0.5rem;
    white-space: nowrap;
  }



</style>
