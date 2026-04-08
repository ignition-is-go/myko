<script lang="ts">
  import type { FieldDiff } from '../utils/entity-diff.js';

  interface Props {
    fields: FieldDiff[];
    /** Full current entity data (for side-by-side view). */
    currentData?: Record<string, unknown>;
    /** Full incoming entity data (for side-by-side view). */
    incomingData?: Record<string, unknown>;
  }

  let { fields, currentData, incomingData }: Props = $props();

  const hasSideBySide = $derived(!!currentData && !!incomingData);

  function formatValue(v: unknown): string {
    if (v === undefined || v === null) return '';
    if (typeof v === 'object') return JSON.stringify(v, null, 2);
    return String(v);
  }

  function isObject(v: unknown): v is Record<string, unknown> {
    return typeof v === 'object' && v !== null && !Array.isArray(v);
  }

  interface DiffLine {
    key: string;
    status: 'added' | 'removed' | 'changed' | 'unchanged';
    oldVal?: string;
    newVal?: string;
  }

  function computeJsonDiff(oldVal: unknown, newVal: unknown): DiffLine[] | null {
    if (!isObject(oldVal) || !isObject(newVal)) return null;
    const allKeys = new Set([...Object.keys(oldVal), ...Object.keys(newVal)]);
    const lines: DiffLine[] = [];
    for (const key of allKeys) {
      const o = oldVal[key];
      const n = newVal[key];
      if (!(key in newVal)) {
        lines.push({ key, status: 'removed', oldVal: JSON.stringify(o) });
      } else if (!(key in oldVal)) {
        lines.push({ key, status: 'added', newVal: JSON.stringify(n) });
      } else if (JSON.stringify(o) !== JSON.stringify(n)) {
        lines.push({ key, status: 'changed', oldVal: JSON.stringify(o), newVal: JSON.stringify(n) });
      } else {
        lines.push({ key, status: 'unchanged', oldVal: JSON.stringify(o), newVal: JSON.stringify(n) });
      }
    }
    return lines;
  }
</script>

{#if hasSideBySide}
  <div class="diff-fields">
    {#each fields as field}
      {@const jsonDiff = computeJsonDiff(field.oldValue, field.newValue)}
      <div class="field-section">
        <span class="field-name">{field.field}</span>
        {#if jsonDiff}
          <div class="diff-table json-layout">
            <div class="diff-header json-layout">
              <span class="col-label col-key"></span>
              <span class="col-label">existing</span>
              <span class="col-label">pending</span>
              <span class="col-label">result</span>
            </div>
            {#each jsonDiff as line}
              <div
                class="diff-row json-layout"
                class:row-added={line.status === 'added'}
                class:row-removed={line.status === 'removed'}
                class:row-changed={line.status === 'changed'}
              >
                <span class="cell cell-key">{line.key}</span>
                <span class="cell cell-existing">{line.status === 'added' ? '' : line.oldVal}</span>
                <span class="cell cell-pending">{line.status === 'removed' ? '' : line.newVal}</span>
                <span class="cell cell-result">{line.status === 'removed' ? '' : (line.newVal ?? line.oldVal)}</span>
              </div>
            {/each}
          </div>
        {:else}
          <div class="diff-table scalar-layout">
            <div class="diff-header scalar-layout">
              <span class="col-label">existing</span>
              <span class="col-label">pending</span>
              <span class="col-label">result</span>
            </div>
            <div
              class="diff-row scalar-layout"
              class:row-changed={field.oldValue !== undefined && field.newValue !== undefined}
              class:row-added={field.oldValue === undefined}
              class:row-removed={field.newValue === undefined}
            >
              <span class="cell cell-existing">{formatValue(field.oldValue)}</span>
              <span class="cell cell-pending">{formatValue(field.newValue)}</span>
              <span class="cell cell-result">{formatValue(field.newValue)}</span>
            </div>
          </div>
        {/if}
      </div>
    {/each}
  </div>
{:else}
  <div class="pl-6 text-sm space-y-1">
    {#each fields as field}
      <div class="font-mono">
        <span class="text-base-content/60">{field.field}:</span>
        {#if field.oldValue === undefined}
          <span class="text-success">+ {formatValue(field.newValue)}</span>
        {:else if field.newValue === undefined}
          <span class="text-error">- {formatValue(field.oldValue)}</span>
        {:else}
          <span class="text-error">{formatValue(field.oldValue)}</span>
          <span class="text-base-content/40">&rarr;</span>
          <span class="text-success">{formatValue(field.newValue)}</span>
        {/if}
      </div>
    {/each}
  </div>
{/if}

<style>
  .diff-fields {
    margin-left: 1.5rem;
    font-size: var(--rs-fs-xs, 0.75rem);
    font-family: monospace;
    overflow-x: auto;
  }

  .field-section {
    padding: 0.5rem 0;
    border-bottom: 1px solid oklch(var(--bc) / 0.05);
  }

  .field-section:last-child {
    border-bottom: none;
  }

  .field-name {
    display: block;
    opacity: 0.5;
    font-size: 0.65rem;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    margin-bottom: 0.375rem;
    font-family: sans-serif;
  }

  /* --- Diff table --- */
  .diff-table {
    min-width: max-content;
  }

  .diff-header {
    display: grid;
    gap: 1.5rem;
    padding: 0 0.5rem 0.25rem;
  }

  .col-label {
    font-size: 0.6rem;
    opacity: 0.35;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  .col-label.col-key {
    opacity: 0;
  }

  .diff-row {
    display: grid;
    gap: 1.5rem;
    padding: 0.25rem 0.5rem;
    border-radius: 0.25rem;
  }

  /* Grid layouts */
  .json-layout {
    grid-template-columns: minmax(100px, max-content) 1fr 1fr 1fr;
  }

  .scalar-layout {
    grid-template-columns: 1fr 1fr 1fr;
  }

  /* Row backgrounds */
  .diff-row.row-added {
    background: oklch(var(--su) / 0.08);
  }

  .diff-row.row-removed {
    background: oklch(var(--er) / 0.08);
  }

  .diff-row.row-changed {
    background: oklch(var(--wa) / 0.08);
  }

  /* Cells */
  .cell {
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .cell-key {
    opacity: 0.6;
  }

  .cell-existing {
    color: oklch(var(--bc) / 0.5);
  }

  .cell-pending {
    color: oklch(var(--bc) / 0.8);
  }

  .cell-result {
    color: oklch(var(--bc) / 0.6);
  }

  /* Row-specific highlight colors */
  .row-removed .cell-existing {
    color: oklch(var(--er));
  }

  .row-added .cell-pending,
  .row-added .cell-result {
    color: oklch(var(--su));
  }

  .row-changed .cell-existing {
    color: oklch(var(--er));
  }

  .row-changed .cell-pending {
    color: oklch(var(--wa));
  }

  .row-changed .cell-result {
    color: oklch(var(--wa));
  }
</style>
