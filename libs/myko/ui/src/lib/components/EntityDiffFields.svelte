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
    if (v === undefined || v === null) return '—';
    if (typeof v === 'object') return JSON.stringify(v, null, 2);
    return String(v);
  }
</script>

{#if hasSideBySide}
  <div class="side-by-side">
    {#each fields as field}
      <div class="field-row">
        <span class="field-name">{field.field}</span>
        <div class="field-values">
          <div class="val-col">
            <span class="col-label">current</span>
            <pre class="val val-old">{formatValue(field.oldValue)}</pre>
          </div>
          <div class="val-col">
            <span class="col-label">incoming</span>
            <pre class="val val-new">{formatValue(field.newValue)}</pre>
          </div>
        </div>
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
  .side-by-side {
    margin-left: 1.5rem;
    font-size: var(--rs-fs-xs, 0.75rem);
  }

  .field-row {
    padding: 0.375rem 0;
    border-bottom: 1px solid oklch(var(--bc) / 0.05);
  }

  .field-row:last-child {
    border-bottom: none;
  }

  .field-name {
    display: block;
    opacity: 0.5;
    font-size: 0.65rem;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    margin-bottom: 0.25rem;
  }

  .field-values {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.5rem;
  }

  .val-col {
    min-width: 0;
  }

  .col-label {
    display: block;
    font-size: 0.6rem;
    opacity: 0.35;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 0.125rem;
  }

  .val {
    margin: 0;
    padding: 0.25rem 0.5rem;
    border-radius: 0.25rem;
    white-space: pre-wrap;
    word-break: break-word;
    font-family: monospace;
    font-size: inherit;
    overflow-x: auto;
  }

  .val-old {
    background: oklch(var(--er) / 0.1);
    color: oklch(var(--er));
  }

  .val-new {
    background: oklch(var(--su) / 0.1);
    color: oklch(var(--su));
  }
</style>
