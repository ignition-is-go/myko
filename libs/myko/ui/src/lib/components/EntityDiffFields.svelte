<script lang="ts">
  import type { FieldDiff } from '../utils/entity-diff.js';

  interface Props {
    fields: FieldDiff[];
  }

  let { fields }: Props = $props();

  function formatValue(v: unknown): string {
    if (v === undefined || v === null) return '(none)';
    if (typeof v === 'string') return v;
    return JSON.stringify(v);
  }
</script>

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
