<script lang="ts">
  import type { EntityDiff } from '../utils/entity-diff.js';
  import EntityDiffBadge from './EntityDiffBadge.svelte';
  import EntityDiffFields from './EntityDiffFields.svelte';

  interface Props {
    node: EntityDiff;
    showUnchanged?: boolean;
  }

  let { node, showUnchanged = false }: Props = $props();
  let expanded = $state(node.status === 'modified');

  const visible = $derived(showUnchanged || node.status !== 'unchanged');
  const hasDetails = $derived(
    (node.fields && node.fields.length > 0) || node.children.length > 0,
  );
</script>

{#if visible}
  <div class="border-l-2 border-base-300 pl-3 py-1">
    <button
      class="flex items-center gap-2 w-full text-left hover:bg-base-200 rounded px-1"
      onclick={() => (expanded = !expanded)}
      disabled={!hasDetails}
    >
      {#if hasDetails}
        <span class="text-xs">{expanded ? '▼' : '▶'}</span>
      {:else}
        <span class="text-xs w-3"></span>
      {/if}
      <span class="text-xs text-base-content/50">{node.type}</span>
      <span class="font-medium">{node.name ?? node.id}</span>
      <EntityDiffBadge status={node.status} />
    </button>

    {#if expanded}
      {#if node.fields && node.fields.length > 0}
        <EntityDiffFields fields={node.fields} />
      {/if}

      {#if node.children.length > 0}
        <div class="pl-2">
          {#each node.children as child}
            <svelte:self node={child} {showUnchanged} />
          {/each}
        </div>
      {/if}
    {/if}
  </div>
{/if}
