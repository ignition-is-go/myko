<script lang="ts">
  import type { EntityDiff } from '../utils/entity-diff.js';
  import EntityDiffBadge from './EntityDiffBadge.svelte';
  import EntityDiffFields from './EntityDiffFields.svelte';

  interface Props {
    node: EntityDiff;
    showUnchanged?: boolean;
    /** Whether this is the root node (always visible). */
    isRoot?: boolean;
  }

  let { node, showUnchanged = false, isRoot = false }: Props = $props();
  let expanded = $state(node.status !== 'unchanged' || isRoot);

  const hasChangedDescendant = $derived.by((): boolean => {
    const check = (n: EntityDiff): boolean => {
      if (n.status !== 'unchanged') return true;
      return n.children.some(check);
    };
    return node.children.some(check);
  });

  const visible = $derived(
    isRoot || showUnchanged || node.status !== 'unchanged' || hasChangedDescendant,
  );
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
          {#each node.children as child (child.type + ':' + child.id)}
            <svelte:self node={child} {showUnchanged} />
          {/each}
        </div>
      {/if}
    {/if}
  </div>
{/if}
