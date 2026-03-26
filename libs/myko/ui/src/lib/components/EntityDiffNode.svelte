<script lang="ts">
  import type { EntityDiff } from '../utils/entity-diff.js';
  import EntityDiffBadge from './EntityDiffBadge.svelte';
  import EntityDiffFields from './EntityDiffFields.svelte';

  interface Props {
    node: EntityDiff;
    showUnchanged?: boolean;
    /** Set of "Type:id" keys excluded from import. */
    excluded?: Set<string>;
    /** Callback to toggle an entity's excluded state. */
    onToggleExclude?: (key: string) => void;
    /** Whether this is the root node (always visible). */
    isRoot?: boolean;
  }

  let { node, showUnchanged = false, excluded, onToggleExclude, isRoot = false }: Props = $props();
  let expanded = $state(node.status !== 'unchanged' || isRoot);

  const key = $derived(`${node.type}:${node.id}`);
  const isExcluded = $derived(excluded?.has(key) ?? false);
  const canExclude = $derived(
    !isRoot && (node.status === 'modified' || node.status === 'added' || node.status === 'removed'),
  );

  const toggleExclude = (e: Event) => {
    e.stopPropagation();
    onToggleExclude?.(key);
  };

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
  <div class="border-l-2 border-base-300 pl-3 py-1" class:opacity-40={isExcluded}>
    <div class="flex items-center gap-1">
      <button
        class="flex items-center gap-2 flex-1 text-left hover:bg-base-200 rounded px-1"
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
        {#if isExcluded}
          <span class="badge badge-sm badge-ghost">keeping original</span>
        {/if}
      </button>
      {#if canExclude && onToggleExclude}
        <button
          class="btn btn-xs btn-ghost text-xs opacity-60 hover:opacity-100"
          onclick={toggleExclude}
          title={isExcluded ? 'Accept incoming version' : 'Keep original version'}
        >
          {isExcluded ? 'overwrite' : 'keep original'}
        </button>
      {/if}
    </div>

    {#if expanded && !isExcluded}
      {#if node.fields && node.fields.length > 0}
        <EntityDiffFields
          fields={node.fields}
          currentData={node.currentData}
          incomingData={node.incomingData}
        />
      {/if}

      {#if node.children.length > 0}
        <div class="pl-2">
          {#each node.children as child (child.type + ':' + child.id)}
            <svelte:self node={child} {showUnchanged} {excluded} {onToggleExclude} />
          {/each}
        </div>
      {/if}
    {/if}
  </div>
{/if}
