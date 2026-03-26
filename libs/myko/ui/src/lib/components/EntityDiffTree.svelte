<script lang="ts">
  import type { ExportedEntity } from '../utils/entity-diff.js';
  import { diffEntityLists } from '../utils/entity-diff.js';
  import type { ParentFkFields } from '../utils/entity-tree.js';
  import { buildDiffTree } from '../utils/entity-tree.js';
  import EntityDiffNode from './EntityDiffNode.svelte';

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

  const stats = $derived.by(() => {
    let added = 0, removed = 0, modified = 0, unchanged = 0, skipped = 0;
    for (const [key, d] of diffs) {
      if (excluded.has(key)) { skipped++; continue; }
      if (d.status === 'added') added++;
      else if (d.status === 'removed') removed++;
      else if (d.status === 'modified') modified++;
      else unchanged++;
    }
    return { added, removed, modified, unchanged, skipped };
  });
</script>

<div class="space-y-3">
  <div class="flex gap-3 text-sm">
    {#if stats.added > 0}
      <span class="text-success">{stats.added} added</span>
    {/if}
    {#if stats.removed > 0}
      <span class="text-error">{stats.removed} removed</span>
    {/if}
    {#if stats.modified > 0}
      <span class="text-warning">{stats.modified} modified</span>
    {/if}
    {#if stats.skipped > 0}
      <span class="text-base-content/60">{stats.skipped} keeping original</span>
    {/if}
    <span class="text-base-content/40">{stats.unchanged} unchanged</span>
  </div>

  <EntityDiffNode node={tree} {showUnchanged} {excluded} onToggleExclude={toggleExclude} isRoot />
</div>
