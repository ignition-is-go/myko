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
    /** Whether to show unchanged entities */
    showUnchanged?: boolean;
  }

  let { rootType, rootId, current, incoming, parentFkFields, showUnchanged = false }: Props = $props();

  const diffs = $derived(diffEntityLists(current, incoming));
  const tree = $derived(buildDiffTree(rootType, rootId, diffs, parentFkFields));

  const stats = $derived.by(() => {
    let added = 0, removed = 0, modified = 0, unchanged = 0;
    for (const d of diffs.values()) {
      if (d.status === 'added') added++;
      else if (d.status === 'removed') removed++;
      else if (d.status === 'modified') modified++;
      else unchanged++;
    }
    return { added, removed, modified, unchanged };
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
    <span class="text-base-content/40">{stats.unchanged} unchanged</span>
  </div>

  <EntityDiffNode node={tree} {showUnchanged} isRoot />
</div>
