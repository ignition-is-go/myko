<script lang="ts">
  import type { DiffStatus } from '../utils/entity-diff.js';

  interface Props {
    status: DiffStatus;
  }

  let { status }: Props = $props();

  const config: Record<DiffStatus, { label: string; class: string }> = {
    added: { label: 'Added', class: 'badge-success' },
    removed: { label: 'Removed', class: 'badge-error' },
    modified: { label: 'Modified', class: 'badge-warning' },
    unchanged: { label: 'Unchanged', class: 'badge-ghost' },
  };
</script>

<span class="badge badge-sm {config[status].class}">{config[status].label}</span>
