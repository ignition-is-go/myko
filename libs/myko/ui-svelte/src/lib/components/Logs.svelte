<script lang="ts">
	import { startWith } from 'rxjs';
	import { myko as client } from '../services/svelte-client.svelte.js';
	import { Loggers, type ID } from '@myko/ts';

	type Props = {
		serverId: ID;
	};

	const { serverId }: Props = $props();

	const logs = client.watchReport(new Loggers({})).pipe(startWith([]));
</script>

{#each $logs as log}
	<div class="log">
		<input type="checkbox" />
		<span>
			{log}
		</span>
	</div>
{/each}

<style>
	.log {
		display: flex;
		align-items: center;
	}

	.log input {
		margin-right: 0.5rem;
	}

	.log span {
		flex: 1;
	}
</style>
