<script lang="ts">
	import { myko as client } from '../../services/svelte-client.svelte.js';
	import { EventsForTransaction, type ID } from '@myko/ts';
	import { fromISOMemo, FULL_DATE_FORMAT } from '../state/viewstate.svelte.js';

	const { tx }: { tx: ID } = $props();
	const events = client.watchReport(new EventsForTransaction({ transactionId: tx }));
</script>

{#if $events}
	{#each $events as event (event)}
		<div class="div">
			<p>{event.itemType}</p>
			<p>{event.changeType}</p>
			<p>{fromISOMemo(event.createdAt).diffNow().toHuman()}</p>
			<p>{fromISOMemo(event.createdAt).toFormat(FULL_DATE_FORMAT)}</p>
		</div>
	{/each}
{/if}

<style>
	.div {
		display: flex;
		gap: 1rem;
	}
</style>
