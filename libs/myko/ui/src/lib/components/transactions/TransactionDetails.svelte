<script lang="ts">
	import { client } from '$lib/services/client.js';
	import { EventsForTransaction, type ID } from '@myko/core';
	import { DateTime } from 'luxon';
	import { FULL_DATE_FORMAT } from '../state/viewstate.svelte.js';

	const { tx }: { tx: ID } = $props();
	const events = client.watchReport(new EventsForTransaction(tx));
</script>

{#if $events}
	{#each $events as event (event)}
		<div class="div">
			<p>{event.itemType}</p>
			<p>{event.changeType}</p>
			<p>{DateTime.fromISO(event.createdAt).diffNow().toHuman()}</p>
			<p>{DateTime.fromISO(event.createdAt).toFormat(FULL_DATE_FORMAT)}</p>
		</div>
	{/each}
{/if}

<style>
	.div {
		display: flex;
		gap: 1rem;
	}
</style>
