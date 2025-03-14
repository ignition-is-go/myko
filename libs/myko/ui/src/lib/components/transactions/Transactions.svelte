<script lang="ts">
	import { client } from '$lib/services/client.js';
	import { TransactionsInRange } from '@myko/core';
	import { DateTime } from 'luxon';
	import TransactonView from './TransactonView.svelte';

	const trx = client.watchReport(
		new TransactionsInRange([], DateTime.utc().minus({ days: 1 }).toISO())
	);
</script>

{#if $trx === undefined}
	<p>Loading...</p>
{:else if $trx.length === 0}
	<p>No transactions</p>
{:else}
	{#each $trx as tx (tx)}
		<TransactonView {tx}></TransactonView>
	{/each}
{/if}
