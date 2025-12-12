<script lang="ts">
	import type { ID } from '@myko/core';
	import TransactionDetails from './TransactionDetails.svelte';

	let open = $state(false);

	const { tx }: { tx: ID } = $props();
</script>

<div
	class="transaction"
	role="presentation"
	onclick={() => {
		open = !open;
	}}
>
	<span>
		{tx}
	</span>

	{#if open}
		<TransactionDetails {tx}></TransactionDetails>
	{/if}
</div>
