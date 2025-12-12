<script lang="ts" generics="Q extends QueryReturn<unknown>">
	import type { QueryReturn, QueryItem } from '@myko/ts';
	import { getMykoClient, type SvelteMykoClient } from '../services/svelte-client.svelte.js';
	import type { Snippet } from 'svelte';
	import { onDestroy } from 'svelte';

	type Item = QueryItem<Q> & { id: string };

	interface Props {
		query: Q;
		client?: SvelteMykoClient;
		children: Snippet<[Item]>;
		empty?: Snippet;
		loading?: Snippet;
	}

	let { query, client, children, empty, loading }: Props = $props();

	const resolvedClient = client ?? getMykoClient();
	const result = resolvedClient.query(query);

	onDestroy(() => {
		result.release();
	});
</script>

{#if result.items.size === 0}
	{#if !result.resolved}
		{#if loading}
			{@render loading()}
		{:else}
			<span class="query-loading">Loading...</span>
		{/if}
	{:else if empty}
		{@render empty()}
	{/if}
{:else}
	{#each [...result.items.values()] as item (item.id)}
		{@render children(item)}
	{/each}
{/if}
