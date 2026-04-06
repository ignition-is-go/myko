<script lang="ts" generics="Q extends Query<unknown>">
	import type { Query, QueryItem } from '@myko/core';
	import { getMykoClient, type SvelteMykoClient } from '../services/svelte-client.svelte.js';
	import type { Snippet } from 'svelte';

	interface Props {
		query: Q;
		client?: SvelteMykoClient;
		children: Snippet<[QueryItem<Q> & { id: string }]>;
		empty?: Snippet;
		loading?: Snippet;
	}

	let { query, client, children, empty, loading }: Props = $props();

	const resolvedClient = client ?? getMykoClient();
	const result = resolvedClient.liveQuery(() => query);
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
	{#each result.items as [id, item] (id)}
		{@render children(item)}
	{/each}
{/if}
