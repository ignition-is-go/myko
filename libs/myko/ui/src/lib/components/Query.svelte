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
	}

	let { query, client, children, empty }: Props = $props();

	const resolvedClient = client ?? getMykoClient();
	const result = resolvedClient.query(query);

	onDestroy(() => {
		result.release();
	});
</script>

{#if result.items.size === 0 && empty}
	{@render empty()}
{:else}
	{#each [...result.items.values()] as item (item.id)}
		{@render children(item)}
	{/each}
{/if}
