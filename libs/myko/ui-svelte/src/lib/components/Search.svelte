<script lang="ts" generics="T extends { id: string }">
	import { EntitySearch, type Query } from '@myko/ts';
	import { getMykoClient, type SvelteMykoClient } from '../services/svelte-client.svelte.js';
	import type { Snippet } from 'svelte';

	interface Props {
		/** Entity type to search (e.g., "Target", "Scene") */
		entityType: string;
		/** Search query string */
		query: string;
		/** Maximum number of results (default: 100) */
		limit?: number;
		/** Query factory to fetch items by IDs - receives array of matching IDs */
		queryByIds: (ids: string[]) => Query<T>;
		/** Query to use when search query is empty (shows all items) */
		showAllOnEmpty?: Query<T>;
		/** Optional client instance */
		client?: SvelteMykoClient;
		/** Render snippet for each result item */
		children: Snippet<[T]>;
		/** Render snippet when no results found */
		empty?: Snippet;
		/** Render snippet while loading */
		loading?: Snippet;
	}

	let {
		entityType,
		query,
		limit = 100,
		queryByIds,
		showAllOnEmpty,
		client,
		children,
		empty,
		loading
	}: Props = $props();

	const resolvedClient = client ?? getMykoClient();

	// Subscribe to EntitySearch report when we have a query
	const searchResult = resolvedClient.liveReport(() =>
		query?.trim() ? new EntitySearch({ entityType, query, limit }) : null,
	);

	// Get the matching IDs from search result
	const searchIds = $derived(searchResult.value?.ids ?? []);

	// Subscribe to items - either by search IDs, showAllOnEmpty, or nothing
	const itemsResult = resolvedClient.liveQuery(() => {
		if (query?.trim()) {
			// Have a search query - fetch by search result IDs
			return searchIds.length > 0 ? queryByIds(searchIds) : null;
		} else if (showAllOnEmpty) {
			// Empty query with showAllOnEmpty - fetch all
			return showAllOnEmpty;
		}
		return null;
	});

	// Derived loading state
	const isLoading = $derived(
		query?.trim()
			? !searchResult.value || (searchIds.length > 0 && !itemsResult.resolved)
			: showAllOnEmpty
				? !itemsResult.resolved
				: false,
	);
</script>

{#if isLoading}
	{#if loading}
		{@render loading()}
	{:else}
		<span class="search-loading">Searching...</span>
	{/if}
{:else if itemsResult.items.size === 0}
	{#if empty}
		{@render empty()}
	{/if}
{:else}
	{#each [...itemsResult.items.values()] as item (item.id)}
		{@render children(item)}
	{/each}
{/if}
