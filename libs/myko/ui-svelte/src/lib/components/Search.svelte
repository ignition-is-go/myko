<script lang="ts" generics="T extends { id: string }">
	import type { QueryReturn, ReportReturn, EntitySearchResult } from '@myko/ts';
	import {
		getMykoClient,
		type ReactiveReport,
		type ReactiveQuery,
		type SvelteMykoClient
	} from '../services/svelte-client.svelte.js';
	import type { Snippet } from 'svelte';
	import { onDestroy } from 'svelte';

	interface Props {
		/** Entity type to search (e.g., "Target", "Scene") */
		entityType: string;
		/** Search query string */
		query: string;
		/** Maximum number of results (default: 100) */
		limit?: number;
		/** Query factory to fetch items by IDs - receives array of matching IDs */
		queryByIds: (ids: string[]) => QueryReturn<T[]>;
		/** Query to use when search query is empty (shows all items) */
		showAllOnEmpty?: QueryReturn<T[]>;
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

	// Build EntitySearch report factory
	function buildSearchReport(q: string): ReportReturn<EntitySearchResult> {
		return {
			report: { entityType, query: q, limit },
			reportId: 'EntitySearch'
		} as ReportReturn<EntitySearchResult>;
	}

	// Track current subscriptions for cleanup
	let currentSearchReport: ReactiveReport<ReportReturn<EntitySearchResult>> | null = null;
	let currentItemsQuery: ReactiveQuery<QueryReturn<T[]>> | null = null;

	// Reactive search results
	let searchResult = $state<EntitySearchResult | undefined>(undefined);
	let items = $state<Map<string, T>>(new Map());
	let isLoading = $state(true);

	// Effect to update search when query changes
	$effect(() => {
		// Clean up previous subscriptions
		currentSearchReport?.release();
		currentItemsQuery?.release();

		if (!query || query.trim() === '') {
			// Empty query - use showAllOnEmpty if provided
			if (showAllOnEmpty) {
				isLoading = true;
				currentItemsQuery = resolvedClient.query(showAllOnEmpty);

				$effect(() => {
					if (currentItemsQuery) {
						items = new Map(currentItemsQuery.items);
						isLoading = !currentItemsQuery.resolved;
					}
				});
			} else {
				searchResult = { ids: [] };
				items = new Map();
				isLoading = false;
			}
			return;
		}

		isLoading = true;

		// Subscribe to EntitySearch report
		const report = buildSearchReport(query);
		currentSearchReport = resolvedClient.report(report);

		// Watch the search result and update items query
		$effect(() => {
			const result = currentSearchReport?.value;
			if (result) {
				searchResult = result;

				// Clean up previous items query
				currentItemsQuery?.release();

				if (result.ids.length > 0) {
					// Fetch actual items by IDs
					const itemsQueryFactory = queryByIds(result.ids);
					currentItemsQuery = resolvedClient.query(itemsQueryFactory);

					// Watch items query
					$effect(() => {
						if (currentItemsQuery) {
							items = new Map(currentItemsQuery.items);
							isLoading = !currentItemsQuery.resolved;
						}
					});
				} else {
					items = new Map();
					isLoading = false;
				}
			}
		});
	});

	onDestroy(() => {
		currentSearchReport?.release();
		currentItemsQuery?.release();
	});
</script>

{#if isLoading}
	{#if loading}
		{@render loading()}
	{:else}
		<span class="search-loading">Searching...</span>
	{/if}
{:else if items.size === 0}
	{#if empty}
		{@render empty()}
	{/if}
{:else}
	{#each [...items.values()] as item (item.id)}
		{@render children(item)}
	{/each}
{/if}
