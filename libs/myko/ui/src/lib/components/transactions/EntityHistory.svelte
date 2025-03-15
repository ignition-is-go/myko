<script lang="ts">
	import { client } from '$lib/services/client.js';
	import { ChildEntities, EventsForEntity, type ID } from '@myko/core';
	import { startWith } from 'rxjs';
	import { getContext } from 'svelte';
	import {
		TRANSACTIONS_VIEW_STATE,
		type TransactionsViewState
	} from '../state/viewstate.svelte.js';

	import { DateTime } from 'luxon';
	import EntityHistory from './EntityHistory.svelte';
	import TransactionEvent from './TransactionEvent.svelte';
	const { id, itemType, level = 0 }: { id: ID; itemType: string; level?: number } = $props();

	const viewState = getContext(TRANSACTIONS_VIEW_STATE) as TransactionsViewState;

	const start = $derived(viewState.leftTime.toUTC().toISO());
	const end = $derived(viewState.rightTime.toUTC().toISO());

	const isRoot = $derived(level == 0);

	const history = $derived(client.watchReport(new EventsForEntity(id)).pipe(startWith([])));

	const name = $derived(
		$history.length > 0 && 'name' in $history[0].item
			? $history[0].item.name
			: `Unknown ${itemType}`
	);

	$effect(() => {
		const firstEvent = $history && $history.length > 0 ? $history[$history.length - 1] : undefined;
		if (isRoot && firstEvent) {
			viewState.timeZero = DateTime.fromISO(firstEvent.createdAt);
		}
	});

	$effect(() => {
		viewState.registerEvents($history.map((x) => x.createdAt));
	});

	const visibleEvents = $derived(
		$history.filter(
			(x) => (start ? x.createdAt >= start : true) && (end ? x.createdAt <= end : true)
		)
	);

	const children = $derived(client.watchReport(new ChildEntities(itemType, id)));
</script>

<div class="entity-history">
	<div class="self">
		<h2 style="margin-left: {level * 1}rem;">
			{name}
		</h2>
		<div class="events">
			{#each visibleEvents as event (`event-${event.item.id}-${event.createdAt}`)}
				<TransactionEvent {event} />
			{/each}
		</div>
	</div>

	{#each $children as child (child.item.id)}
		<EntityHistory id={child.item.id} itemType={child.itemType} level={level + 1} />
	{/each}
</div>

<style>
	.self {
		display: flex;
		align-items: center;
		position: relative;
		margin-bottom: 0.1rem;
	}
	.entity-history {
		background-color: rgba(255, 255, 255, 0.05);
		min-height: 10px;
	}

	h2 {
		opacity: 0.5;
	}

	.events {
		position: absolute;
		display: flex;
		align-items: center;
		inset: 0;
	}
</style>
