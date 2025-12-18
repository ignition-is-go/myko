<script lang="ts">
	import { myko as client } from '../../services/svelte-client.svelte.js';
	import { ChildEntitiesAllTime, EventsForEntity, type EventContainer, type ID } from '@myko/core';
	import { startWith } from 'rxjs';
	import { getContext } from 'svelte';
	import {
		fromISOMemo,
		isoToMillisMemo,
		TRANSACTIONS_VIEW_STATE,
		type TransactionsViewState
	} from '../state/viewstate.svelte.js';

	import { inview } from 'svelte-inview';
	import EntityHistory from './EntityHistory.svelte';
	import TransactionEvent from './TransactionEvent.svelte';
	import TransactionEventGroup from './TransactionEventGroup.svelte';

	const {
		id,
		itemType,
		level = 0
	}: {
		id: ID;
		itemType: string;
		level?: number;
	} = $props();

	const viewState = getContext(TRANSACTIONS_VIEW_STATE) as TransactionsViewState;

	const start = $derived(viewState.leftTime.toUTC().toISO());
	const end = $derived(viewState.rightTime.toUTC().toISO());

	const isRoot = $derived(level == 0);

	const history = client.watchQuery(new EventsForEntity(id)).pipe(startWith([]));

	const name = $derived(
		$history.length > 0 && 'name' in $history[0].event.item
			? $history[0].event.item.name
			: `Unknown ${itemType}`
	);

	$effect(() => {
		const firstEvent = $history && $history.length > 0 ? $history[0] : undefined;
		if (isRoot && firstEvent) {
			viewState.timeZero = fromISOMemo(firstEvent.event.createdAt);
		}
	});

	$effect(() => {
		viewState.registerEvents($history.map((x) => x.event.createdAt));
	});

	const visibleEvents = $derived(
		$history.filter(
			(x) => (start ? x.event.createdAt >= start : true) && (end ? x.event.createdAt <= end : true)
		)
	);

	type EventRenderInfo = {
		eventContainer: EventContainer;
		leftPx: number;
	};

	const visibleEventRenders: EventRenderInfo[] = $derived(
		visibleEvents.map((x) => {
			const timeMilis = $derived(isoToMillisMemo(x.event.createdAt));

			const leftTimeMilis = $derived(timeMilis - viewState.leftTimeMilis);

			const leftPx = $derived(leftTimeMilis / viewState.durationMilisPerPx);

			return {
				eventContainer: x,
				leftPx
			};
		})
	);

	type EventRenderGroup = {
		firstPx: number;
		lastPx: number;
		events: EventRenderInfo[];
	};

	const eventGroups = $derived(
		visibleEventRenders.reduce((acc, x) => {
			const lastGroup = acc[acc.length - 1];
			if (lastGroup && x.leftPx - lastGroup.lastPx < 10) {
				lastGroup.lastPx = x.leftPx;
				lastGroup.events.push(x);
			} else {
				acc.push({
					firstPx: x.leftPx,
					lastPx: x.leftPx,
					events: [x]
				});
			}
			return acc;
		}, [] as EventRenderGroup[])
	);

	const children = $derived(
		client.watchReport(new ChildEntitiesAllTime(itemType, id)).pipe(startWith([]))
	);

	let isInView = $state(false);
</script>

<div class="entity-history" style="min-height:{($children.length + 1) * 10}px;">
	<div
		class="self"
		use:inview={{}}
		oninview_enter={() => (isInView = true)}
		oninview_leave={() => (isInView = false)}
	>
		<h2 style="margin-left: {level * 1}rem;">
			{name}
			{$children.length}
		</h2>
		<div class="events">
			{#if isInView}
				{#each eventGroups as group}
					{#if group.events.length === 1}
						<TransactionEvent event={group.events[0].eventContainer.event} />
					{:else}
						<TransactionEventGroup
							firstPx={group.firstPx}
							lastPx={group.lastPx}
							numEvents={group.events.length}
						/>
					{/if}
				{/each}
			{/if}
		</div>
	</div>

	{#each $children as child (child.id)}
		<EntityHistory id={child.id} itemType={child.itemType} level={level + 1} />
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
		position: relative;
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
