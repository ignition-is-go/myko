<script lang="ts">
	import type { MEvent } from '@myko/core';
	import { DateTime } from 'luxon';
	import { getContext } from 'svelte';
	import {
		FULL_DATE_FORMAT,
		TRANSACTIONS_VIEW_STATE,
		type TransactionsViewState
	} from '../state/viewstate.svelte.js';

	const { event }: { event: MEvent } = $props();

	const time = $derived(DateTime.fromISO(event.createdAt));

	const viewState = getContext(TRANSACTIONS_VIEW_STATE) as TransactionsViewState;

	const leftTime = $derived(time.diff(viewState.leftTime));

	const leftPx = $derived(leftTime.as('milliseconds') / viewState.durationPerPx.as('milliseconds'));

	let showDetail = $state(false);

	const onmouseover = () => {
		showDetail = true;
	};

	const onmouseleave = () => {
		showDetail = false;
	};

	// $inspect(leftTime, 'leftTime');
</script>

<div class="event {event.changeType}" style="left: {leftPx}px;">
	{#if showDetail}
		<div class="hover-data">
			<p>{event.itemType}</p>
			<p>{event.changeType}</p>
			<p>{time.toFormat(FULL_DATE_FORMAT)}</p>
			<pre>{JSON.stringify(event.item, null, 2)}</pre>
		</div>
	{/if}
	<div
		class="icon"
		role="presentation"
		{onmouseover}
		{onmouseleave}
		onfocus={onmouseover}
		onblur={onmouseleave}
	></div>
</div>

<style>
	.event {
		position: relative;
		width: 0;
		height: 0;
	}

	.icon {
		--size: 8px;
		width: var(--size);
		height: var(--size);
		margin-left: calc(var(--size) / -2);
		margin-top: calc(var(--size) / -2);
		border-radius: 50%;
	}

	.DEL .icon {
		background-color: orangered;
	}

	.SET .icon {
		background-color: lightgreen;
	}

	.hover-data {
		position: absolute;
		right: 1rem;
		z-index: 1000;
		background-color: rgba(0, 0, 0, 0.5);
		padding: 0.5rem;
		border-radius: 0.25rem;
	}
</style>
