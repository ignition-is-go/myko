<script lang="ts">
	import { getContext } from 'svelte';
	import { watchResize } from 'svelte-watch-resize';
	import {
		FULL_DATE_FORMAT,
		TRANSACTIONS_VIEW_STATE,
		type TransactionsViewState
	} from '../state/viewstate.svelte.js';

	const viewState = getContext(TRANSACTIONS_VIEW_STATE) as TransactionsViewState;

	const fullDuration = $derived(viewState.now.diff(viewState.timeZero));

	const viewLeftDuration = $derived(viewState.leftTime.diff(viewState.timeZero));
	const viewDuration = $derived(viewState.rightTime.diff(viewState.leftTime));

	const viewLeftPx = $derived(
		(viewLeftDuration.as('milliseconds') / fullDuration.as('milliseconds')) * viewState.width
	);
	const viewWidthPx = $derived(
		(viewDuration.as('milliseconds') / fullDuration.as('milliseconds')) * viewState.width
	);

	let leftTextRect: DOMRect | undefined = $state();
	let rightTextRect: DOMRect | undefined = $state();

	const shuntLeft = $derived(
		leftTextRect && viewLeftPx > leftTextRect.width && leftTextRect.width + 10 > viewWidthPx / 2
	);
	const shuntRight = $derived(
		rightTextRect &&
			viewState.width - viewLeftPx - viewWidthPx > rightTextRect.width &&
			rightTextRect.width + 10 > viewWidthPx / 2
	);

	const doubleShuntLeft = $derived(
		shuntLeft && !shuntRight && viewWidthPx < (rightTextRect?.width ?? 0)
	);
	const doubleShuntRight = $derived(
		shuntRight && !shuntLeft && viewWidthPx < (leftTextRect?.width ?? 0)
	);
</script>

<div class="all-time">
	<div class="all-times">
		{#each viewState.allEventTimestamps as timestamp}
			<div class="tick" style="left: {timestamp}px"></div>
		{/each}
	</div>

	<div
		class="view-region"
		style="left: {viewLeftPx}px; width: {viewWidthPx}px; --left-width: {leftTextRect?.width}px; --right-width: {rightTextRect?.width}px; --width: {viewWidthPx}px;"
	>
		<p
			class="text left"
			use:watchResize={(e) => {
				leftTextRect = e.getBoundingClientRect();
			}}
			class:shuntLeft
			class:doubleShuntLeft
		>
			{viewState.leftTime.toFormat(FULL_DATE_FORMAT)}
		</p>
		<p
			class="text right"
			use:watchResize={(e) => {
				rightTextRect = e.getBoundingClientRect();
			}}
			class:shuntRight
			class:doubleShuntRight
		>
			{viewState.rightTime.toFormat(FULL_DATE_FORMAT)}
		</p>
	</div>
</div>

<style>
	.all-time {
		position: relative;
		height: 1.5rem;
		background-color: rgba(255, 255, 255, 0.1);
	}

	.text {
		position: absolute;
		white-space: nowrap;
	}

	.left {
		left: 0.5rem;
	}

	.right {
		right: 0.5rem;
	}

	.shuntLeft {
		right: calc(var(--width) + 0.5rem) !important;
		left: unset;
		background-color: red;
	}

	.shuntRight {
		right: unset;
		left: calc(var(--width) + 0.5rem) !important;
		background-color: red;
	}

	.doubleShuntLeft {
		left: unset;
		right: calc(var(--right-width) + 1rem) !important;
		background-color: blue;
	}

	.doubleShuntRight {
		right: unset;
		left: calc(var(--left-width) + 1rem) !important;
		background-color: blue;
	}

	.view-region {
		position: absolute;
		top: 3px;
		bottom: 3px;
		border-radius: 0.2rem;
		background-color: rgba(255, 255, 255, 0.1);
		display: flex;
		justify-content: space-between;
		align-items: center;
		color: white;
		padding: 0 0.5rem;
		box-sizing: border-box;
		user-select: none;
	}

	.tick {
		position: absolute;
		top: 0;
		bottom: 0;
		width: 1px;
		background-color: rgba(255, 255, 255, 0.1);
	}
</style>
