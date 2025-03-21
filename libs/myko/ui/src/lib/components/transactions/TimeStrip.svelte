<script lang="ts">
	import { DateTime } from 'luxon';
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

	const { windbackCursor }: { windbackCursor: DateTime } = $props();

	const windbackCursorPx = $derived(
		(windbackCursor.diff(viewState.timeZero).as('milliseconds') / fullDuration.as('milliseconds')) *
			viewState.width
	);

	const windbackCursorVisible = $derived(
		windbackCursor >= viewState.timeZero && windbackCursor <= viewState.now
	);

	const cursorMilis = $derived(windbackCursor ? windbackCursor.toMillis() : undefined);

	const cursorPx = $derived(
		cursorMilis ? (cursorMilis - viewState.leftTimeMilis) / viewState.durationMilisPerPx : 0
	);

	let timestampsEl: HTMLElement | null = $state(null);
</script>

<div class="bounds">
	<div class="start-time">{viewState.timeZero.toFormat(FULL_DATE_FORMAT)}</div>
	<div class="end-time">{viewState.now.toFormat(FULL_DATE_FORMAT)}</div>
</div>

<div class="all-time">
	<div class="all-times">
		{#each viewState.allEventTimestamps as timestamp}
			<div class="event" style="left: {timestamp}px"></div>
		{/each}
	</div>

	<div class="cursor-time">
		<div
			class="cursor"
			style="left: {windbackCursorPx}px; display: {windbackCursorVisible ? 'block' : 'none'}"
		>
			<div class="line"></div>
			<div class="triangle"></div>
		</div>
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
<div class="timestamps" bind:this={timestampsEl}>
	{#each viewState.majors as { leftPx, time: majorTime }, i}
		<div class="major timestamp" style="left: {leftPx}px;">
			<div class="tick"></div>
			<span class="timestamp-text"
				>{DateTime.fromMillis(majorTime).toFormat(viewState.majorResolution.majorFormat)}</span
			>
			{#each viewState.minors as { leftPx, time: minorTime }, i}
				<div class="minor timestamp" style="left: {leftPx}px;">
					<div class="tick"></div>
					<span class="timestamp-text"
						>{DateTime.fromMillis(minorTime + majorTime).toFormat(
							viewState.minorResolution.minorFormat
						)}</span
					>
				</div>
			{/each}
		</div>
	{/each}
	<div class="overlay">
		<div class="cursor" style="left: {cursorPx}px;">
			<div class="line"></div>
			<div class="triangle"></div>
		</div>
	</div>
</div>

<style>
	/* bounds */

	.bounds {
		display: flex;
		width: 100%;
		padding: 0.1rem 0.5rem;
		justify-content: space-between;
		border-bottom: 1px solid rgba(255, 255, 255, 0.1);
	}

	/* VIEW REGION (scroll bar) */

	.all-time {
		position: relative;
		height: 1.5rem;
		border-bottom: rgba(255, 255, 255, 0.1) 1px solid;
	}
	.view-region {
		position: absolute;
		top: 3px;
		bottom: 3px;
		border-radius: 0.2rem;
		background-color: rgba(255, 255, 255, 0.2);
		display: flex;
		justify-content: space-between;
		align-items: center;
		color: white;
		padding: 0 0.5rem;
		box-sizing: border-box;
		user-select: none;
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
	}

	.shuntRight {
		right: unset;
		left: calc(var(--width) + 0.5rem) !important;
	}

	.doubleShuntLeft {
		left: unset;
		right: calc(var(--right-width) + 1rem) !important;
	}

	.doubleShuntRight {
		right: unset;
		left: calc(var(--left-width) + 1rem) !important;
	}

	.event {
		position: absolute;
		height: 100%;
		border-left: 1px solid rgba(255, 255, 255, 0.2);
	}

	/* timestamps and grid lines */
	.timestamps {
		position: relative;
		height: 2rem;
	}
	.timestamp {
		position: absolute;
	}

	.timestamp-text {
		left: 3px;
		position: relative;
	}

	.overlay {
		position: absolute;
		inset: 0;
		bottom: unset;
		height: 100vh;
		pointer-events: none;
		z-index: 999;
	}

	.tick {
		position: absolute;
		top: 0;
		height: 100vh;
		width: 0px;
		border-left: 1px solid rgba(255, 255, 255, 0.5);
	}

	.minor .tick {
		opacity: 0.25;
	}

	.major {
		opacity: 0.5;
	}
</style>
