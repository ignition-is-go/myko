<script lang="ts">
	import { DateTime } from 'luxon';
	import { getContext } from 'svelte';
	import {
		FULL_DATE_FORMAT,
		TRANSACTIONS_VIEW_STATE,
		type TransactionsViewState
	} from '../state/viewstate.svelte.js';
	import { windbackState } from '../state/windback.svelte.js';

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

	// let leftTextRect: DOMRect | undefined = $state();
	// let rightTextRect: DOMRect | undefined = $state();

	// const shuntLeft = $derived(
	// 	leftTextRect && viewLeftPx > leftTextRect.width && leftTextRect.width + 10 > viewWidthPx / 2
	// );
	// const shuntRight = $derived(
	// 	rightTextRect &&
	// 		viewState.width - viewLeftPx - viewWidthPx > rightTextRect.width &&
	// 		rightTextRect.width + 10 > viewWidthPx / 2
	// );

	// const doubleShuntLeft = $derived(
	// 	shuntLeft && !shuntRight && viewWidthPx < (rightTextRect?.width ?? 0)
	// );
	// const doubleShuntRight = $derived(
	// 	shuntRight && !shuntLeft && viewWidthPx < (leftTextRect?.width ?? 0)
	// );

	let timestampsEl: HTMLElement | null = $state(null);
</script>

<div class="all-time">
	<div class="bounds">
		<div class="start-time">{viewState.timeZero.toFormat(FULL_DATE_FORMAT)}</div>
		<div class="end-time">{viewState.now.toFormat(FULL_DATE_FORMAT)}</div>
	</div>
	<div class="all-times">
		{#each viewState.allEventTimestamps as timestamp}
			<div class="event" style="left: {timestamp}px"></div>
		{/each}
	</div>

	<div class="cursor-time">
		<div class="cursor" style="left: {viewState.windbackFullX}px; ">
			<div class="line"></div>
			<div class="triangle"></div>
		</div>
	</div>

	<div
		class="view-region"
		style="left: {viewLeftPx}px; width: {viewWidthPx}px; --width: {viewWidthPx}px;"
	></div>
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
		{#if windbackState.cursor}
			<div class="windback-cursor cursor" class:lagging={!viewState.windbackState.caughtUp}>
				<div class="line" style="left: {viewState.windbackX}px">
					<div class="text">
						<p>
							{windbackState.cursor.toFormat(FULL_DATE_FORMAT)}
						</p>
						<p>
							{viewState.now
								.diff(windbackState.cursor)
								.rescale()
								.normalize()
								.toHuman({ compactDisplay: 'short', unitDisplay: 'narrow' })} ago
						</p>
					</div>
					<div class="click-area"></div>
				</div>
			</div>
		{/if}
		<div class="live-cursor cursor">
			<div class="line" style="left: {viewState.mouseX}px">
				<div class="text">
					<p>
						{viewState.mouseTime.toFormat(viewState.majorResolution.majorFormat)}
					</p>
					<p>
						{viewState.mouseTimeRelative.rescale().normalize().toHuman({
							compactDisplay: 'short',
							unitDisplay: 'narrow',
							maximumFractionDigits: 0
						})}
						ago
					</p>
				</div>
			</div>
		</div>
	</div>
</div>

<style>
	/* bounds */

	.all-time {
		position: relative;
		border-bottom: rgba(255, 255, 255, 0.1) 1px solid;
	}

	.bounds {
		display: flex;
		width: 100%;
		padding: 0.1rem 0.5rem;
		justify-content: space-between;
	}

	/* VIEW REGION (scroll bar) */

	.all-times {
		position: absolute;
		inset: 0;
		z-index: 999;
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

	/* .text {
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
	} */

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

	.cursor .line {
		position: absolute;
		top: 0;
		bottom: 0;
		border-left: 1px solid rgba(255, 255, 255, 0.2);
	}

	.cursor .text {
		white-space: nowrap;
		background-color: rgba(0, 0, 0, 0.9);
		padding: 0 0.25rem;
	}

	.windback-cursor {
		border-color: cornflowerblue;

		color: cornflowerblue;

		.line {
			border-color: cornflowerblue;
		}
	}

	.click-area {
		position: absolute;
		inset: 0;
		width: 20px;
		left: -10px;
		z-index: 999;
		background-color: rgba(255, 255, 255, 0.1);
	}

	.click-area:hover {
		cursor: ew-resize;
		background-color: rgba(255, 255, 255, 0.9);
	}

	.lagging {
		.line {
			border-color: orangered;
		}
	}
</style>
