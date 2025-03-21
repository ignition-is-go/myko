<script lang="ts">
	import { type ID } from '@myko/core';
	import type { DateTime } from 'luxon';
	import { setContext } from 'svelte';
	import { watchResize } from 'svelte-watch-resize';
	import { TRANSACTIONS_VIEW_STATE, TransactionsViewState } from '../state/viewstate.svelte';
	import EntityHistory from './EntityHistory.svelte';
	import TimeStrip from './TimeStrip.svelte';

	const viewstate = new TransactionsViewState();

	setContext(TRANSACTIONS_VIEW_STATE, viewstate);

	const {
		entrypointId,
		entrypointItemType,
		windbackCursor,
		onWindbackCursorUpdate
	}: {
		entrypointId: ID;
		entrypointItemType: string;
		windbackCursor: DateTime;
		onWindbackCursorUpdate: (cursor: DateTime) => void;
	} = $props();

	const onwheel = (e: WheelEvent) => {
		if (e.ctrlKey || e.metaKey) {
			e.preventDefault();
			viewstate.zoom(e.deltaY);
		}

		if (e.shiftKey) {
			e.preventDefault();
			viewstate.pan(e.deltaY);
		}
	};

	const onmousemove = (e: MouseEvent) => {
		viewstate.mouseX = e.clientX;
	};

	const onkeydown = (e: KeyboardEvent) => {
		if (e.key === 'z') {
			viewstate.zoomAllTheWayOut();
		}
	};
</script>

<svelte:window {onkeydown} />
<div
	class="transactions-frame extra class"
	onmousedown={(e) => e.stopPropagation()}
	onmouseup={(e) => e.stopPropagation()}
	role="presentation"
	use:watchResize={(e) => {
		console.log('RESIZE', e.clientWidth);
		viewstate.width = e.clientWidth;
	}}
	{onwheel}
	{onmousemove}
>
	<div class="header">
		<TimeStrip {windbackCursor}></TimeStrip>
	</div>
	<div class="scroll">
		<EntityHistory id={entrypointId} itemType={entrypointItemType} />
	</div>
</div>

<style>
	.transactions-frame {
		height: 100%;
		min-height: 100%;
		overflow: hidden;
		display: flex;
		flex-direction: column;
		justify-content: flex-start;
	}

	.scroll {
		overflow: scroll;
	}

	.header {
		flex-shrink: 0;
	}
</style>
