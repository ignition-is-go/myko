<script lang="ts">
	import { myko as client } from '../../services/svelte-client.svelte.js';
	import { EntitySnapshotDifference, type ID } from '@myko/core';
	import { setContext } from 'svelte';
	import { watchResize } from 'svelte-watch-resize';
	import { TRANSACTIONS_VIEW_STATE, TransactionsViewState } from '../state/viewstate.svelte';
	import { windbackState } from '../state/windback.svelte.js';
	import EntityHistory from './EntityHistory.svelte';
	import TimeStrip from './TimeStrip.svelte';

	const viewstate = new TransactionsViewState(windbackState);

	setContext(TRANSACTIONS_VIEW_STATE, viewstate);

	let isMouseDown = $state(false);

	const {
		entrypointId,
		entrypointItemType
	}: {
		entrypointId: ID;
		entrypointItemType: string;
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

	const onmousedown = (e: MouseEvent) => {
		e.stopPropagation();
		if (e.button === 0) {
		}

		viewstate.startDragWindback();
	};

	const onmouseup = (e: MouseEvent) => {
		e.stopPropagation();

		viewstate.stopDragWindback();
	};

	const diff = client.watchReport(
		new EntitySnapshotDifference({
			parentType: entrypointItemType,
			parentId: entrypointId
		})
	);
</script>

<svelte:window {onkeydown} {onmousedown} {onmouseup} />
<div
	class:windbackCursor={viewstate.isOverWindback}
	class="transactions-frame extra class"
	role="presentation"
	use:watchResize={(e) => {
		console.log('RESIZE', e.clientWidth);
		viewstate.width = e.clientWidth;
	}}
	{onwheel}
	{onmousemove}
>
	<div class="header pad">
		<TimeStrip></TimeStrip>
	</div>
	<div class="scroll pad">
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
		scrollbar-width: none;
	}

	.header {
		flex-shrink: 0;
	}

	.windbackCursor {
		cursor: ew-resize;
	}
</style>
