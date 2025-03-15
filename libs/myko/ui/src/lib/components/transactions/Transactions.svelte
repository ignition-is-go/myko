<script lang="ts">
	import { type ID } from '@myko/core';
	import { setContext } from 'svelte';
	import { watchResize } from 'svelte-watch-resize';
	import { TRANSACTIONS_VIEW_STATE, TransactionsViewState } from '../state/viewstate.svelte';
	import EntityHistory from './EntityHistory.svelte';
	import TimeStrip from './TimeStrip.svelte';

	const viewstate = new TransactionsViewState();

	setContext(TRANSACTIONS_VIEW_STATE, viewstate);

	const { entrypointId, entrypointItemType }: { entrypointId: ID; entrypointItemType: string } =
		$props();

	const onscroll = (e: WheelEvent) => {
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
	class="transactions-frame"
	role="presentation"
	use:watchResize={(e) => {
		console.log('RESIZE', e.clientWidth);
		viewstate.width = e.clientWidth;
	}}
	onwheel={onscroll}
	{onmousemove}
>
	<div class="header">
		<TimeStrip></TimeStrip>
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
