<script lang="ts">
	import type { Snippet } from 'svelte';
	import { windbackState } from '../state/windback.svelte.js';
	import Transactions from '../transactions/Transactions.svelte';

	const { children }: { children: Snippet } = $props();
</script>

<div class="bg"></div>

<div class="windback">
	<div class="windback-frame" class:windingBack={windbackState.windingBack}>
		{@render children()}
	</div>

	<div class="windback-control" class:windingBack={windbackState.windingBack}>
		{#if windbackState.ctx}
			<Transactions
				entrypointId={windbackState.ctx.id}
				entrypointItemType={windbackState.ctx.itemType}
			/>
		{/if}
	</div>
</div>

<style>
	.bg {
		position: absolute;
		top: 0;
		left: 0;
		width: 100%;
		height: 100%;
		z-index: -1;
	}

	.windback {
		height: 100%;
		width: 100%;
		display: flex;
		flex-direction: column;
	}

	.windback-frame {
		transform-origin: 50% 10%;
		flex: 1;
		position: relative;
		width: 100%;
		background-color: black;
		transition: filter 1s ease-in-out;

		&.windingBack {
			filter: saturate(0.25);
		}
	}

	.windback-control {
		background-color: rgba(0, 0, 0, 0.5);
		z-index: 1000;
		height: 0%;
		transition: height 1s ease-in-out;

		&.windingBack {
			height: 30%;
		}
	}
</style>
