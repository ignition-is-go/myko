<script lang="ts">
	const { firstPx, lastPx, numEvents }: { firstPx: number; lastPx: number; numEvents: number } =
		$props();

	let showDetail = $state(false);

	const onmouseover = () => {
		showDetail = true;
	};

	const onmouseleave = () => {
		showDetail = false;
	};

	// $inspect(leftTime, 'leftTime');
</script>

<div class="event" style="left: {firstPx}px; width: {lastPx - firstPx}px;">
	{#if showDetail}
		<div class="hover-data">
			<p>{numEvents} Events</p>
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
		height: var(--size);
		margin-left: calc(var(--size) / -2);
		margin-top: calc(var(--size) / -2);
		/* border-radius: 5px; */
		background-color: cornflowerblue;
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
