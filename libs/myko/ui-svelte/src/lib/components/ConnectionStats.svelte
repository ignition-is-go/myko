<script lang="ts">
	import { GetConnectedServer } from '@myko/core';
	import { getMykoClient, type SvelteMykoClient } from '../services/svelte-client.svelte.js';

	interface Props {
		/** Optional client instance (defaults to global singleton) */
		client?: SvelteMykoClient;
		/** CSS class for the container */
		class?: string;
	}

	let { client, class: className = '' }: Props = $props();

	const resolvedClient = client ?? getMykoClient();

	const stats = $derived(resolvedClient.stats);
	const isConnected = $derived(resolvedClient.isConnected);

	// Query for connected server
	const serverQuery = resolvedClient.liveQuery(() => new GetConnectedServer({}));

	const server = $derived([...serverQuery.items.values()][0]);
</script>

{#if isConnected && stats}
	<div class="connection-stats {className}">
		{#if server}
			<span class="stat server" title="Connected server">
				<span class="value">{server.address}:{server.port}</span>
			</span>
			<span class="stat version" title="Server version">
				<span class="label">v</span>
				<span class="value">{server.version}</span>
			</span>
		{/if}
		<span class="stat ping" title="Round-trip latency">
			<span class="label">Ping</span>
			<span class="value">{stats.ping}ms</span>
		</span>
		<span class="stat down" title="Messages received per second">
			<span class="label">Down</span>
			<span class="value">{stats.mpsDown}/s</span>
		</span>
		<span class="stat up" title="Messages sent per second">
			<span class="label">Up</span>
			<span class="value">{stats.mpsUp}/s</span>
		</span>
	</div>
{:else}
	<div class="connection-stats disconnected {className}">
		<span class="stat">
			<span class="value">--</span>
		</span>
	</div>
{/if}

<style>
	.connection-stats {
		display: inline-flex;
		gap: 1rem;
		font-size: 0.875rem;
		font-family: monospace;
		white-space: nowrap;
	}

	.stat {
		display: flex;
		gap: 0.25rem;
		align-items: center;
	}

	.label {
		opacity: 0.6;
	}

	.value {
		font-weight: 500;
	}

	.disconnected {
		opacity: 0.4;
	}
</style>
