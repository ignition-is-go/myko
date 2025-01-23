<script lang="ts">
	import { client } from '../services/client.js';
	import { GetConnectedServer, GetPeerServers, Log } from '@myko/core';
	import { map, startWith } from 'rxjs';
	import Server from './Server.svelte';
	import Logs from './Logs.svelte';

	const peers = client.watchQuery(new GetPeerServers()).pipe(startWith([]));

	const connected = client.watchQuery(new GetConnectedServer()).pipe(map((x) => x.shift()));
</script>

<div class="page">
	{#if $connected}
		<Server server={$connected} isClientServer isLeader={false} />
	{/if}
	{#each $peers as server (server.id)}
		<Server {server} isLeader={false} />
	{/each}
</div>

<style>
	.page {
		overflow: auto;
		box-sizing: border-box;
		padding: 1rem;
		display: flex;
		gap: 1rem;
		flex-direction: row;
		max-height: 100%;
	}
</style>
