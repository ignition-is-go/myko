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
		<div class="scroll">
			<Server server={$connected} isClientServer isLeader={false} />
			<Logs serverId={$connected.id}></Logs>
		</div>
	{/if}
	{#each $peers as server (server.id)}
		<div class="scroll">
			<Server {server} isLeader={false} />
			<Logs serverId={server.id}></Logs>
		</div>
	{/each}
</div>

<style>
	.page {
		overflow: auto;
		box-sizing: border-box;
		padding: 1rem;
		display: flex;
		flex-direction: row;
		max-height: 100%;
	}

	.scroll {
		display: flex;
		flex-direction: column;
		gap: 0.5rem;
		overflow: auto;
		padding: 0.5rem;
	}
</style>
