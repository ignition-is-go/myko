<script lang="ts">
	import { client } from '../services/client.js';
	import { GetConnectedServer, GetPeerServers, Log } from '@myko/core';
	import { map, of, startWith } from 'rxjs';
	import Server from './Server.svelte';
	import Logs from './Logs.svelte';

	const peers = client.watchQuery(new GetPeerServers()).pipe(startWith([]));

	const connected = client.watchQuery(new GetConnectedServer()).pipe(map((x) => x.shift()));

	// $: leader = $connected
	//   ? client.watchReport(new GroupLeader($connected?.groupId))
	//   : of(null)
</script>

<div class="page">
	<div class="scroll">
		{#if $connected}
			<Server server={$connected} isClientServer isLeader={false} />
			<Logs serverId={$connected.id}></Logs>
		{/if}
		{#each $peers as server (server.id)}
			<Server {server} isLeader={false} />
			<Logs serverId={server.id}></Logs>
		{/each}
	</div>
</div>

<style>
	.page {
		overflow: auto;
		box-sizing: border-box;
		padding: 1rem;
		display: flex;
		max-height: 100%;
	}

	.scroll {
		display: flex;
		flex-direction: column;
		gap: 0.5rem;
		overflow: auto;
	}
</style>
