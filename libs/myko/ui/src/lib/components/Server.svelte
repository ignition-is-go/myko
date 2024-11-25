<script lang="ts">
	import { PeerAlive, type Server } from '@myko/core';
	import { DateTime } from 'luxon';
	import { Observable, interval, map, of, switchMap } from 'rxjs';
	import { client } from '../services/client.js';

	export let server: Server;

	export let isClientServer: boolean = false;

	export let isLeader: boolean;

	$: alive = isClientServer
		? (interval(500).pipe(switchMap(() => client.ping().catch((e) => false))) as Observable<
				number | false
			>)
		: client.watchReport(new PeerAlive(server.id));

	$: ping =
		$alive === undefined ? 'Connecting' : $alive === false ? 'Dead' : `${Math.round($alive)}ms`;
</script>

<div class="server flex gap-5">
	<div class="info">
		<span class="address">{server.address}:{server.port}</span>
		<span class="version">{server.version}</span>
		<span class="name">Group: {server.groupId}</span>
		<span class="id">ID: {server.id}</span>
		<span class="started">Started: {DateTime.fromISO(server.startedAt).toRelative()}</span>
	</div>
	<div class="stats flex flex-col flex-1 items-end justify-between">
		<div class="badges flex gap-2">
			{#if isClientServer}
				<span class="badge connected">connected</span>
			{/if}
			{#if isLeader}
				<span class="badge leader">leader</span>
			{/if}
		</div>
		<span>Ping: {ping}</span>
	</div>
</div>

<style>
	.badge {
		padding: 0 0.5rem;
		border-radius: 10rem;
	}

	.connected {
		background-color: rgb(0, 77, 77);
	}

	.leader {
		background-color: rgb(77, 0, 68);
	}

	.server {
		position: relative;
		padding: 0.5rem;
		width: 350px;
	}

	span {
		display: block;
	}

	.version {
		opacity: 0.5;
	}

	.id {
		white-space: nowrap;
	}
</style>
