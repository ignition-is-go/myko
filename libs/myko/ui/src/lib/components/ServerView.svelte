<script lang="ts">
	import { PeerAlive, type Server } from '@myko/core';
	import { DateTime } from 'luxon';
	import { Observable, interval, switchMap } from 'rxjs';
	import { onDestroy } from 'svelte';
	import { client } from '../services/client.js';

	interface Props {
		server: Server;
		isClientServer?: boolean;
	}

	let { server, isClientServer = false }: Props = $props();

	let alive = $derived(
		isClientServer
			? (interval(500).pipe(switchMap(() => client.ping().catch((e) => false))) as Observable<
					number | false
				>)
			: client.watchReport(new PeerAlive(server.id))
	);

	let ping = $derived(
		$alive === undefined ? 'Connecting' : $alive === false ? 'Dead' : `${Math.round($alive)}ms`
	);

	let started = $state(DateTime.fromISO(server.startedAt).toRelative());

	let startedInterbal = setInterval(() => {
		started = DateTime.fromISO(server.startedAt).toRelative();
	}, 1000);

	onDestroy(() => {
		clearInterval(startedInterbal);
	});
</script>

<div class="server flex gap-5">
	<div class="info">
		<span class="address">{server.address}:{server.port}</span>
		<span class="version">{server.version}</span>
		<span class="id">ID: {server.id}</span>
		<span class="started">Started: {started}</span>
		<span>Ping {isClientServer ? '' : 'to connected'}: {ping}</span>
	</div>
</div>

<style>
	.server {
		position: relative;
	}

	span {
		display: block;
		white-space: nowrap;
	}

	.version {
		opacity: 0.5;
	}

	.id {
		white-space: nowrap;
	}
</style>
