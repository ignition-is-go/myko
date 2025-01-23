<script lang="ts">
	import { PeerAlive, type Server } from '@myko/core';
	import { DateTime } from 'luxon';
	import { Observable, interval, map, of, switchMap } from 'rxjs';
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
</script>

<div class="server flex gap-5">
	<div class="info">
		<span class="address">{server.address}:{server.port}</span>
		<span class="version">{server.version}</span>
		<span class="id">ID: {server.id}</span>
		<span class="started">Started: {DateTime.fromISO(server.startedAt).toRelative()}</span>
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
