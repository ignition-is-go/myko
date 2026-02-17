<script lang="ts">
	import {
		LOG_RANK,
		type LogLevel,
		PeerAlive,
		ServerLogLevel,
		SetLogLevel,
		type Server
	} from '@myko/ts';
	import { DateTime } from 'luxon';
	import { Observable, interval, switchMap } from 'rxjs';
	import { onDestroy } from 'svelte';
	import { myko as client } from '../services/svelte-client.svelte.js';

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
			: client.watchReport(new PeerAlive({ peerId: server.id }))
	);

	let ping = $derived.by(() => {
		const value = $alive;
		if (value === undefined) return 'Connecting';
		if (value === false) return 'Dead';
		if (!Number.isFinite(value) || value < 0) return 'Unknown';
		return `${Math.round(value)}ms`;
	});

	let started = $state(DateTime.fromISO(server.startedAt).toRelative());

	let startedInterbal = setInterval(() => {
		started = DateTime.fromISO(server.startedAt).toRelative();
	}, 1000);

	onDestroy(() => {
		clearInterval(startedInterbal);
	});

	const currentLogLevel = $derived(client.watchReport(new ServerLogLevel({ serverId: server.id })));
</script>

<div class="server flex gap-5">
	<div class="info">
		<span class="address">{server.address}:{server.port}</span>
		<span class="version">{server.version}</span>
		<span class="id">ID: {server.id}</span>
		<span class="started">Started: {started}</span>
		<span>Ping {isClientServer ? '' : 'to connected'}: {ping}</span>
		<select
			aria-label="Log Level"
			class="select"
			onchange={(e) => {
				console.log(e.currentTarget.value);
				client.sendCommand(new SetLogLevel(server.id, e.currentTarget.value as LogLevel));
			}}
		>
			{#each LOG_RANK as rank}
				<option selected={rank === $currentLogLevel} value={rank}>{rank}</option>
			{/each}
		</select>
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
