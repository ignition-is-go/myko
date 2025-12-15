<script lang="ts" generics="R extends Report<unknown>">
	import type { Report, ReportResult } from '@myko/ts';
	import {
		getMykoClient,
		type ReactiveReport,
		type SvelteMykoClient
	} from '../services/svelte-client.svelte.js';
	import type { Snippet } from 'svelte';
	import { onDestroy, onMount } from 'svelte';

	interface Props {
		report: R;
		client?: SvelteMykoClient;
		children: Snippet<[ReportResult<R>]>;
		loading?: Snippet;
	}

	let { report, client, children, loading }: Props = $props();

	const resolvedClient = client ?? getMykoClient();
	let result: ReactiveReport<R> | undefined = $state(undefined);

	onMount(() => {
		result = resolvedClient.report(report);
	});

	onDestroy(() => {
		result?.release();
	});
</script>

{#if result === undefined || result.value === undefined}
	{#if loading}
		{@render loading()}
	{/if}
{:else}
	{@render children(result.value)}
{/if}
