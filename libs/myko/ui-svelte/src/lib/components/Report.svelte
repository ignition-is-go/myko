<script lang="ts" generics="R extends Report<unknown>">
	import type { Report, ReportResult } from '@myko/ts';
	import {
		getMykoClient,
		type ReactiveReport,
		type SvelteMykoClient
	} from '../services/svelte-client.svelte.js';
	import type { Snippet } from 'svelte';
	import { onDestroy } from 'svelte';

	interface Props {
		report: R;
		client?: SvelteMykoClient;
		children: Snippet<[ReportResult<R>]>;
		loading?: Snippet;
	}

	let { report, client, children, loading }: Props = $props();

	const resolvedClient = client ?? getMykoClient();
	const result: ReactiveReport<R> = resolvedClient.report(report);

	onDestroy(() => {
		result.release();
	});
</script>

{#if result.value === undefined && loading}
	{@render loading()}
{:else if result.value !== undefined}
	{@render children(result.value)}
{/if}
