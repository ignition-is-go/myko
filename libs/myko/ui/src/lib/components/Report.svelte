<script lang="ts" generics="R extends ReportReturn<unknown>">
	import type { ReportReturn, ReportResult } from '@myko/ts';
	import { getMykoClient, type SvelteMykoClient } from '../services/svelte-client.svelte.js';
	import type { Snippet } from 'svelte';
	import { onDestroy } from 'svelte';

	type Result = ReportResult<R>;

	interface Props {
		report: R;
		client?: SvelteMykoClient;
		children: Snippet<[Result]>;
		loading?: Snippet;
	}

	let { report, client, children, loading }: Props = $props();

	const resolvedClient = client ?? getMykoClient();
	const result = resolvedClient.report(report);

	onDestroy(() => {
		result.release();
	});
</script>

{#if result.value === undefined && loading}
	{@render loading()}
{:else if result.value !== undefined}
	{@render children(result.value)}
{/if}
