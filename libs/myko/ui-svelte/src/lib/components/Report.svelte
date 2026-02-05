<script lang="ts" generics="R extends Report<unknown>">
	import type { Report, ReportResult } from '@myko/ts';
	import { getMykoClient, type SvelteMykoClient } from '../services/svelte-client.svelte.js';
	import type { Snippet } from 'svelte';

	interface Props {
		report: R;
		client?: SvelteMykoClient;
		children: Snippet<[ReportResult<R>]>;
		loading?: Snippet;
	}

	let { report, client, children, loading }: Props = $props();

	const resolvedClient = client ?? getMykoClient();
	const result = resolvedClient.liveReport(() => report);
</script>

{#if result.value === undefined}
	{#if loading}
		{@render loading()}
	{/if}
{:else}
	{@render children(result.value)}
{/if}
