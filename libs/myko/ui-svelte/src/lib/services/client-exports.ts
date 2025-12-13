// Svelte-friendly Myko client exports
// This module avoids the $lib alias issues from the main index.ts

export {
	createMykoClient,
	getMykoClient,
	myko,
	SvelteMykoClient,
	type CommandError,
	type CommandSuccess,
	type ReactiveQuery,
	type ReactiveReport
} from './svelte-client.svelte.js';

export { default as ConnectionStats } from '../components/ConnectionStats.svelte';
export { default as Query } from '../components/Query.svelte';
export { default as Report } from '../components/Report.svelte';
export { default as Search } from '../components/Search.svelte';

// Re-export useful types from @myko/ts
export { ConnectionStatus, type ClientStats } from '@myko/ts';
