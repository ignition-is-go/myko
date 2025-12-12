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

export { default as Query } from '../components/Query.svelte';
export { default as Report } from '../components/Report.svelte';

// Re-export useful types from @myko/ts
export { ConnectionStatus } from '@myko/ts';
