// Components
export { default as ConnectionStats } from './components/ConnectionStats.svelte';
export { default as Logs } from './components/Logs.svelte';
export { default as Query } from './components/Query.svelte';
export { default as Report } from './components/Report.svelte';
export { default as Search } from './components/Search.svelte';
export { default as ServerView } from './components/ServerView.svelte';
export { default as Transactions } from './components/transactions/Transactions.svelte';
export * from './components/state/windback.svelte.js';
export * from './components/windback/index.js';

// Svelte-friendly Myko client
export {
	createMykoClient,
	getMykoClient,
	myko,
	myko as client,
	SvelteMykoClient,
	type CommandError,
	type CommandSuccess,
	type LiveQuery,
	type LiveReport
} from './services/svelte-client.svelte.js';

// Re-export useful types from @myko/core
export { ConnectionStatus, type ClientStats } from '@myko/core';
