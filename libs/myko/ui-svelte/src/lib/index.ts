// Components
export { default as ConnectionStats } from './components/ConnectionStats.svelte';
export { default as Logs } from './components/Logs.svelte';
export { default as Query } from './components/Query.svelte';
export { default as Report } from './components/Report.svelte';
export { default as Search } from './components/Search.svelte';
export { default as ServerView } from './components/ServerView.svelte';
export { default as Transactions } from './components/transactions/Transactions.svelte';
export { default as EntityDiffTree } from './components/EntityDiffTree.svelte';
export { default as EntityDiffBadge } from './components/EntityDiffBadge.svelte';
export { default as EntityDiffFields } from './components/EntityDiffFields.svelte';
export { default as EntityDiffNode } from './components/EntityDiffNode.svelte';
export * from './components/state/windback.svelte';
export * from './components/windback/index';

// Utils
export * from './utils/entity-diff';
export * from './utils/entity-tree';
export * from './utils/entity-apply';

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
} from './services/svelte-client.svelte';

// Re-export useful types from @myko/core
export { ConnectionStatus, MykoProtocol, type ClientStats } from '@myko/core';
