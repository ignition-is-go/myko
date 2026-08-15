// Vue-friendly Myko client exports
export {
	createMykoClient,
	getMykoClient,
	myko,
	VueMykoClient,
	type CommandError,
	type CommandSuccess,
	type ReactiveQuery,
	type ReactiveQueryState,
	type ReactiveReport
} from './services/vue-client';

// Vue composables
export {
	useQuery,
	useQueryState,
	useReport,
	useConnection,
	type UseQueryReturn,
	type UseQueryStateReturn,
	type UseReportReturn,
	type UseConnectionReturn
} from './composables';

// Re-export useful types from @myko/ts
export { ConnectionStatus } from '@myko/core';
