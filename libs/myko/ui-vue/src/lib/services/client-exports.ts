// Vue-friendly Myko client exports
// This module provides a cleaner import path for common client exports

export {
	createMykoClient,
	getMykoClient,
	myko,
	VueMykoClient,
	type CommandError,
	type CommandSuccess,
	type ReactiveQuery,
	type ReactiveReport
} from './vue-client';

// Vue composables
export {
	useQuery,
	useReport,
	useConnection,
	type UseQueryReturn,
	type UseReportReturn,
	type UseConnectionReturn
} from '../composables';

// Re-export useful types from @myko/ts
export { ConnectionStatus } from '@myko/ts';
