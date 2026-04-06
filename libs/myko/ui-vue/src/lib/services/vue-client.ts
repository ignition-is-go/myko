/**
 * Vue-friendly Myko client wrapper
 *
 * Provides reactive state using Vue 3 reactivity and reactive Map for efficient updates.
 * Queries and reports are deduplicated - multiple calls with the same args share
 * the same subscription and are only cancelled when all consumers unsubscribe.
 */

import {
	ConnectionStatus,
	MykoClient,
	type Command,
	type CommandResult,
	type QueryDiff,
	type QueryItem,
	type Query,
	type ReportResult,
	type Report
} from '@myko/core';
import { ref, shallowRef, shallowReactive, type Ref, type ShallowRef } from 'vue';
import { Subject, type Observable, type Subscription } from 'rxjs';

/** Command success event */
export type CommandSuccess = {
	commandId: string;
	response: unknown;
};

/** Command error event */
export type CommandError = {
	commandId: string;
	error: Error;
};

/** Reactive query result using reactive Map - generic over the Query factory type */
export type ReactiveQuery<Q extends Query<unknown>> = {
	/** Reactive map of items by ID */
	readonly items: Map<string, QueryItem<Q> & { id: string }>;
	/** Whether the query has received its first response */
	readonly resolved: Ref<boolean>;
	/** Release this consumer's reference (unsubscribes when last consumer releases) */
	release: () => void;
};

/** Reactive report result - generic over the Report factory type */
export type ReactiveReport<R extends Report<unknown>> = {
	/** Current value (reactive via ref) */
	readonly value: ShallowRef<ReportResult<R> | undefined>;
	/** Release this consumer's reference (unsubscribes when last consumer releases) */
	release: () => void;
};

/** Internal state for a shared query */
type SharedQuery<T extends { id: string }> = {
	items: Map<string, T>;
	resolved: Ref<boolean>;
	subscription: Subscription;
	refCount: number;
};

/** Internal state for a shared report */
type SharedReport<T> = {
	value: ShallowRef<T | undefined>;
	subscription: Subscription;
	refCount: number;
};

/**
 * Vue-friendly Myko client
 *
 * Wraps MykoClient to provide reactive Vue state with automatic deduplication.
 */
export class VueMykoClient {
	private client: MykoClient;

	// Shared queries by cache key
	private sharedQueries = new Map<string, SharedQuery<{ id: string }>>();

	// Shared reports by cache key
	private sharedReports = new Map<string, SharedReport<unknown>>();

	// Command outcome subjects
	private commandSuccessSubject = new Subject<CommandSuccess>();
	private commandErrorSubject = new Subject<CommandError>();

	/** Observable of all command successes */
	readonly commandSuccess$: Observable<CommandSuccess> = this.commandSuccessSubject.asObservable();

	/** Observable of all command errors */
	readonly commandError$: Observable<CommandError> = this.commandErrorSubject.asObservable();

	// Reactive connection status
	private _connectionStatus = ref<ConnectionStatus>(ConnectionStatus.Disconnected);

	constructor() {
		this.client = new MykoClient();

		// Sync connection status to reactive state
		this.client.connectionStatus$.subscribe((status: ConnectionStatus) => {
			this._connectionStatus.value = status;
		});
	}

	/** Create a stable cache key from a query/report factory */
	private getCacheKey(
		type: 'query' | 'report',
		factory: {
			query?: Record<string, unknown>;
			report?: Record<string, unknown>;
			queryId?: string;
			reportId?: string;
		}
	): string {
		const id = type === 'query' ? factory.queryId : factory.reportId;
		const args = type === 'query' ? factory.query : factory.report;
		return `${type}:${id}:${JSON.stringify(args)}`;
	}

	/** Current connection status (reactive) */
	get connectionStatus(): Ref<ConnectionStatus> {
		return this._connectionStatus;
	}

	/** Whether currently connected (reactive) - use .value to access */
	get isConnected(): boolean {
		return this._connectionStatus.value === ConnectionStatus.Connected;
	}

	/** Set the server address and connect */
	connect(address: string): void {
		this.client.setAddress(address);
	}

	/** Disconnect from the server */
	disconnect(): void {
		// Unsubscribe all shared queries
		for (const shared of this.sharedQueries.values()) {
			shared.subscription.unsubscribe();
		}
		this.sharedQueries.clear();

		// Unsubscribe all shared reports
		for (const shared of this.sharedReports.values()) {
			shared.subscription.unsubscribe();
		}
		this.sharedReports.clear();

		this.client.disconnect();
	}

	/**
	 * Watch a query with reactive Map updates.
	 *
	 * Multiple calls with the same query args share the same Map,
	 * and the subscription is only cancelled when all consumers release.
	 *
	 * @example
	 * ```vue
	 * <script setup>
	 *   import { onUnmounted } from 'vue'
	 *   const { items, resolved, release } = client.query(queries.GetAllTargets({}))
	 *   onUnmounted(release)
	 * </script>
	 *
	 * <template>
	 *   <div v-for="[id, target] in items" :key="id">{{ target.name }}</div>
	 * </template>
	 * ```
	 */
	query<Q extends Query<unknown>>(queryFactory: Q): ReactiveQuery<Q> {
		type Item = QueryItem<Q> & { id: string };
		const cacheKey = this.getCacheKey('query', queryFactory);

		// Return existing shared query if available
		let shared = this.sharedQueries.get(cacheKey) as SharedQuery<Item> | undefined;

		if (!shared) {
			// Create new shared query with reactive Map
			const items = shallowReactive(new Map<string, Item>());
			const resolved = ref(false);

			const subscription = this.client.watchQueryDiff(queryFactory).subscribe({
				next: (diff) => {
					if (diff.sequence === 0n) {
						items.clear();
					}
					for (const id of diff.deletes) {
						items.delete(id);
					}
					for (const item of diff.upserts as Item[]) {
						items.set(item.id, item);
					}
					resolved.value = true;
				}
			});

			shared = { items, resolved, subscription, refCount: 0 };
			this.sharedQueries.set(cacheKey, shared as SharedQuery<{ id: string }>);
		}

		// Increment reference count
		shared.refCount++;

		let released = false;

		return {
			items: shared.items,
			resolved: shared.resolved,
			release: () => {
				if (released) return;
				released = true;

				const s = this.sharedQueries.get(cacheKey);
				if (s) {
					s.refCount--;
					if (s.refCount <= 0) {
						s.subscription.unsubscribe();
						this.sharedQueries.delete(cacheKey);
					}
				}
			}
		};
	}

	/**
	 * Watch a report with reactive value updates.
	 *
	 * Multiple calls with the same report args share the same subscription,
	 * and the subscription is only cancelled when all consumers release.
	 *
	 * @example
	 * ```vue
	 * <script setup>
	 *   import { onUnmounted } from 'vue'
	 *   const { value, release } = client.report(reports.CountAllTargets({}))
	 *   onUnmounted(release)
	 * </script>
	 *
	 * <template>
	 *   <div>Count: {{ value?.count ?? 'loading...' }}</div>
	 * </template>
	 * ```
	 */
	report<R extends Report<unknown>>(reportFactory: R): ReactiveReport<R> {
		type Result = ReportResult<R>;
		const cacheKey = this.getCacheKey('report', reportFactory);

		// Return existing shared report if available
		let shared = this.sharedReports.get(cacheKey) as SharedReport<Result> | undefined;

		if (!shared) {
			// Create new shared report with reactive state
			const value = shallowRef<Result | undefined>(undefined);
			const subscription = this.client.watchReport(reportFactory).subscribe({
				next: (result) => {
					value.value = result;
				}
			});

			shared = {
				value: value as ShallowRef<Result | undefined>,
				subscription,
				refCount: 0
			};
			this.sharedReports.set(cacheKey, shared as SharedReport<unknown>);
		}

		// Increment reference count (shared is guaranteed to be defined here)
		shared!.refCount++;

		let released = false;

		return {
			value: shared!.value as ShallowRef<Result | undefined>,
			release: () => {
				if (released) return;
				released = true;

				const s = this.sharedReports.get(cacheKey);
				if (s) {
					s.refCount--;
					if (s.refCount <= 0) {
						s.subscription.unsubscribe();
						this.sharedReports.delete(cacheKey);
					}
				}
			}
		};
	}

	/**
	 * Send a command and wait for the response.
	 *
	 * Emits to commandSuccess$ or commandError$ observables for generic handling.
	 *
	 * @example
	 * ```vue
	 * <script setup>
	 *   async function deleteMachine(id: string) {
	 *     const result = await myko.sendCommand(commands.DeleteMachine({ machineId: id }))
	 *     console.log('Deleted:', result)
	 *   }
	 * </script>
	 * ```
	 */
	async sendCommand<C extends Command<unknown>>(
		commandFactory: C
	): Promise<CommandResult<C>> {
		const commandId = commandFactory.commandId;
		try {
			const response = await this.client.sendCommand(commandFactory);
			this.commandSuccessSubject.next({ commandId, response });
			return response;
		} catch (e) {
			const error = e instanceof Error ? e : new Error(String(e));
			this.commandErrorSubject.next({ commandId, error });
			throw e;
		}
	}

	/** Access the underlying MykoClient for advanced use cases */
	get raw(): MykoClient {
		return this.client;
	}
}

/** Global singleton client instance (auto-initialized) */
export const myko = new VueMykoClient();

// HMR cleanup - disconnect old client when module is hot-reloaded
if (import.meta.hot) {
	import.meta.hot.dispose(() => {
		myko.disconnect();
	});
}

/** Get the global MykoClient instance */
export function getMykoClient(): VueMykoClient {
	return myko;
}

/** Create a new VueMykoClient instance (non-singleton, for advanced use) */
export function createMykoClient(): VueMykoClient {
	return new VueMykoClient();
}
