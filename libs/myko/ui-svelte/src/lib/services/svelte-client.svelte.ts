/**
 * Svelte-friendly Myko client wrapper
 *
 * Provides reactive state using Svelte 5 runes and SvelteMap for efficient updates.
 * Queries and reports are deduplicated - multiple calls with the same args share
 * the same subscription and are only cancelled when all consumers unsubscribe.
 */

import {
	ConnectionStatus,
	MykoClient,
	type ClientStats,
	type Command,
	type CommandResult,
	type MykoError,
	type Query,
	type QueryDiff,
	type QueryItem,
	type QueryResult,
	type Report,
	type ReportResult
} from '@myko/ts';
import { SvelteMap } from 'svelte/reactivity';
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

/** Reactive query result using SvelteMap - generic over the Query class type */
export type ReactiveQuery<Q extends Query<unknown>> = {
	/** Reactive map of items by ID */
	readonly items: SvelteMap<string, QueryItem<Q> & { id: string }>;
	/** Whether the query has received its first response */
	readonly resolved: boolean;
	/** Release this consumer's reference (unsubscribes when last consumer releases) */
	release: () => void;
};

/** Reactive report result - generic over the Report class type */
export type ReactiveReport<R extends Report<unknown>> = {
	/** Current value (reactive via $state) */
	readonly value: ReportResult<R> | undefined;
	/** Release this consumer's reference (unsubscribes when last consumer releases) */
	release: () => void;
};

/** Internal state for a shared query */
type SharedQuery<T extends { id: string }> = {
	items: SvelteMap<string, T>;
	getResolved: () => boolean;
	subscription: Subscription;
	refCount: number;
};

/** Internal state for a shared report */
type SharedReport<T> = {
	getValue: () => T | undefined;
	subscription: Subscription;
	refCount: number;
};

/**
 * Svelte-friendly Myko client
 *
 * Wraps MykoClient to provide reactive Svelte state with automatic deduplication.
 */
export class SvelteMykoClient {
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
	#connectionStatus = $state<ConnectionStatus>(ConnectionStatus.Disconnected);

	// Reactive connection stats
	#stats = $state<ClientStats | null>(null);
	private statsSubscription: Subscription | null = null;

	constructor() {
		this.client = new MykoClient();

		// Sync connection status to reactive state
		this.client.connectionStatus$.subscribe((status: ConnectionStatus) => {
			this.#connectionStatus = status;

			// Start/stop stats subscription based on connection
			if (status === ConnectionStatus.Connected && !this.statsSubscription) {
				this.statsSubscription = this.client.stats().subscribe((stats) => {
					this.#stats = stats;
				});
			} else if (status === ConnectionStatus.Disconnected && this.statsSubscription) {
				this.statsSubscription.unsubscribe();
				this.statsSubscription = null;
				this.#stats = null;
			}
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
	get connectionStatus(): ConnectionStatus {
		return this.#connectionStatus;
	}

	/** Whether currently connected (reactive) */
	get isConnected(): boolean {
		return this.#connectionStatus === ConnectionStatus.Connected;
	}

	/** Current connection stats (reactive, null when disconnected) */
	get stats(): ClientStats | null {
		return this.#stats;
	}

	/** Set the server address and connect */
	connect(address: string): void {
		this.client.setAddress(address);
	}

	/**
	 * Enable automatic peer discovery via GetPeerServers query.
	 * When enabled, the client will automatically add discovered peer servers
	 * to the connection pool for redundancy and load balancing.
	 */
	enablePeerDiscovery(enabled: boolean, secure = false): void {
		this.client.enablePeerDiscovery(enabled, secure);
	}

	/** Set authentication token for commands */
	setToken(token: string | null): void {
		this.client.setToken(token);
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

		// Unsubscribe stats
		if (this.statsSubscription) {
			this.statsSubscription.unsubscribe();
			this.statsSubscription = null;
			this.#stats = null;
		}

		this.client.disconnect();
	}

	/**
	 * Watch a query with reactive SvelteMap updates.
	 *
	 * Multiple calls with the same query args share the same SvelteMap,
	 * and the subscription is only cancelled when all consumers release.
	 *
	 * @example
	 * ```svelte
	 * <script>
	 *   const { items, release } = client.query(new GetAllTargets({}))
	 *   onDestroy(release)
	 * </script>
	 *
	 * {#each [...items.values()] as target}
	 *   <div>{target.name}</div>
	 * {/each}
	 * ```
	 */
	query<Q extends Query<unknown>>(queryFactory: Q): ReactiveQuery<Q> {
		type Item = QueryItem<Q> & { id: string };
		const cacheKey = this.getCacheKey('query', queryFactory);

		// Return existing shared query if available
		let shared = this.sharedQueries.get(cacheKey) as SharedQuery<Item> | undefined;

		if (!shared) {
			// Create new shared query
			const items = new SvelteMap<string, Item>();
			let resolved = $state(false);

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
					resolved = true;
				}
			});

			shared = { items, getResolved: () => resolved, subscription, refCount: 0 };
			this.sharedQueries.set(cacheKey, shared as SharedQuery<{ id: string }>);
		}

		// Increment reference count
		shared.refCount++;

		let released = false;
		const getResolved = shared.getResolved;

		return {
			items: shared.items,
			get resolved() {
				return getResolved();
			},
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
	 * ```svelte
	 * <script>
	 *   const count = client.report(new CountAllTargets({}))
	 *   onDestroy(count.release)
	 * </script>
	 *
	 * <div>Count: {count.value?.count ?? 'loading...'}</div>
	 * ```
	 */
	report<R extends Report<unknown>>(reportFactory: R): ReactiveReport<R> {
		type Result = ReportResult<R>;
		const cacheKey = this.getCacheKey('report', reportFactory);

		// Return existing shared report if available
		let shared = this.sharedReports.get(cacheKey) as SharedReport<Result> | undefined;

		if (!shared) {
			// Create new shared report with reactive state
			let value = $state<Result | undefined>(undefined);
			const subscription = this.client.watchReport(reportFactory).subscribe({
				next: (result: Result) => {
					value = result;
				}
			});

			shared = {
				getValue: () => value,
				subscription,
				refCount: 0
			};
			this.sharedReports.set(cacheKey, shared as SharedReport<unknown>);
		}

		// Increment reference count
		shared.refCount++;

		let released = false;
		const getValue = shared.getValue;

		return {
			get value() {
				return getValue();
			},
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
	 * Watch a query with Observable-based updates.
	 *
	 * Returns an Observable that emits the full array of items whenever
	 * the result set changes. For Svelte-optimized reactive state, use
	 * the `query()` method instead.
	 *
	 * @example
	 * ```svelte
	 * <script>
	 *   const targets$ = client.watchQuery(new GetAllTargets({}))
	 *   targets$.subscribe(targets => console.log('Got', targets.length, 'targets'))
	 * </script>
	 * ```
	 */
	watchQuery<Q extends Query<unknown>>(queryFactory: Q): Observable<QueryResult<Q>> {
		return this.client.watchQuery(queryFactory);
	}

	/**
	 * Watch a report with Observable-based updates.
	 *
	 * Returns an Observable that emits whenever the report result changes.
	 * For Svelte-optimized reactive state, use the `report()` method instead.
	 *
	 * @example
	 * ```svelte
	 * <script>
	 *   const count$ = client.watchReport(new CountAllTargets({}))
	 *   count$.subscribe(result => console.log('Count:', result.count))
	 * </script>
	 * ```
	 */
	watchReport<R extends Report<unknown>>(reportFactory: R): Observable<ReportResult<R>> {
		return this.client.watchReport(reportFactory);
	}

	/**
	 * Send a command and wait for the response.
	 *
	 * Emits to commandSuccess$ or commandError$ observables for generic handling.
	 *
	 * @example
	 * ```svelte
	 * <script>
	 *   async function deleteMachine(id: string) {
	 *     const result = await myko.sendCommand(new DeleteMachine({ id }))
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

	/** Measure round-trip latency */
	ping(): Promise<number> {
		return this.client.ping();
	}

	/** Observable of all errors from the server */
	get errors(): Observable<MykoError> {
		return this.client.errors$;
	}

	/**
	 * Watch command completion status.
	 * Note: This is a compatibility shim - the underlying MykoClient doesn't track
	 * command completions the same way as the legacy WSMClient.
	 */
	watchCommandStatus(): Observable<unknown[]> {
		// Return empty observable - command tracking is handled via sendCommand promises
		return new Subject<unknown[]>().asObservable();
	}

	/**
	 * Clear a command completion from tracking.
	 * Note: This is a compatibility shim - no-op in the new client.
	 */
	clearCommandCompletion(_tx: string): void {
		// No-op - command tracking is handled via sendCommand promises
	}
}

/** Global singleton client instance (auto-initialized) */
export const myko = new SvelteMykoClient();

// HMR cleanup - disconnect old client when module is hot-reloaded
if (import.meta.hot) {
	import.meta.hot.dispose(() => {
		myko.disconnect();
	});
}

/** Get the global MykoClient instance */
export function getMykoClient(): SvelteMykoClient {
	return myko;
}

/** Create a new SvelteMykoClient instance (non-singleton, for advanced use) */
export function createMykoClient(): SvelteMykoClient {
	return new SvelteMykoClient();
}
