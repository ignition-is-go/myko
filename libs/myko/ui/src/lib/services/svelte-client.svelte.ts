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
	type CommandReturn,
	type QueryDiff,
	type QueryItem,
	type QueryReturn,
	type ReportResult,
	type ReportReturn
} from '@myko/ts';
import { SvelteMap } from 'svelte/reactivity';
import type { Subscription } from 'rxjs';

/** Reactive query result using SvelteMap */
export type ReactiveQuery<T extends { id: string }> = {
	/** Reactive map of items by ID */
	readonly items: SvelteMap<string, T>;
	/** Release this consumer's reference (unsubscribes when last consumer releases) */
	release: () => void;
};

/** Reactive report result */
export type ReactiveReport<T> = {
	/** Current value (reactive via $state) */
	readonly value: T | undefined;
	/** Release this consumer's reference (unsubscribes when last consumer releases) */
	release: () => void;
};

/** Internal state for a shared query */
type SharedQuery<T extends { id: string }> = {
	items: SvelteMap<string, T>;
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

	// Reactive connection status
	#connectionStatus = $state<ConnectionStatus>(ConnectionStatus.Disconnected);

	constructor() {
		this.client = new MykoClient();

		// Sync connection status to reactive state
		this.client.connectionStatus$.subscribe((status) => {
			this.#connectionStatus = status;
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
	 * Watch a query with reactive SvelteMap updates.
	 *
	 * Multiple calls with the same query args share the same SvelteMap,
	 * and the subscription is only cancelled when all consumers release.
	 *
	 * @example
	 * ```svelte
	 * <script>
	 *   const { items, release } = client.query(queries.GetAllTargets({}))
	 *   onDestroy(release)
	 * </script>
	 *
	 * {#each [...items.values()] as target}
	 *   <div>{target.name}</div>
	 * {/each}
	 * ```
	 */
	query<Q extends QueryReturn<unknown>>(
		queryFactory: Q
	): ReactiveQuery<QueryItem<Q> & { id: string }> {
		type Item = QueryItem<Q> & { id: string };
		const cacheKey = this.getCacheKey('query', queryFactory);

		// Return existing shared query if available
		let shared = this.sharedQueries.get(cacheKey) as SharedQuery<Item> | undefined;

		if (!shared) {
			// Create new shared query
			const items = new SvelteMap<string, Item>();

			const subscription = this.client.watchQueryDiff(queryFactory).subscribe({
				next: (diff: QueryDiff<Item>) => {
					if (diff.sequence === 0n) {
						items.clear();
					}
					for (const id of diff.deletes) {
						items.delete(id);
					}
					for (const item of diff.upserts) {
						items.set(item.id, item);
					}
				}
			});

			shared = { items, subscription, refCount: 0 };
			this.sharedQueries.set(cacheKey, shared as SharedQuery<{ id: string }>);
		}

		// Increment reference count
		shared.refCount++;

		let released = false;

		return {
			items: shared.items,
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
	 *   const count = client.report(reports.CountAllTargets({}))
	 *   onDestroy(count.release)
	 * </script>
	 *
	 * <div>Count: {count.value?.count ?? 'loading...'}</div>
	 * ```
	 */
	report<R extends ReportReturn<unknown>>(reportFactory: R): ReactiveReport<ReportResult<R>> {
		type Result = ReportResult<R>;
		const cacheKey = this.getCacheKey('report', reportFactory);

		// Return existing shared report if available
		let shared = this.sharedReports.get(cacheKey) as SharedReport<Result> | undefined;

		if (!shared) {
			// Create new shared report with reactive state
			let value = $state<Result | undefined>(undefined);

			const subscription = this.client.watchReport(reportFactory).subscribe({
				next: (result) => {
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
	 * Send a command and wait for the response.
	 *
	 * @example
	 * ```svelte
	 * <script>
	 *   async function deleteMachine(id: string) {
	 *     const result = await myko.sendCommand(commands.DeleteMachine({ machineId: id }))
	 *     console.log('Deleted:', result)
	 *   }
	 * </script>
	 * ```
	 */
	sendCommand<C extends CommandReturn<unknown>>(
		commandFactory: C
	): Promise<C extends CommandReturn<infer R> ? R : unknown> {
		return this.client.sendCommand(commandFactory);
	}

	/** Access the underlying MykoClient for advanced use cases */
	get raw(): MykoClient {
		return this.client;
	}
}

/** Global singleton client instance (auto-initialized) */
export const myko = new SvelteMykoClient();

/** Get the global MykoClient instance */
export function getMykoClient(): SvelteMykoClient {
	return myko;
}

/** Create a new SvelteMykoClient instance (non-singleton, for advanced use) */
export function createMykoClient(): SvelteMykoClient {
	return new SvelteMykoClient();
}
