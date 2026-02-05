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

/** Command sent event (before response) */
export type CommandSent = {
	commandId: string;
};

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

/** Live report result with automatic lifecycle management */
export type LiveReport<R extends Report<unknown>> = {
	/** Current value (reactive via $state) */
	readonly value: ReportResult<R> | undefined;
	/** Current error if any */
	readonly error: Error | undefined;
};

/** Live query result with automatic lifecycle management */
export type LiveQuery<Q extends Query<unknown>> = {
	/** Reactive map of items by ID */
	readonly items: SvelteMap<string, QueryItem<Q> & { id: string }>;
	/** Whether the query has received its first response */
	readonly resolved: boolean;
	/** Current error if any */
	readonly error: Error | undefined;
};

/**
 * Svelte-friendly Myko client
 *
 * Wraps MykoClient to provide reactive Svelte state with automatic lifecycle management.
 */
export class SvelteMykoClient {
	private client: MykoClient;

	// Command lifecycle subjects
	private commandSentSubject = new Subject<CommandSent>();
	private commandSuccessSubject = new Subject<CommandSuccess>();
	private commandErrorSubject = new Subject<CommandError>();

	/** Observable of all commands when sent (before response) */
	readonly commandSent$: Observable<CommandSent> = this.commandSentSubject.asObservable();

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
				this.statsSubscription = this.client.stats().subscribe((stats: ClientStats) => {
					this.#stats = stats;
				});
			} else if (status === ConnectionStatus.Disconnected && this.statsSubscription) {
				this.statsSubscription.unsubscribe();
				this.statsSubscription = null;
				this.#stats = null;
			}
		});
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
		// Unsubscribe stats
		if (this.statsSubscription) {
			this.statsSubscription.unsubscribe();
			this.statsSubscription = null;
			this.#stats = null;
		}

		this.client.disconnect();
	}

	/**
	 * Create a live report subscription with automatic lifecycle management.
	 *
	 * The subscription automatically:
	 * - Re-subscribes when dependencies in the factory function change
	 * - Cleans up when the component unmounts
	 * - No manual release() call needed
	 *
	 * If the factory returns null/undefined, no subscription is created and value remains undefined.
	 *
	 * IMPORTANT: Must be called during component initialization (in the <script> block),
	 * not inside event handlers or other callbacks.
	 *
	 * @example
	 * ```svelte
	 * <script>
	 *   let { nodeId, sessionId } = $props()
	 *   const output = client.liveReport(() =>
	 *     sessionId ? new BindingNodeOutputValue({ nodeId, sessionId }) : null
	 *   )
	 * </script>
	 *
	 * <div>{output.value?.datagram.data}</div>
	 * ```
	 */
	liveReport<R extends Report<unknown>>(factory: () => R | null | undefined): LiveReport<R> {
		type Result = ReportResult<R>;
		let value = $state<Result | undefined>(undefined);
		let error = $state<Error | undefined>(undefined);

		$effect(() => {
			const report = factory();

			// If factory returns null/undefined, don't subscribe
			if (!report) {
				value = undefined;
				error = undefined;
				return;
			}

			error = undefined;

			const subscription = this.client.watchReport(report).subscribe({
				next: (result: Result) => {
					value = result;
					error = undefined;
				},
				error: (e) => {
					error = e instanceof Error ? e : new Error(String(e));
				}
			});

			return () => subscription.unsubscribe();
		});

		return {
			get value() {
				return value;
			},
			get error() {
				return error;
			}
		};
	}

	/**
	 * Create a live query subscription with automatic lifecycle management.
	 *
	 * The subscription automatically:
	 * - Re-subscribes when dependencies in the factory function change
	 * - Cleans up when the component unmounts
	 * - No manual release() call needed
	 *
	 * If the factory returns null/undefined, no subscription is created and items map is cleared.
	 *
	 * IMPORTANT: Must be called during component initialization (in the <script> block),
	 * not inside event handlers or other callbacks.
	 *
	 * @example
	 * ```svelte
	 * <script>
	 *   let { sceneId } = $props()
	 *   const nodes = client.liveQuery(() =>
	 *     sceneId ? new GetBindingNodesByQuery({ sceneId }) : null
	 *   )
	 * </script>
	 *
	 * {#each [...nodes.items.values()] as node}
	 *   <div>{node.name}</div>
	 * {/each}
	 * ```
	 */
	liveQuery<Q extends Query<unknown>>(factory: () => Q | null | undefined): LiveQuery<Q> {
		type Item = QueryItem<Q> & { id: string };
		const items = new SvelteMap<string, Item>();
		let resolved = $state(false);
		let error = $state<Error | undefined>(undefined);

		$effect(() => {
			const query = factory();

			// If factory returns null/undefined, clear and don't subscribe
			if (!query) {
				items.clear();
				resolved = false;
				error = undefined;
				return;
			}

			error = undefined;

			const subscription = this.client.watchQueryDiff(query).subscribe({
				next: (diff) => {
					if (diff.sequence === 0n) {
						items.clear();
					}
					for (const id of diff.deletes) {
						items.delete(id);
					}
					for (const item of diff.upserts) {
						const typedItem = item as Item;
						items.set(typedItem.id, typedItem);
					}
					resolved = true;
					error = undefined;
				},
				error: (e) => {
					error = e instanceof Error ? e : new Error(String(e));
				}
			});

			return () => {
				subscription.unsubscribe();
				items.clear();
				resolved = false;
			};
		});

		return {
			items,
			get resolved() {
				return resolved;
			},
			get error() {
				return error;
			}
		};
	}

	/**
	 * Watch a query with Observable-based updates.
	 *
	 * Returns an Observable that emits the full array of items whenever
	 * the result set changes.
	 *
	 * @deprecated Use `liveQuery()` for component usage. This method requires manual
	 * subscription management and doesn't integrate with Svelte's lifecycle.
	 * Only use for non-component contexts (e.g., context classes).
	 */
	watchQuery<Q extends Query<unknown>>(queryFactory: Q): Observable<QueryResult<Q>> {
		return this.client.watchQuery(queryFactory);
	}

	/**
	 * Watch a report with Observable-based updates.
	 *
	 * Returns an Observable that emits whenever the report result changes.
	 *
	 * @deprecated Use `liveReport()` for component usage. This method requires manual
	 * subscription management and doesn't integrate with Svelte's lifecycle.
	 * Only use for non-component contexts (e.g., context classes).
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
		this.commandSentSubject.next({ commandId });
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
