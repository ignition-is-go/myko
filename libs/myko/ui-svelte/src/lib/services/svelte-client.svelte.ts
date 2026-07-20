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
	MykoProtocol,
	type ClientStats,
	type Command,
	type CommandResult,
	type MykoError,
	type Query,
	type QueryWindow,
	type QueryWatchOptions,
	type QueryDiff,
	type QueryItem,
	type QueryWindowInfo,
	type QueryResult,
	type View,
	type ViewItem,
	type ViewResult,
	type Report,
	type ReportResult
} from '@myko/core';
import { SvelteMap } from 'svelte/reactivity';
import { Subject, type Observable, type Subscription } from 'rxjs';
import { untrack } from 'svelte';

function stableStringify(value: unknown): string | null {
	const seen = new WeakSet<object>();
	try {
		return JSON.stringify(value, (_key, raw) => {
			if (typeof raw === 'bigint') {
				return `__bigint:${raw.toString()}`;
			}
			if (!raw || typeof raw !== 'object') {
				return raw;
			}
			if (seen.has(raw)) {
				return '__circular__';
			}
			seen.add(raw);
			if (Array.isArray(raw)) {
				return raw;
			}
			const sorted: Record<string, unknown> = {};
			for (const key of Object.keys(raw as Record<string, unknown>).sort()) {
				sorted[key] = (raw as Record<string, unknown>)[key];
			}
			return sorted;
		});
	} catch {
		return null;
	}
}

function requestKey(kind: 'query' | 'view' | 'report', request: unknown): string | null {
	const serialized = stableStringify(request);
	return serialized ? `${kind}:${serialized}` : null;
}

/**
 * Frame-paced callback dispatcher for incoming report values.
 *
 * Default behaviour: every queued callback runs on the next animation frame.
 * This already coalesces multiple updates per anchor into a single Svelte
 * effect cascade per frame and combines bursts spanning several WS frames.
 *
 * Burst handling: when the queue grows past `BURST_THRESHOLD` (i.e. an
 * initial scene load lands hundreds of pending updates at once), the flush
 * processes only `BURST_BATCH` callbacks per frame and reschedules the rest.
 * That spreads the cost across multiple frames so the main thread can paint
 * between chunks. Steady-state high-frequency updates stay below the
 * threshold and run every frame with no added lag.
 *
 * This logic belongs in the Svelte client (browser-specific) — the core
 * MykoClient stays runtime-agnostic.
 */
const BURST_THRESHOLD = 96;
const BURST_BATCH = 32;
let pendingValueCallbacks: Array<() => void> = [];
let valueFlushScheduled = false;

function scheduleFlush(): void {
	if (valueFlushScheduled) return;
	valueFlushScheduled = true;
	if (typeof requestAnimationFrame === 'function') {
		requestAnimationFrame(flushPendingValues);
	} else {
		setTimeout(flushPendingValues, 0);
	}
}

function scheduleValueDispatch(cb: () => void): void {
	pendingValueCallbacks.push(cb);
	scheduleFlush();
}

function flushPendingValues(): void {
	valueFlushScheduled = false;
	if (pendingValueCallbacks.length <= BURST_THRESHOLD) {
		// Normal path: process everything queued this frame. Per-anchor
		// coalescing in liveReport keeps the queue size proportional to the
		// number of distinct anchors with new data, so this scales naturally.
		const drained = pendingValueCallbacks;
		pendingValueCallbacks = [];
		for (const cb of drained) cb();
		return;
	}

	// Burst: spread across multiple frames so the user sees mounted nodes
	// before the entire value cascade lands.
	const batch = pendingValueCallbacks.splice(0, BURST_BATCH);
	for (const cb of batch) cb();
	if (pendingValueCallbacks.length > 0) scheduleFlush();
}

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

/** Live windowed query result with automatic lifecycle management */
export type LiveWindowedQuery<Q extends Query<unknown>> = {
	/** Current windowed items */
	readonly items: QueryResult<Q>;
	/** Current reported total count */
	readonly totalCount: number | null;
	/** Current applied server window */
	readonly window: QueryWindow | null;
	/** Whether first response has been received */
	readonly resolved: boolean;
	/** Current error if any */
	readonly error: Error | undefined;
	/** Update server-side window without re-subscribing */
	setWindow(window: QueryWindow | null): void;
};

/** Live view result with automatic lifecycle management */
export type LiveView<V extends View<unknown>> = {
	readonly items: SvelteMap<string, ViewItem<V> & { id: string }>;
	readonly resolved: boolean;
	readonly error: Error | undefined;
};

/** Live windowed view result with automatic lifecycle management */
export type LiveWindowedView<V extends View<unknown>> = {
	readonly items: ViewResult<V>;
	readonly totalCount: number | null;
	readonly window: QueryWindow | null;
	readonly resolved: boolean;
	readonly error: Error | undefined;
	setWindow(window: QueryWindow | null): void;
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

	/** Set the wire protocol (JSON or CBOR). Default is JSON. */
	setProtocol(protocol: MykoProtocol): void {
		this.client.setProtocol(protocol);
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
		const reportKey = $derived.by(() => {
			const report = factory();
			if (!report) return null;
			return requestKey('report', report);
		});

		// Use the direct-callback subscribeReport instead of `watchReport().subscribe`.
		// The Observable form wraps the per-tx Subject in `pipe(map, finalize,
		// shareReplay, map)` — each subscriber walks that operator chain on
		// subscribe, and a workspace with hundreds of anchor views was paying
		// measurable cost in RxJS plumbing during scene mount.
		//
		// Cell writes (`value = result`) are routed through the frame-paced
		// dispatcher so a burst of incoming responses doesn't all cascade
		// through Svelte effects in a single task.
		$effect(() => {
			const key = reportKey;
			const report = untrack(factory);

			if (!report) {
				value = undefined;
				error = undefined;
				return;
			}
			if (!key) {
				value = undefined;
				error = new Error('Failed to compute stable report key');
				return;
			}

			error = undefined;

			let disposed = false;
			let latestResult: Result | undefined = undefined;
			let hasPending = false;

			const dispose = this.client.subscribeReport(
				report,
				(result: Result) => {
					if (disposed) return;
					latestResult = result;
					if (hasPending) return;
					hasPending = true;
					scheduleValueDispatch(() => {
						hasPending = false;
						if (disposed) return;
						value = latestResult;
						error = undefined;
					});
				},
				(e: Error) => {
					if (disposed) return;
					error = e;
				}
			);

			return () => {
				disposed = true;
				dispose();
			};
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
	 *     sceneId ? new GetBindingNodesByQuery({ sceneId: { kind: "eq", value: sceneId } }) : null
	 *   )
	 * </script>
	 *
	 * {#each nodes.items as [id, node] (id)}
	 *   <div>{node.name}</div>
	 * {/each}
	 * ```
	 */
	liveQuery<Q extends Query<unknown>>(factory: () => Q | null | undefined): LiveQuery<Q> {
		type Item = QueryItem<Q> & { id: string };
		const items = new SvelteMap<string, Item>();
		let resolved = $state(false);
		let error = $state<Error | undefined>(undefined);
		const queryKey = $derived.by(() => {
			const query = factory();
			if (!query) return null;
			return requestKey('query', query);
		});

		$effect(() => {
			const key = queryKey;
			const query = untrack(factory);

			// If factory returns null/undefined, clear and don't subscribe
			if (!query) {
				items.clear();
				resolved = false;
				error = undefined;
				return;
			}
			if (!key) {
				items.clear();
				resolved = false;
				error = new Error('Failed to compute stable query key');
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

	/** Create a live view subscription with automatic lifecycle management. */
	liveView<V extends View<unknown>>(factory: () => V | null | undefined): LiveView<V> {
		type Item = ViewItem<V> & { id: string };
		const items = new SvelteMap<string, Item>();
		let resolved = $state(false);
		let error = $state<Error | undefined>(undefined);
		const viewKey = $derived.by(() => {
			const view = factory();
			if (!view) return null;
			return requestKey('view', view);
		});

		$effect(() => {
			const key = viewKey;
			const view = untrack(factory);
			if (!view) {
				items.clear();
				resolved = false;
				error = undefined;
				return;
			}
			if (!key) {
				items.clear();
				resolved = false;
				error = new Error('Failed to compute stable view key');
				return;
			}

			error = undefined;
			const subscription = this.client.watchViewDiff(view).subscribe({
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
	 * Create a live windowed query with automatic lifecycle management.
	 *
	 * Uses server-side query windows while exposing Svelte-native reactive state.
	 */
	liveQueryWindowed<Q extends Query<unknown>>(
		factory: () => Q | null | undefined,
		optionsFactory?: () => QueryWatchOptions | undefined
	): LiveWindowedQuery<Q> {
		type Result = QueryResult<Q>;
		let items = $state<Result>([] as unknown as Result);
		let totalCount = $state<number | null>(null);
		let window = $state<QueryWindow | null>(null);
		let resolved = $state(false);
		let error = $state<Error | undefined>(undefined);
		let setWindowFn: (window: QueryWindow | null) => void = () => {};
		const queryWindowKey = $derived.by(() => {
			const query = factory();
			if (!query) return null;
			const q = requestKey('query', query);
			const o = stableStringify(optionsFactory?.());
			if (!q) return null;
			return `${q}|options:${o ?? '__none__'}`;
		});

		$effect(() => {
			const key = queryWindowKey;
			const query = untrack(factory);
			const options = untrack(() => optionsFactory?.());

			if (!query) {
				items = [] as unknown as Result;
				totalCount = null;
				window = null;
				resolved = false;
				error = undefined;
				setWindowFn = () => {};
				return;
			}
			if (!key) {
				items = [] as unknown as Result;
				totalCount = null;
				window = null;
				resolved = false;
				error = new Error('Failed to compute stable windowed query key');
				setWindowFn = () => {};
				return;
			}

			error = undefined;
			resolved = false;

			const handle = this.client.watchQueryWindowed(query, options);
			setWindowFn = handle.setWindow;

			const itemsSub = handle.results$.subscribe({
				next: (next) => {
					items = next as Result;
					resolved = true;
					error = undefined;
				},
				error: (e) => {
					error = e instanceof Error ? e : new Error(String(e));
				}
			});

			const infoSub = handle.windowInfo$.subscribe({
				next: (info) => {
					totalCount = info.totalCount;
					window = info.window;
					resolved = true;
					error = undefined;
				},
				error: (e) => {
					error = e instanceof Error ? e : new Error(String(e));
				}
			});

			return () => {
				itemsSub.unsubscribe();
				infoSub.unsubscribe();
				items = [] as unknown as Result;
				totalCount = null;
				window = null;
				resolved = false;
				setWindowFn = () => {};
			};
		});

		return {
			get items() {
				return items;
			},
			get totalCount() {
				return totalCount;
			},
			get window() {
				return window;
			},
			get resolved() {
				return resolved;
			},
			get error() {
				return error;
			},
			setWindow(nextWindow: QueryWindow | null) {
				setWindowFn(nextWindow);
			}
		};
	}

	/** Create a live windowed view with automatic lifecycle management. */
	liveViewWindowed<V extends View<unknown>>(
		factory: () => V | null | undefined,
		optionsFactory?: () => QueryWatchOptions | undefined
	): LiveWindowedView<V> {
		type Result = ViewResult<V>;
		let items = $state<Result>([] as unknown as Result);
		let totalCount = $state<number | null>(null);
		let window = $state<QueryWindow | null>(null);
		let resolved = $state(false);
		let error = $state<Error | undefined>(undefined);
		let setWindowFn: (window: QueryWindow | null) => void = () => {};
		const viewWindowKey = $derived.by(() => {
			const view = factory();
			if (!view) return null;
			const v = requestKey('view', view);
			const o = stableStringify(optionsFactory?.());
			if (!v) return null;
			return `${v}|options:${o ?? '__none__'}`;
		});

		$effect(() => {
			const key = viewWindowKey;
			const view = untrack(factory);
			const options = untrack(() => optionsFactory?.());

			if (!view) {
				items = [] as unknown as Result;
				totalCount = null;
				window = null;
				resolved = false;
				error = undefined;
				setWindowFn = () => {};
				return;
			}
			if (!key) {
				items = [] as unknown as Result;
				totalCount = null;
				window = null;
				resolved = false;
				error = new Error('Failed to compute stable windowed view key');
				setWindowFn = () => {};
				return;
			}

			error = undefined;
			resolved = false;

			const handle = this.client.watchViewWindowed(view, options);
			setWindowFn = handle.setWindow;

			const itemsSub = handle.results$.subscribe({
				next: (next) => {
					items = next as Result;
					resolved = true;
					error = undefined;
				},
				error: (e) => {
					error = e instanceof Error ? e : new Error(String(e));
				}
			});

			const infoSub = handle.windowInfo$.subscribe({
				next: (info) => {
					totalCount = info.totalCount;
					window = info.window;
					resolved = true;
					error = undefined;
				},
				error: (e) => {
					error = e instanceof Error ? e : new Error(String(e));
				}
			});

			return () => {
				itemsSub.unsubscribe();
				infoSub.unsubscribe();
				items = [] as unknown as Result;
				totalCount = null;
				window = null;
				resolved = false;
				setWindowFn = () => {};
			};
		});

		return {
			get items() {
				return items;
			},
			get totalCount() {
				return totalCount;
			},
			get window() {
				return window;
			},
			get resolved() {
				return resolved;
			},
			get error() {
				return error;
			},
			setWindow(nextWindow: QueryWindow | null) {
				setWindowFn(nextWindow);
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
	 * Watch a view with Observable-based updates.
	 *
	 * @deprecated Use `liveView()` for component usage. Kept for backward compatibility
	 * with existing service code paths that still consume observables.
	 */
	watchView<V extends View<unknown>>(viewFactory: V): Observable<ViewResult<V>> {
		return this.client.watchView(viewFactory);
	}

	/**
	 * Watch a query with optional server-side windowing.
	 *
	 * @deprecated Use `liveQuery()` (or `liveQueryWindowed()` for runes-native windowing)
	 * in component/service rune contexts.
	 */
	watchQueryWithOptions<Q extends Query<unknown>>(
		queryFactory: Q,
		options?: QueryWatchOptions
	): Observable<QueryResult<Q>> {
		return this.client.watchQuery(queryFactory, options);
	}

	/**
	 * Watch a view with optional server-side windowing.
	 *
	 * @deprecated Use `liveView()` in component/service rune contexts.
	 */
	watchViewWithOptions<V extends View<unknown>>(
		viewFactory: V,
		options?: QueryWatchOptions
	): Observable<ViewResult<V>> {
		return this.client.watchView(viewFactory, options);
	}

	/**
	 * Watch a query and mutate its server-side window without re-subscribing.
	 *
	 * @deprecated Use `liveQueryWindowed()` in component/service rune contexts.
	 */
	watchQueryWindowed<Q extends Query<unknown>>(
		queryFactory: Q,
		options?: QueryWatchOptions
	): {
		tx: string;
		results$: Observable<QueryResult<Q>>;
		windowInfo$: Observable<QueryWindowInfo>;
		setWindow: (window: QueryWindow | null) => void;
	} {
		return this.client.watchQueryWindowed(queryFactory, options);
	}

	/**
	 * Watch a view and mutate its server-side window without re-subscribing.
	 *
	 * @deprecated Use `liveView()` in component/service rune contexts.
	 */
	watchViewWindowed<V extends View<unknown>>(
		viewFactory: V,
		options?: QueryWatchOptions
	): {
		tx: string;
		results$: Observable<ViewResult<V>>;
		windowInfo$: Observable<QueryWindowInfo>;
		setWindow: (window: QueryWindow | null) => void;
	} {
		return this.client.watchViewWindowed(viewFactory, options);
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
	async sendCommand<C extends Command<unknown>>(commandFactory: C): Promise<CommandResult<C>> {
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
