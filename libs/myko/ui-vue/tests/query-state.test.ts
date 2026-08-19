import { describe, expect, test } from 'bun:test';
import type { Subject } from 'rxjs';

import type { MykoClient, Query } from '@myko/core';
import { VueMykoClient } from '../src/lib/services/vue-client';

type Item = { id: string; value: number };

type WrapperInternals = { client: MykoClient };
type ClientInternals = {
	activeQueries: Map<string, unknown>;
	queryResponseRoutes: Map<string, Subject<unknown>>;
	queryErrorRoutes: Map<string, Subject<unknown>>;
};

const query: Query<Item> = {
	queryId: 'ItemsByQuery',
	queryItemType: 'Item',
	query: {}
};

describe('VueMykoClient queryState', () => {
	test('exposes reactive lifecycle and incremental change metadata', () => {
		const wrapper = new VueMykoClient();
		const state = wrapper.queryState(query);
		const client = (wrapper as unknown as WrapperInternals).client;
		const internals = client as unknown as ClientInternals;
		const tx = [...internals.activeQueries.keys()][0];

		expect(state.status.value).toBe('loading');
		internals.queryResponseRoutes.get(tx)?.next({
			event: 'ws:m:query-response',
			data: {
				tx,
				sequence: '0',
				deletes: [],
				upserts: [{ itemType: 'Item', item: { id: 'a', value: 1 } }]
			}
		});

		expect(state.status.value).toBe('live');
		expect(state.resolved.value).toBe(true);
		expect(state.revision.value).toBe(1);
		expect(state.items.get('a')).toEqual({ id: 'a', value: 1 });
		expect(state.changes.value).toMatchObject({ sequence: 0n, reset: true });

		internals.queryErrorRoutes.get(tx)?.next({
			event: 'ws:m:query-error',
			data: { tx, message: 'query failed' }
		});
		expect(state.status.value).toBe('error');
		expect(state.error.value?.message).toBe('query failed');
		expect(state.items.get('a')).toEqual({ id: 'a', value: 1 });

		state.release();
		wrapper.disconnect();
	});
});
