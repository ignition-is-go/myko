import { onUnmounted, computed, type ComputedRef, type Ref, type ShallowRef } from 'vue';
import type { CollectionChanges, LiveCollectionStatus, Query, QueryItem } from '@myko/core';
import { getMykoClient } from '../services/vue-client';

export interface UseQueryReturn<Q extends Query<unknown>> {
	/** Reactive map of items by ID */
	items: Map<string, QueryItem<Q> & { id: string }>;
	/** Array of all items (computed for convenience) */
	itemsArray: ComputedRef<(QueryItem<Q> & { id: string })[]>;
	/** Whether the query has received its first response */
	resolved: Ref<boolean>;
	/** Manually release the subscription (also called on unmount) */
	release: () => void;
}

export interface UseQueryStateReturn<Q extends Query<unknown>> extends UseQueryReturn<Q> {
	/** Loading, live, or terminal error state. */
	status: Ref<LiveCollectionStatus>;
	/** Latest subscription error while retaining the last live data. */
	error: ShallowRef<Error | undefined>;
	/** Monotonic local revision updated for each diff or error. */
	revision: Ref<number>;
	/** Latest reset/upsert/delete set for row-oriented consumers. */
	changes: ShallowRef<CollectionChanges<QueryItem<Q> & { id: string }>>;
}

/**
 * Vue composable for watching a Myko query.
 *
 * Automatically releases the subscription when the component is unmounted.
 *
 * @example
 * ```vue
 * <script setup>
 *   import { useQuery } from '@myko/ui-vue'
 *   import { queries } from '@your-app/entities'
 *
 *   const { items, itemsArray, resolved } = useQuery(queries.GetAllTargets({}))
 * </script>
 *
 * <template>
 *   <div v-if="!resolved">Loading...</div>
 *   <div v-for="target in itemsArray" :key="target.id">
 *     {{ target.name }}
 *   </div>
 * </template>
 * ```
 */
export function useQuery<Q extends Query<unknown>>(queryFactory: Q): UseQueryReturn<Q> {
	return useQueryState(queryFactory);
}

/** Vue query composable with explicit lifecycle and incremental change metadata. */
export function useQueryState<Q extends Query<unknown>>(queryFactory: Q): UseQueryStateReturn<Q> {
	const client = getMykoClient();
	const result = client.queryState(queryFactory);

	// Auto-release on unmount
	onUnmounted(() => {
		result.release();
	});

	// Convenience computed for array iteration
	const itemsArray = computed(() => Array.from(result.items.values()));

	return {
		items: result.items,
		itemsArray,
		resolved: result.resolved,
		status: result.status,
		error: result.error,
		revision: result.revision,
		changes: result.changes,
		release: result.release
	};
}
