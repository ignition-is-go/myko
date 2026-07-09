import { onUnmounted, computed, type ComputedRef, type Ref } from 'vue';
import type { Query, QueryItem } from '@myko/core';
import { getMykoClient, type ReactiveQuery } from '../services/vue-client';

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
export function useQuery<Q extends Query<unknown>>(
	queryFactory: Q
): UseQueryReturn<Q> {
	const client = getMykoClient();
	const result = client.query(queryFactory);

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
		release: result.release
	};
}
