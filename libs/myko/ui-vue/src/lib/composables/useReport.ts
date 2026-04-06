import { onUnmounted, type ShallowRef } from 'vue';
import type { Report, ReportResult } from '@myko/core';
import { getMykoClient, type ReactiveReport } from '../services/vue-client';

export interface UseReportReturn<R extends Report<unknown>> {
	/** Current value (reactive via ref) */
	value: ShallowRef<ReportResult<R> | undefined>;
	/** Manually release the subscription (also called on unmount) */
	release: () => void;
}

/**
 * Vue composable for watching a Myko report.
 *
 * Automatically releases the subscription when the component is unmounted.
 *
 * @example
 * ```vue
 * <script setup>
 *   import { useReport } from '@myko/ui-vue'
 *   import { reports } from '@rship/entities'
 *
 *   const { value } = useReport(reports.CountAllTargets({}))
 * </script>
 *
 * <template>
 *   <div>Count: {{ value?.count ?? 'loading...' }}</div>
 * </template>
 * ```
 */
export function useReport<R extends Report<unknown>>(
	reportFactory: R
): UseReportReturn<R> {
	const client = getMykoClient();
	const result = client.report(reportFactory);

	// Auto-release on unmount
	onUnmounted(() => {
		result.release();
	});

	return {
		value: result.value,
		release: result.release
	};
}
