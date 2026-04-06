import { computed, type ComputedRef, type Ref } from 'vue';
import { ConnectionStatus } from '@myko/core';
import { getMykoClient } from '../services/vue-client';

export interface UseConnectionReturn {
	/** Current connection status (reactive) */
	status: Ref<ConnectionStatus>;
	/** Whether currently connected (computed) */
	isConnected: ComputedRef<boolean>;
	/** Whether currently connecting (computed) */
	isConnecting: ComputedRef<boolean>;
	/** Whether currently disconnected (computed) */
	isDisconnected: ComputedRef<boolean>;
	/** Connect to a server address */
	connect: (address: string) => void;
	/** Disconnect from the server */
	disconnect: () => void;
}

/**
 * Vue composable for managing Myko connection status.
 *
 * @example
 * ```vue
 * <script setup>
 *   import { useConnection } from '@myko/ui-vue'
 *
 *   const { status, isConnected, connect, disconnect } = useConnection()
 *
 *   function handleConnect() {
 *     connect('ws://localhost:5155')
 *   }
 * </script>
 *
 * <template>
 *   <div>Status: {{ status }}</div>
 *   <button v-if="!isConnected" @click="handleConnect">Connect</button>
 *   <button v-else @click="disconnect">Disconnect</button>
 * </template>
 * ```
 */
export function useConnection(): UseConnectionReturn {
	const client = getMykoClient();

	const isConnected = computed(() => client.connectionStatus.value === ConnectionStatus.Connected);
	const isConnecting = computed(() => client.connectionStatus.value === ConnectionStatus.Connecting);
	const isDisconnected = computed(() => client.connectionStatus.value === ConnectionStatus.Disconnected);

	return {
		status: client.connectionStatus,
		isConnected,
		isConnecting,
		isDisconnected,
		connect: (address: string) => client.connect(address),
		disconnect: () => client.disconnect()
	};
}
