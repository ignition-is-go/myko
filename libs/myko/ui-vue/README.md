# @myko/ui-vue

Vue 3 reactive wrappers for the Myko client.

## Installation

```bash
pnpm add @myko/ui-vue
```

## Usage

### Using Composables (Recommended)

```vue
<script setup>
import { useQuery, useReport, useConnection } from '@myko/ui-vue'
import { queries, reports } from '@rship/entities'

// Connection management
const { status, isConnected, connect, disconnect } = useConnection()

// Query data with automatic cleanup
const { items, itemsArray, resolved } = useQuery(queries.GetAllTargets({}))

// Report data with automatic cleanup
const { value: count } = useReport(reports.CountAllTargets({}))

// Connect on mount
connect('ws://localhost:5155')
</script>

<template>
  <div>Status: {{ status }}</div>
  <div v-if="!resolved">Loading...</div>
  <div v-for="target in itemsArray" :key="target.id">
    {{ target.name }}
  </div>
  <div>Total: {{ count?.count ?? 0 }}</div>
</template>
```

### Using the Client Directly

```vue
<script setup>
import { onUnmounted } from 'vue'
import { getMykoClient } from '@myko/ui-vue'
import { queries, commands } from '@rship/entities'

const client = getMykoClient()

// Manual query subscription
const { items, release } = client.query(queries.GetAllTargets({}))
onUnmounted(release)

// Send commands
async function deleteTarget(id: string) {
  await client.sendCommand(commands.DeleteTarget({ targetId: id }))
}
</script>
```

## API

### Composables

- `useQuery(queryFactory)` - Watch a query with reactive Map updates
- `useReport(reportFactory)` - Watch a report with reactive value
- `useConnection()` - Manage connection status

### Client

- `getMykoClient()` - Get the global singleton client
- `createMykoClient()` - Create a new client instance
- `myko` - Direct access to the global client

## Types

```typescript
import type {
  ReactiveQuery,
  ReactiveReport,
  UseQueryReturn,
  UseReportReturn,
  UseConnectionReturn
} from '@myko/ui-vue'
```
