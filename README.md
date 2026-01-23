# Rocketship (rship)

A control platform for orchestrating reactive event relationships within networks of integrated multimedia systems.

## Core Concept

**Executors** connect via WebSocket to publish **Targets** with **Emitters** (state observers) and **Actions** (commands). **Bindings** define reactive relationships between Emitters and Actions, organized into **Scenes**.

## Quick Start

```bash
# Prerequisites: Node 20+, Bun, pnpm 10+, Rust, protobuf

# Install
pnpm install

# Run server
pnpm dev --filter @rship/server

# Run UI (separate terminal)
pnpm dev --filter @rship/ui
```

## Project Structure

```
/apps/
├── server/     # Bun server
├── ui/         # Svelte 5 + SvelteKit
├── execs/      # 15+ executor implementations
└── linkd/      # Link daemon

/libs/
├── myko/       # Event sourcing framework
├── entities/   # Domain entities & handlers
├── sdk/        # Multi-language executor SDK
└── ...
```

## Documentation

- [Development Guide](docs/DEVELOPMENT.md)
- [Contributing](docs/CONTRIBUTING.md)
- [Architecture Overview](docs/architecture/OVERVIEW.md)
- [API Reference](docs/API_REFERENCE.md)

## Environment Variables

| Variable | Description |
|----------|-------------|
| `KAFKA_BROKERS` | Kafka broker addresses |
| `MYKO_HOST_ADDRESS` | Server host |
| `MYKO_PORT` | Server port (default: 5155) |
| `RSHIP_CLUSTER_SECRET` | Cluster auth secret |
| `AUTH_0_DOMAIN` | Auth0 domain |

## License

Proprietary
