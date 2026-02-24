# Rocketship (rship)

A control platform for orchestrating reactive event relationships within networks of integrated multimedia systems.

## Core Concept

**Executors** connect via WebSocket to publish **Targets** with **Emitters** (state observers) and **Actions** (commands). **Bindings** define reactive relationships between Emitters and Actions, organized into **Scenes**.

## Quick Start

```bash
# Prerequisites: Bun, Rust, protobuf
# Optional: Docker (local Kafka)

# Install
bun install

# Start Kafka (required for the primary Rust server)
docker compose -f docker-compose.local.yml up -d

# Run primary server (Rust)
export MYKO_HOST_ADDRESS=localhost
export MYKO_PORT=5155
export KAFKA_BROKERS=localhost:9092
cargo run -p rship-server --target-dir target/agent

# Run UI (separate terminal)
bun run --filter @rship/ui dev

# (Optional) Legacy Bun/TypeScript server (supports local-only mode)
# bun run --filter @rship/server dev
```

## Project Structure

```
/apps/
├── server/  # Primary Rust server
├── server/     # Legacy Bun/TypeScript server
├── ui/         # Svelte 5 + SvelteKit
├── asset_store/# Asset Store service
├── execs/      # 15+ executor implementations
└── linkd/      # Link daemon

/libs/
├── myko/       # Event sourcing framework
├── entities/   # Domain entities (Rust) + generated bindings
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

Legacy Bun/TypeScript server (`apps/server`) uses additional environment variables (for example `RSHIP_CLUSTER_SECRET`,
`AUTH_0_DOMAIN`, and `RSHIP_LOCAL_ONLY`). See `docs/DEVELOPMENT.md` and `apps/server` for details.

## License

Proprietary
