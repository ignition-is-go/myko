# myko-rs Python Bindings

Python bindings for the Rust myko-rs library, providing high-performance WebSocket connectivity to Myko servers.

## Installation

### Development

```bash
# Install maturin if not already installed
pip install maturin

# Build and install in development mode
just build-myko-py
# or directly:
cd libs/myko/py && maturin develop
```

### Release Build

```bash
just build-myko-py-release
# or directly:
cd libs/myko/py && maturin build --release
pip install target/wheels/myko_rs-*.whl
```

## Usage

```python
from myko_rs import MykoClient, ConnectionStatus

# Create client
client = MykoClient()

# Connect to server
client.set_address("ws://localhost:5155/myko")

# Check connection status
status = client.get_connection_status()
if status == ConnectionStatus.Connected:
    print("Connected!")

# Send an event
client.send_event({
    "item": {"id": "...", "name": "..."},
    "itemType": "Target",
    "changeType": "SET",
    "tx": "...",
    "createdAt": "..."
})

# Disconnect
client.disconnect()
```

## API Reference

### MykoClient

- `MykoClient()` - Create a new client instance
- `set_address(address: str | None)` - Set server address or disconnect
- `get_connection_status() -> ConnectionStatus` - Get current connection status
- `disconnect()` - Disconnect from server
- `send_event(event: dict)` - Send an event to the server

### ConnectionStatus

- `ConnectionStatus.Connected` - Client is connected
- `ConnectionStatus.Disconnected` - Client is not connected

## Architecture

This package wraps the Rust `myko_rs::client::MykoClient` using PyO3, providing:

- Native performance WebSocket handling
- Automatic reconnection
- Thread-safe async operations via Tokio runtime
- JSON serialization for Python dict ↔ Rust Value conversion

## Development

```bash
# Check compilation
cd libs/myko/py && cargo check

# Run tests
cd libs/myko/py && cargo test
```
