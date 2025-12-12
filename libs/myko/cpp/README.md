# myko-rs C++ Bindings

C++ bindings for the Rust myko-rs library using [cxx](https://cxx.rs/), providing WebSocket connectivity to Myko servers.

## Building

### Prerequisites

- Rust toolchain (cargo)
- C++17 compatible compiler
- CMake 3.16+

### Build the Rust Library

```bash
# From this directory
cargo build --release

# Or using just from repo root
just build-myko-cpp
```

### Build with CMake

```bash
# Configure
cmake -B build -DCMAKE_BUILD_TYPE=Release

# Build
cmake --build build

# Optionally build examples
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_EXAMPLES=ON
cmake --build build
```

### With Corrosion (Recommended)

If you have [corrosion](https://github.com/corrosion-rs/corrosion) installed, CMake will automatically handle Rust compilation:

```bash
cmake -B build
cmake --build build
```

## Usage

```cpp
#include <myko/myko.h>
#include <iostream>

int main() {
    // Create client
    auto client = myko::new_client();

    // Connect to server
    client->set_address("ws://localhost:5155/myko");

    // Check connection
    if (client->is_connected()) {
        std::cout << "Connected!" << std::endl;

        // Send event as JSON
        auto result = client->send_event_json(R"({
            "item": {"id": "...", "name": "..."},
            "itemType": "Target",
            "changeType": "SET",
            "tx": "...",
            "createdAt": "..."
        })");

        if (!result.empty()) {
            std::cerr << "Error: " << result << std::endl;
        }
    }

    // Disconnect
    client->disconnect();
    return 0;
}
```

## API Reference

### `myko::new_client() -> Box<MykoClientWrapper>`

Create a new MykoClient instance.

### `MykoClientWrapper::set_address(address: &str)`

Set the server WebSocket address (e.g., `"ws://localhost:5155/myko"`).
Pass an empty string to disconnect.

### `MykoClientWrapper::disconnect()`

Disconnect from the server.

### `MykoClientWrapper::is_connected() -> bool`

Check if the client is currently connected.

### `MykoClientWrapper::send_event_json(event_json: &str) -> String`

Send an event as a JSON string. Returns empty string on success, or error message on failure.

## Linking

When linking manually, you need to link against:

- The generated static library (`libmyko_cpp.a`)
- System libraries:
  - Linux: `pthread`, `dl`, `m`
  - macOS: `pthread`, `dl`, `Security`, `CoreFoundation`
  - Windows: `ws2_32`, `userenv`, `bcrypt`

## Architecture

This library uses cxx to create safe bindings between Rust and C++:

- Rust code wraps `myko_rs::client::MykoClient`
- cxx generates both Rust and C++ code for the bridge
- A global Tokio runtime handles async operations
- JSON is used for event serialization (type-safe variants planned)
