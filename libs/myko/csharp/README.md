# MykoSdk for .NET

MykoSdk is a low-level WebSocket communication library for real-time applications in the Rship ecosystem.

## Features

- Real-time WebSocket communication
- Event-driven architecture
- Connection status monitoring
- Automatic reconnection handling
- Message serialization/deserialization
- Comprehensive logging support

## Installation

```bash
dotnet add package MykoSdk
```

## Quick Start

```csharp
using MykoSdk.Client;

// Create and connect to Myko server
var client = new MykoClient();
await client.ConnectAsync("ws://localhost:8080/myko");

// Send events
var eventData = Event<MyData>.FromItem(
    new MyData { Value = "Hello" }, 
    EventType.SET, 
    Guid.NewGuid().ToString()
);
await client.SendEventAsync(eventData);

// Monitor connection status
client.ConnectionStatusChanged += (sender, status) => {
    Console.WriteLine($"Connection status: {status}");
};
```

## Documentation

For more detailed documentation and examples, visit the [myko repository](https://github.com/ignition-is-go/myko).

## License

This project is licensed under the AGPL-3.0-or-later license.
