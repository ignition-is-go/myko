"""
myko-rs: Python bindings for the Myko event-sourcing CQRS framework.

This package provides Python bindings for the Rust myko-rs library,
enabling reactive queries, reports, and commands over WebSocket.

Example:
    >>> from myko_rs import MykoClient
    >>>
    >>> async def main():
    ...     client = MykoClient()
    ...     client.set_address("ws://localhost:5155/myko")
    ...
    ...     # Watch a query reactively
    ...     async for servers in client.watch_query({"queryId": "GetAllServers", "query": {}}):
    ...         print(f"Got {len(servers)} servers")
"""

from myko_rs._native import (
    MykoClient,
    ConnectionStatus,
)

__all__ = [
    "MykoClient",
    "ConnectionStatus",
]

__version__ = "3.0.0-canary.809"
