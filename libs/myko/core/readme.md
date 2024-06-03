# Myko

> easy flexible event sourcing.

Myko is grounded in realtime data. It uses an event log to establish the source of truth for a distributed system, and provides mechanisms for servers to reduce those events into query and report results that update in real time.

## Architecture

### Servers

Myko Servers host WebSocket gateways for clients to connect, and persist and sync data between server cluster members.

Servers are responsible for

- persisting the event log
- providing handlers for Commands, Queries and Reports
- coordinating inter-client communication
- authenticating Commands
- publishing events to change the state of entities based on Commands

### Clients

Myko Clients open a websocket connection to a Myko Server, and communicate using JSON to publish events, subscribe to query and report results, and send commands to update data.

## Types

The Main Myko types are Items, Events, Commands, Queries and Reports

### Items

Items are the smallest unit as far as Myko is concerned. They have the minimum of the data below. It should be exended for each entity type your application cares about. They can be thought of like rows in a DB table, or an entry in a DB collection. Their 'type name', e.g. 'Posts', 'Users', can be thought of as the table name or topic.

> NOTE: the hash for each item can be omitted, and will be computed when the [[#MEvent]] that sets the item hits the server. It can, be provided if you would like to additionally limit recalculation, for example if there are fields which may change that do not warrant an update in the server.

Base Myko Item

```ts
{
  id: ID
  hash: string
}
```

Wrapped Myko Item

```ts
{
  itemType: string,
  item: {
    // ... item
  }
}
```

## Events

Events author the source of truth for an Item

Clients and Servers alike can be the source of truth for various items, and thus publish events for that item, allowing for distribution of authority over the realtime data. Events are accepted by the server at face value, and will be propogated globally without authentication.

> it is known that this is potentially dangerous, and there are plans to allow servers to lock down events received from clients by Type, or other permissions. It is currently open for the sake of speed.

## Queries & Reports

Queries are a live selection over a given Item type with a filter object. Clients send the query request payload to the server, and until the server receieves a matching query cancel, the server sends crud updates to the client about any items that match the query, as well as changes to the list as a whole.

Reports are a more general form of query that can return any type as a live response, allowing for complex joins and reductions on the server to be provided in real time to the clients.

## Commands

Commands are the main way that clients can request a change of state in the Myko Server. They are authenticated, and provide a response as to their success, as well as an optional return value. Often a command will cause a query or report to update as additional feedback.

## Getting Started

This package provides types that are shared between the Myko Client and Server implementations in typescript. It would be rare to use this package on its own, but if you have reason to build your own websocket client from the ground up, you're welcome to do so.

More likely, you want The `@myko/ws` package, which provides a `WSMClient` class that will take care of all the Myko connection handshakes, and the protocol is descrived there as well. Myko Client libraries are also being developed in Rust, Python, C++, and other langages.

## Development

Myko is developed as part of Rship @ Lucid

## Roadmap

There are many exciting features down the road for Myko including but not limited to

- client to client communication,
- global transaction rollback and auditing
- if you'd like to help, drop us a line [here](mailto:trevor@lucid.rocks)
