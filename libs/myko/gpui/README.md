# myko-gpui

A styling-agnostic bridge from Myko's live remote data to GPUI entities. It supports
native GPUI platforms and Zed's WebAssembly backend at the pinned revision used by
`pulse-gpui`.

```rust
use myko::entities::server::GetConnectedServer;
use myko_gpui::{live_query, observe_remote, provide_myko, render_remote_list};

provide_myko("ws://127.0.0.1:5155/myko", cx);
let servers = live_query(GetConnectedServer {}, cx);
// Inside the rendering owner's `Context<Self>` constructor:
let subscription = observe_remote(&servers, cx);

let element = render_remote_list(
    &servers,
    cx,
    |_| gpui::div().child("Loading…"),
    |error, _| gpui::div().child(error.to_owned()),
    || gpui::div().child("No servers"),
    |items| gpui::div().children(items.iter().map(|item| {
        gpui::div().child(item.address.clone())
    })),
);
```

Keep the observation subscription in the rendering owner. This is the normal
GPUI entity-observation pattern and makes Myko updates redraw that owner.

The crate supplies state and render branching, but no colors, spacing, typography,
or component styling. Hyphae/WebSocket callbacks are marshalled onto GPUI's
foreground executor before the owning entity is updated and notified.

## Commands

Each command invocation has its own fine-grained GPUI entity. It is `Pending`
synchronously, then notifies only its own observers once when it becomes
`Success` or `Failed`:

```rust
use myko_gpui::{CommandHooks, CommandState, command_boundary};

let save = command_boundary::<_, SaveResult, _>(
    &SaveDocument { id },
    |state| match state {
        CommandState::Pending => gpui::div().child("Saving…").into_any_element(),
        CommandState::Success(result) => gpui::div()
            .child(format!("Saved {}", result.version))
            .into_any_element(),
        CommandState::Failed(error) => gpui::div()
            .child(error.to_string())
            .into_any_element(),
    },
    CommandHooks::default()
        .on_success(|result, cx| { /* update GPUI state */ })
        .on_failed(|error, cx| { /* report the error */ }),
    cx,
);
```

Lifecycle hooks run from the entity transition event, never from rendering, and
fire at most once. `Pending` includes both queued-while-disconnected and
in-flight commands: Myko queues and flushes disconnected sends, but does not
currently expose a distinct delivery acknowledgement.

## CRUD lists

`CrudCommands` composes optional, domain-typed create, rename, and delete
factories. Their result types may differ, and every create control or keyed row
operation retains its own `Command` entity. Applications may pass create input
directly with `CrudController::create`, or configure `with_create_input` and call
`create_from_provider` from a click handler. A provider can read an existing
form, open an application-owned prompt, return defaults, or return `None` to
cancel; Myko does not impose a form or modal implementation.

Pair the controller with `fine_query_list_from_store_with_key` to give stable row
entities access to their `CrudRowActions`. Observe those action entities from
the row, and use `on_command_change` or `observe_command_in` for side effects.
The controller never applies optimistic list rewrites: the live `QueryStore`
remains authoritative and reconciles membership and values through keyed query
diffs.

Fine-grained server-side joins use the same row model. `live_view_store(view, cx)`
subscribes to a keyed view map, not a rebuilt `Vec`, so `fine_query_list_from_store_with_key`
retains row entities across joined-value changes. A view output should use its
stable domain key (for example, the task ID) even when the server-side join
internally uses composite keys.

## Demo

Start the in-memory server:

```sh
cargo run -p myko-server --example dummy_server --target-dir target/agent
```

Run the native demo:

```sh
cargo run -p myko-gpui --example server_status --target-dir target/agent
```

The browser demo compiles the same `server_status.rs` source. The pinned GPUI web
backend requires nightly, shared Wasm memory, and COOP/COEP headers; these are all
configured in the nested package:

```sh
cd libs/myko/gpui/examples/web
trunk serve
# or validate without serving:
cargo check --target wasm32-unknown-unknown   --target-dir ../../../../../target/agent-web
```
