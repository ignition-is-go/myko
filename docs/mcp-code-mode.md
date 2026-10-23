# Myko MCP operation contracts

Myko exposes `connection_status`, `search`, and `execute` as MCP tools. Use
on-demand contracts to write calls, validate their inputs before local dispatch,
and compute a bounded result inside the sandbox.

## Discover one operation

Search matches operation IDs and descriptions by case-insensitive substring.
Use `query` and an optional `kind` to narrow the catalog. `id` selects an exact
operation. Ordinary search returns small name, type-hint, and omission records.

To inspect one complete available contract, call `search` with:

```json
{"kind": "query", "id": "GetAllServers", "details": true}
```

`details` requires both `kind` and `id`. The same contract is available inside
`execute` without another MCP round trip:

```javascript
return myko.describe('query', 'GetAllServers');
```

Both paths apply operation visibility filters. A description contains argument
metadata, `inputSchema`, a TypeScript-style signature, result-wrapper fields,
and an illustrative call example when required primitive arguments permit one.
Examples do not establish resource existence. Describing an operation does not
invoke its Serde deserializer, construct a reactive cell, or run a handler.
Schema resources at `myko://schema/<kind>/<id>` return the same input schema.

## Understand the input schema

Macros capture Serde deserialization names, aliases, documentation, defaults,
omission rules, and skipped or flattened fields. Rust `scene_id` normally
becomes `sceneId`. Explicit container naming and deserialize-specific field
renames take precedence. Alias schemas reference their canonical property.

`defaulted` means Serde supplies an omitted value. `optional` permits omission.
A flattened field contributes keys to the containing object, rather than a
nested property with its Rust name.

Primitive and collection schemas come from Schemars using the actual Rust field
type. A nested type implementing `JsonSchema` contributes its recursive schema
and enum constraints. Local references retain their meaning when embedded in
the operation schema. Downstream types can opt in with Myko's Schemars re-export:

```rust
#[derive(myko::schemars::JsonSchema)]
#[schemars(crate = "myko::schemars")]
```

Types without `JsonSchema`, custom field deserializers, flattened fields, and
converted or transparent operation inputs remain unconstrained in the generated
schema. `x-myko-opaque` marks these limits. Additional properties remain allowed
in structural preflight; real Serde parsing decides whether unknown fields,
duplicate aliases, custom values, and flattened fields are valid.

An opted-in nested type's schema must describe its deserialization form,
including any custom Serde representation. TypeScript displays remain hints,
and the sandbox does not compile TypeScript.

## Validate calls inside execute

Each operation call checks visibility and callability. It rejects non-object
arguments, then validates known structural constraints before dispatch on an
in-process host. Invalid input produces an operation name, JSON pointer, and
constraint diagnostic with a pointer to `myko.describe(kind, id)` for repair.
Nested alternatives report the deepest available field error.

Schema checks do not deserialize Rust values. The authoritative typed parser
still runs once before creating cells or running handlers. Valid custom
parameters therefore invoke their deserializer once. A remote-client executor
forwards object arguments to its server without applying the local host's typed
schema, because their versions may differ. Remote errors remain authoritative.

## Compute a structured result

Query and view wrappers contain `items` and `count`. Report and command wrappers
contain `result`. Use JavaScript `Map`, `Set`, filtering, and arithmetic to join
records, evaluate membership against existing entities, remove duplicates, and
compute shared and different sets. Return the computed summary.

For example, supply this `execute` request:

```json
{
  "code": "const r = await myko.query('GetAllServers', {}); const ids = new Set(r.items.map(row => row.id)); return {count: ids.size};",
  "outputSchema": {
    "type": "object",
    "properties": {"count": {"type": "integer", "minimum": 0}},
    "required": ["count"],
    "additionalProperties": false
  }
}
```

`outputSchema` is optional and validates the raw returned value. Myko compiles it
before running code. Invalid schemas fail before operations run. Schemas can use
local references; external file and network references are rejected.

Intermediate operation responses stay inside the sandbox. Returned tool text
is compact JSON and limited to 64 KiB. Oversized results fail with guidance to
return selected fields or computed counts. Narrow discovery searches to fit
the same limit. Reduce Maps and Sets to plain JSON values before returning them.

Output validation checks structure, not arithmetic. Compute counts using
`.size` or `.length` from the evaluated data. Output failures do not roll back
earlier successful commands. A script is not a transaction, and an error after
a write does not imply the write failed.

## Verify all three behaviors

Run focused checks through Cargo Flux:

```sh
cargo flux run test-operation-contracts
cargo flux run test-mcp
cargo flux run test-mcp-http
cargo flux run check-server
cargo flux run check-operation-contracts
```

The HTTP tests cover wire naming, aliases, defaults, nested enum schema errors,
and single-pass custom deserialization. A second test seeds an actual Myko store, publishes membership references,
and deletes six records. It reads 36 remaining entities and the references
through two generated queries, including a duplicate reference. JavaScript computes 29 and 25 selected IDs,
18 shared IDs, 11 and seven differences, and six unresolved references. More
than 140 KB of intermediate rows stays inside the sandbox. The final response
contains only six schema-validated counts and is under 256 bytes.

Unit tests cover visibility, remote-client validation authority, output-schema
rejection before code execution, malformed returned structures, result limits,
and rejection of external schema references. These tests establish behavior;
agent task-success improvements require a separate comparison against this MCP.

## Give agents domain context

Myko keeps application vocabulary in Rust definitions and optional domain guides.
It does not infer business meaning from entity names or field suffixes.

Struct and field documentation on `#[myko_item]` becomes entity metadata. Field
names follow Serde's **serialization** contract, including explicit renames,
flattening, and skipped fields. These are static field descriptions, not live
values or a complete output JSON Schema. Rust types are descriptive hints.

```rust
/// A render job requested by a user.
#[myko::myko_item]
pub struct RenderJob {
    /// Requested output dimensions in pixels.
    pub dimensions: [u32; 2],
}

myko::register_domain_guide!(
    "render-workflow",
    "Render workflow",
    "Vocabulary and ordering rules for render jobs",
    include_str!("domain/render-workflow.md")
);
```

Applications can describe units, terminology, lifecycle invariants, prerequisites,
and recommended query/command sequences in guides. Register only documentation
that callers of the MCP endpoint may inspect. Reading a guide runs no operation.

Use `search` with `kind: "entity"`, `"relationship"`, or `"guide"` and a `query`
substring or exact `id`. Compact entries omit entity fields and guide bodies;
`details: true` selects one complete record. Inside `execute`, the same record is
available as `myko.describe('entity', 'RenderJob')` or
`myko.describe('guide', 'render-workflow')`. Resources expose these records at
`myko://domain/<kind>/<id>` using the same lookup and visibility rules.

The relationship map reads existing `belongs_to`, `owns_many`, `ensure_for`, and
typed graph registrations. It describes reference fields, source/target types,
cascade behavior, dependency types, graph endpoint constraints, direction,
pair policies, adjacency, self-loop and endpoint deletion policies. It is a
**type map**, not a snapshot of live entity connections. Query current state
before calculating membership or deciding whether referenced objects exist.

Domain visibility uses the existing filters with names `entity_<type>` and
`guide_<id>`. Relationship entries are omitted when a referenced entity type is
hidden. Graph category constraints are only exposed when all member types are
visible. An operation-only allowlist must explicitly include the domain kinds
it wants to expose. No new permissions or execution path are introduced.

## Design references and boundaries

[Cloudflare's Code Mode guidance](https://developers.cloudflare.com/agents/model-context-protocol/codemode/)
recommends selective discovery for large APIs, computation inside the sandbox,
and focused final results. Authentication and authorization belong in host
callbacks; sandboxed code should not receive credentials or direct networking.
Myko keeps its search/execute interface and host-side call filters, and returns
selected metadata rather than injecting the whole application catalog into
`tools/list`.

[MCP resources](https://modelcontextprotocol.io/specification/2025-06-18/server/resources)
provide a standard way to expose application documentation and schemas.
Entity descriptions, the relationship type map, and guides are Myko framework
additions implementing that pattern; Cloudflare does not prescribe this exact
metadata design. More MCP tools per entity would undermine selective discovery.

The framework does not guess operation idempotency, transactional guarantees,
units, or business invariants. Consumers must document those facts. JavaScript
execution is not a transaction: commands already applied are not rolled back
when a later call or output validation fails.
