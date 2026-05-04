# myko Search Index — Specification

## Context

myko's current entity search lives in `libs/myko/core/src/search/` and is backed
by tantivy (feature-gated behind `search = ["dep:tantivy"]`). Tantivy is a
relevance-ranked full-text search engine designed for indexing documents and
returning scored results — a different shape of problem than the one we have.
Our access pattern is:

- Match against short identifier-like strings (names, tags, categories), not
  prose.
- Three discrete strategies — exact, subsequence, fuzzy — combined into a
  tiered ranking. No BM25, no TF-IDF, no document-level scoring.
- Mutations dominate by volume; queries are interactive and small-result.

The mismatch surfaces as dead-weight machinery on every code path: schema
validation, segment commits, segment merges, a reader/writer split with reload
ceremony, and a 50 MB writer heap reserved up front (`index.rs:47`). On the
write path the cost is concrete: `SearchIndex::index_item`
(`libs/myko/core/src/search/index.rs:83`) takes `&Arc<dyn AnyItem>`, calls
`item.to_value()` (full serde JSON serialization), then walks the
`serde_json::Value` to extract field values by camelCase string key
(`extract_searchable_text`, `search/mod.rs:100`). That happens on every insert,
update, and remove site in `server/context.rs` (~12 sites) and in the server
boot/catch-up paths in `server/src/lib.rs` (~6 sites).

We could measure how slow this is. We don't need to — the structural argument
is sufficient. A purpose-built per-type index built directly off `&T` skips the
serialization, skips the schema layer, skips the segment lifecycle, and is a
better fit for the scoring model we actually want.

This spec describes the replacement: per-type, in-memory, typed search indexes
that plug into the existing macro and report machinery without changing the
wire protocol or the reactive integration.

## Goals

- Exact match (token equality) in microseconds.
- Subsequence / acronym match in fzf-style ("fma" → "FullMetalAlchemist").
- Typo tolerance with Levenshtein distance up to 2.
- Mutations cheap and immediate — no commits, no merges, no rebuild ceremony.
- **No serde on the write path.** Index directly off `&T` via macro-generated
  field extractors.
- Per-type indexes; macro-generated typed search reports per entity type.
- Backwards compatible: the existing `EntitySearch` stringly-typed report keeps
  working as a thin shim over the typed registry.

## Non-goals

- Relevance ranking (BM25, TF-IDF). We rank by tier, not score blending.
- Full-text search across paragraphs of prose. Field values are short identifiers,
  names, and tags.
- Persistence. The index is rebuilt from the entity store on startup, same as
  today's tantivy RAM directory (`Index::create_in_ram` at `index.rs:43`).
- Network/IPC interface. This is an in-process library used by myko query
  resolution.
- Semantic / embedding search. If we add this later, it lives in a separate
  component.
- New reactive plumbing. Generated reports subscribe to typed entity stores via
  hyphae signals exactly the way every other generated report does today.

## Architecture overview

One generic `SearchIndex<T>` instance per searchable entity type, registered
under its `entity_type` string in a `SearchRegistry`. Each index runs three
matchers against a shared candidate set, results merged into a tiered ranking:

```
                    ┌──────────────────────────┐
   query ──────────▶│      SearchIndex<T>      │
                    │                          │
                    │  ┌────────────────────┐  │
                    │  │ exact (HashMap)    │──┼──▶ tier 0 hits
                    │  └────────────────────┘  │
                    │  ┌────────────────────┐  │
                    │  │ nucleo (scan)      │──┼──▶ tier 1 hits
                    │  └────────────────────┘  │
                    │  ┌────────────────────┐  │
                    │  │ Levenshtein (scan) │──┼──▶ tier 2 hits
                    │  └────────────────────┘  │
                    │                          │
                    └──────────────────────────┘
                               │
                               ▼
                       merge, dedupe, sort
                               │
                               ▼
                         Vec<Hit<T::Id>>
```

Both scan-based matchers run in parallel via rayon over the same
`Vec<EntityRecord>`. Exact match short-circuits and skips the scan when it hits.

`SearchIndex<T>` is generic over `T: Searchable` (a per-type trait whose method
names are uniform across all entities; see [Macro changes](#macro-changes)).
The internals — extractor, hits, interner — are fully monomorphized. Typed ids
(`TargetId`, `ServerId`, …) flow through the index unchanged. There is no
`TypeId` compare and no downcast on the typed hot path.

The `&dyn AnyItem` call sites in `server/context.rs` reach the right
monomorphized index through a `SearchRegistry` that holds
`HashMap<&'static str, Arc<dyn DynSearchIndex>>`. The `DynSearchIndex` shim
performs **one** downcast at the registry boundary per insert/remove, then
delegates into the typed `SearchIndex<T>` where everything is monomorphized.
That downcast is a `TypeId` compare (a single `usize` equality with a constant)
plus a pointer adjustment — branch-predictable and effectively free next to
the work it dispatches into.

The whole `SearchIndex<T>` struct is `pub(crate)`. The user-facing surface is
the macro-generated `Search{T}` report per entity type — see
[Public API](#public-api).

## Module layout

The implementation lives inside `myko-core` at `libs/myko/core/src/search/typed/`
— a sibling of the existing `search/` module that hosts the tantivy path. Keeping
it in-tree avoids a new crate boundary that would buy nothing: the index talks
directly to `AnyItem`, the macro-generated registrations, and the typed entity
stores, all of which already live in `myko-core`. A separate crate would force
those types through a public surface for no architectural gain.

```
libs/myko/core/src/search/
├── mod.rs                  # public re-exports + no-op stub for non-search builds
├── index.rs                # legacy tantivy SearchIndex (deleted in phase 4)
├── entity_search.rs        # stringly-typed EntitySearch shim
└── typed/
    ├── mod.rs              # public re-exports
    ├── index.rs            # SearchIndex<T> struct, mutation API (pub(crate))
    ├── interner.rs         # IdInterner — Arc<str> ↔ u32
    ├── searchable.rs       # Searchable trait
    ├── score.rs            # Score enum, Ord impl
    ├── registry.rs         # entity_type → Arc<dyn DynSearchIndex> dispatch
    └── hit.rs              # Hit<Id>, SearchResult<T>
```

The matcher implementations (exact, subsequence, typo) live alongside
`index.rs` rather than in a `matchers/` subdirectory — the per-matcher files
are short enough that a flat layout reads better than nesting.

The current `search` cargo feature continues to gate the entire search subtree
(both the legacy tantivy path and the new `typed/` module) plus the
macro-generated reports, mirroring today's `dep:tantivy` gate. The no-op stub
(`search/mod.rs:46-74`) is preserved for non-search builds.

## Public API

### Generated per-type reports (primary surface)

The `#[myko_item]` macro already auto-generates a query/report family per entity
type (`GetAllTargets`, `GetTargetsByIds`, `GetTargetsByQuery`, etc.). We add one
more, **gated on the entity having at least one `#[searchable]` field**:

```rust
// auto-generated when Target has any #[searchable] field
#[myko_search_report(Target)]
pub struct SearchTargets {
    pub query: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default = "default_max_edit_distance")]
    pub max_edit_distance: u8,
    #[serde(default = "default_true")]
    pub include_subsequence: bool,
    #[serde(default = "default_true")]
    pub include_typo: bool,
}

pub struct SearchResult<T: WithTypedId> {
    pub hits: Vec<Hit<T::Id>>,
}

pub struct Hit<Id> {
    pub id: Id,
    pub score: Score,
    pub matched_field: usize,  // index into the entity's searchable fields
}
```

`SearchResult<T>` is generated alongside the entity (via the same macro pass that
emits `PartialTarget`, `TargetCount`, etc.) so cross-language codegen
(`cargo flux run gen`) produces typed results in TS, Python, etc. Hit ids are
`TargetId`, `ServerId`, etc. — the existing per-entity newtypes wrapping `Arc<str>`.

Callers write:

```rust
let result = ctx.report(SearchTargets {
    query: "audio mixer".into(),
    limit: 50,
    ..Default::default()
});
// result.hits: Vec<Hit<TargetId>>
```

### Stringly-typed shim (backwards compatibility)

The existing `EntitySearch` (`libs/myko/core/src/search/entity_search.rs:38`)
remains as a runtime-dispatched shim:

```rust
#[myko_macros::myko_report(EntitySearchResult)]
pub struct EntitySearch {
    pub entity_type: String,
    pub query: String,
    pub limit: usize,
}

#[myko_macros::myko_report_output]
pub struct EntitySearchResult {
    pub ids: Vec<Arc<str>>,
}
```

`EntitySearch::compute` looks up the entity_type in the registry
(`HashMap<&'static str, Arc<dyn DynSearchIndex>>`) and delegates. The wire format
is unchanged — opaque `Arc<str>` ids — so existing clients (and the cross-type
"search anything" command palette use case) keep working.

### Score

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Score {
    Exact,
    Prefix,                   // computed cheaply: searchable_text.starts_with(query)
    Subsequence(u16),         // nucleo score, higher = better
    Typo(u8),                 // edit distance, lower = better
}
```

Tier ordering: every `Exact` > every `Prefix` > every `Subsequence` > every `Typo`.
Within a tier, score breaks ties.

## Internal types

`SearchIndex<T>` is `pub(crate)` — constructed by the registry, consumed by
macro-generated reports. Generic over `T: Searchable`; the trait's uniform
method name keeps every entity's generated impl identical in shape.

```rust
/// Per-type trait implemented by the #[myko_item] macro for any entity that
/// has at least one #[searchable] field. The method names are uniform across
/// every entity; only the inline body of `extract_searchable` varies.
pub trait Searchable: WithTypedId + Send + Sync + 'static {
    /// Write each searchable field's value into `out`, separated by U+001F.
    /// Push the start offset of each field into `offsets`, in declaration order.
    fn extract_searchable(&self, out: &mut String, offsets: &mut SmallVec<[u32; 4]>);

    /// Field names parallel to the offsets pushed by `extract_searchable`.
    /// UI affordance only — never read in hot paths.
    fn searchable_field_names() -> &'static [&'static str];
}

pub(crate) struct SearchIndex<T: Searchable> {
    entity_type: &'static str,
    interner: IdInterner<T::Id>,                   // typed id ↔ u32
    live: RoaringBitmap,                           // bit set = entity is live

    entities: Vec<EntityRecord>,                   // dense, indexed by internal id
    exact: HashMap<Box<str>, RoaringBitmap>,       // token → internal ids

    matcher_pool: ThreadLocal<RefCell<nucleo::Matcher>>,

    // Reusable buffers for the extractor — cleared, refilled, and copied into
    // each EntityRecord on insert. Avoids allocating per insert.
    scratch_text: String,
    scratch_offsets: SmallVec<[u32; 4]>,

    dead_count: usize,
    compaction_threshold: f32,                     // default 0.5
    _marker: PhantomData<fn(T)>,
}

/// Type-erased shim that the SearchRegistry holds. Lets &dyn AnyItem call
/// sites in server/context.rs dispatch into the right monomorphized index
/// without knowing T. One downcast per insert at the boundary; everything
/// inside is monomorphized.
pub(crate) trait DynSearchIndex: Send + Sync {
    fn entity_type(&self) -> &'static str;
    fn insert_dyn(&self, item: &dyn AnyItem);
    fn remove_dyn(&self, id: &str);
    fn search_arc_str(&self, query: &str, opts: SearchOptions) -> Vec<Hit<Arc<str>>>;
}

impl<T: Searchable> DynSearchIndex for RwLock<SearchIndex<T>> {
    fn entity_type(&self) -> &'static str { /* ... */ }

    fn insert_dyn(&self, item: &dyn AnyItem) {
        // Single TypeId compare (constant); no JSON, no slice walk.
        let Some(t) = item.as_any().downcast_ref::<T>() else {
            debug_assert!(false, "wrong type routed to SearchIndex<T>");
            return;
        };
        self.write().unwrap().insert(t);
    }
    // ...
}

pub(crate) struct SearchRegistry {
    indexes: HashMap<&'static str, Arc<dyn DynSearchIndex>>,
}

struct EntityRecord {
    internal_id: u32,
    searchable_text: Box<str>,                     // concatenated field values, lowercased,
                                                   // separated by U+001F (unit separator)
    field_offsets: SmallVec<[u32; 4]>,             // start offset of each field in searchable_text
    char_bitmap: u128,                             // ASCII char presence (future prefilter)
}

pub(crate) struct IdInterner<Id> {
    forward: HashMap<Id, u32>,
    reverse: Vec<Option<Id>>,                      // None = slot is dead and not yet recycled
    free_list: Vec<u32>,                           // recycled slots from compaction
}
```

### Typed-id flow

The macro-generated `SearchTargets` report calls into `SearchIndex<Target>`
directly through the typed registry handle (`Arc<RwLock<SearchIndex<Target>>>`)
that the registry stores alongside the dyn-shim. Hits are `Hit<TargetId>`
end-to-end — no `Arc<str>` round-trip in the typed path.

The stringly-typed `EntitySearch` shim goes through `DynSearchIndex::search_arc_str`,
which extracts the inner `Arc<str>` from each `T::Id` (the per-entity newtypes
are `#[repr(transparent)]` over `Arc<str>` — conversion is a noop reinterpret).

### Why a `Searchable` trait with a uniform method name

The index's haystack is the concatenated searchable text — there is no per-field
work in the hot path of any matcher. The `#[myko_item]` macro therefore emits
**one** `impl Searchable for T` per entity type with at least one `#[searchable]`
field, writing all searchable fields into a single caller-provided buffer in
declaration order, separated by U+001F. The method name is uniform across all
entities (`Searchable::extract_searchable`); only the inline body varies. No
slice walk, no function-pointer indirection per field, no per-field heap
allocation.

Inside `SearchIndex<T>::insert(t: &T)` the call is **monomorphized** — direct
function call, inlined by LLVM. No vtable hop, no downcast.

Insert-time work becomes:

1. One inlined call into `T::extract_searchable(t, &mut buf, &mut offsets)`,
   writing directly into the index's reusable scratch buffer.
1. Lowercase the buffer in place (single ASCII-fast-path pass).
1. Tokenize, push tokens into `exact`.

Compared to today's `to_value()` → `serde_json::Value::as_object` → field-by-name
lookup, this is roughly an order of magnitude less work and zero heap allocation
for the extraction step itself. This is the primary justification for the rewrite.

**Dynamic dispatch cost at the registry boundary.** The `&dyn AnyItem` call
sites in `server/context.rs` route through `DynSearchIndex::insert_dyn`, which
performs one `TypeId`-keyed downcast (a single `usize` compare with a constant)
to land on the right monomorphized `SearchIndex<T>`. That's the *entire* dyn
cost — branch-predictable and effectively free next to the work it dispatches
into. The typed reports (`SearchTargets` etc.) skip the dyn boundary entirely
and call into `SearchIndex<Target>` directly via the registry's typed handle.

### Why `Box<str>` for tokens and searchable text

We store many short strings. `Box<str>` is one word smaller than `String` (no
capacity field), and we never mutate them in place — on update we replace the
whole record. Saves measurable memory at 50k+ entities.

### Why `searchable_text` is a flat string with separators

Nucleo matches against a single haystack. Concatenating fields with U+001F (which
never appears in user input) lets us:

1. Run nucleo once per entity instead of once per field.
1. Recover which field matched by binary-searching `field_offsets` against the
   match position.

Nucleo's scoring won't bridge across separator characters because U+001F doesn't
appear in any query, so subsequence matches won't accidentally span fields.

### Why `u128` char_bitmap

Future prefilter for when we scale past ~500k entities. Bit 0..127 = presence of
ASCII char. Query char bitmap AND'd against entity char bitmap — if result !=
query bitmap, entity can't possibly contain all query chars, skip. Costs 16
bytes per entity, computed once at insert. **Not used in v1's query path** but
stored upfront so we don't need a migration when we turn it on.

## Macro changes

`#[myko_item]` (`libs/myko/macros/src/relationship.rs:644-664`) currently emits a
string-only `SearchableRegistration` listing field name strings for the
JSON-extraction path. The new emission per entity with any `#[searchable]`
field:

```rust
// Macro emits, per entity with any #[searchable] field:
impl myko::search::typed::Searchable for Target {
    fn extract_searchable(
        &self,
        out: &mut String,
        offsets: &mut SmallVec<[u32; 4]>,
    ) {
        offsets.push(out.len() as u32);
        out.push_str(self.name.as_str());
        out.push('\u{001F}');

        offsets.push(out.len() as u32);
        out.push_str(self.category.as_str());
    }

    fn searchable_field_names() -> &'static [&'static str] {
        &["name", "category"]
    }
}

// Bootstrap registration — same shape as today, just used to tell the registry
// which entity types to allocate a SearchIndex<Target> for. The closure stored
// here is the only place the registry learns the concrete T it needs to
// monomorphize SearchIndex over.
#[cfg(not(target_arch = "wasm32"))]
#krate::submit! {
    #krate::search::SearchableRegistration {
        entity_type: "Target",
        register: |reg: &mut SearchRegistry| reg.register::<Target>(),
    }
}
```

The trait method name (`extract_searchable`) is uniform across every entity —
no `__target_searchable_extract`-style per-entity function names. Only the
inline body varies. The `register` closure carries the type identity through
the inventory submission; the registry calls it once at startup to allocate
each per-type `SearchIndex<T>` and the matching `Arc<dyn DynSearchIndex>` shim.

The macro additionally emits `Search{T}` and `SearchResult<T>` report types
through the same code path that emits `GetAllTargets`, `PartialTarget`, etc.
The report's `compute` resolves the typed registry handle
(`Arc<RwLock<SearchIndex<Target>>>`) and never crosses the dyn boundary.

The macro additionally emits the `Search{T}` and `SearchResult<T>` types via the
same code path that emits `GetAllTargets`, `PartialTarget`, etc. — the developer
writing the entity gets the search report for free, only paying when they
annotate fields with `#[searchable]`.

The existing string-only `SearchableRegistration` is retained as the entry point
the stringly-typed `EntitySearch` shim uses to discover registered entity types.

## Algorithms

### Exact match

```rust
fn exact_match(&self, query: &str) -> RoaringBitmap {
    let lower = query.to_lowercase();
    self.exact.get(lower.as_str())
        .cloned()
        .map(|b| &b & &self.live)
        .unwrap_or_default()
}
```

Tokens are lowercased on both insert and query. We do not stem, do not stop-word,
do not normalize unicode beyond lowercase. Field values are tokenized by
splitting on whitespace and a small set of separators (`/`, `:`, `.`, `-`, `_`).

**Note:** today's tantivy path uses tantivy's default `TEXT` analyzer, which has
slightly different separator behavior. Migration phase 2 (shadow read) will
surface and characterize the diffs.

### Subsequence match (nucleo)

```rust
fn subsequence_scan(&self, query: &str) -> Vec<(u32, u16, usize)> {
    let needle_buf = /* ... */;
    let needle = Utf32Str::new(query, &mut needle_buf);

    self.entities
        .par_iter()
        .filter(|e| self.live.contains(e.internal_id))
        .filter_map(|e| {
            let matcher = self.matcher_pool.get_or(|| {
                RefCell::new(nucleo::Matcher::new(nucleo::Config::DEFAULT))
            });
            let mut matcher = matcher.borrow_mut();
            let mut hay_buf = Vec::new();
            let haystack = Utf32Str::new(&e.searchable_text, &mut hay_buf);

            matcher.fuzzy_match(haystack, needle).map(|score| {
                let field = self.field_of_match(e, /* match position */);
                (e.internal_id, score, field)
            })
        })
        .collect()
}
```

**Configuration:** `Config::DEFAULT` with `ignore_case = true`. We do not enable
`match_paths()` — entity field values are not paths in the general case. If a
specific entity type has path-shaped fields, that's a future extension via
per-type config.

**Smart case:** if the query contains any uppercase character, switch to
case-sensitive matching. This matches fzf and VS Code behavior; users intuit it.

**Recovering matched field:** nucleo returns match positions when called with
`fuzzy_indices` instead of `fuzzy_match`. We use the cheaper `fuzzy_match` for
the score and recover the field via the *first* query character's position in
the haystack — sufficient for "which field did this hit" UX, not exact for
multi-field span matches.

### Typo tolerance (Levenshtein with length prefilter)

```rust
fn typo_scan(&self, query: &str, max_dist: u8) -> Vec<(u32, u8, usize)> {
    let qlen = query.chars().count();
    let qlower = query.to_lowercase();

    self.entities
        .par_iter()
        .filter(|e| self.live.contains(e.internal_id))
        .filter_map(|e| {
            for (field_idx, token) in e.tokens() {
                let tlen = token.chars().count();
                if (qlen as i32 - tlen as i32).unsigned_abs() > max_dist as u32 {
                    continue;  // can't possibly be within max_dist edits
                }
                let dist = strsim::levenshtein(&qlower, token);
                if dist <= max_dist as usize {
                    return Some((e.internal_id, dist as u8, field_idx));
                }
            }
            None
        })
        .collect()
}
```

**Length prefilter is critical.** It lets us skip ~90% of tokens cheaply before
paying for the DP table. Without it, Levenshtein dominates query time.

**Per-token, not per-entity-text.** Levenshtein is meaningful for short
identifier-like tokens, not for the concatenated `searchable_text`. We iterate
the entity's individual tokens and check each.

**Skip if subsequence already matched.** When we merge tiers, a tier-1 hit always
beats a tier-2 hit, so we can skip the typo scan for entities already hit by
nucleo. In practice this is hard to short-circuit cleanly across parallel scans;
simpler to let both run and dedupe in merge.

**Future optimization:** swap `strsim::levenshtein` for `triple_accel::levenshtein`
if profiling shows it dominates. Triple-accel uses SIMD for the DP table and is
roughly 3-5x faster on short strings.

### Score combination & ranking

```rust
impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        use Score::*;
        match (self, other) {
            (Exact, Exact) => Ordering::Equal,
            (Exact, _) => Ordering::Greater,
            (_, Exact) => Ordering::Less,

            (Prefix, Prefix) => Ordering::Equal,
            (Prefix, _) => Ordering::Greater,
            (_, Prefix) => Ordering::Less,

            (Subsequence(a), Subsequence(b)) => a.cmp(b),
            (Subsequence(_), Typo(_)) => Ordering::Greater,
            (Typo(_), Subsequence(_)) => Ordering::Less,

            (Typo(a), Typo(b)) => b.cmp(a),  // lower distance = better
        }
    }
}
```

**Merge logic:** group hits by internal id, keep the best `Score`. An exact hit
dominates a subsequence hit dominates a typo hit, regardless of inner score.

## Mutation semantics

### Insert

```rust
impl<T: Searchable> SearchIndex<T> {
    pub(crate) fn insert(&mut self, entity: &T) {
        let id = entity.typed_id();
        let internal_id = self.interner.intern(&id);

        // Reusable scratch buffers held on &mut self, cleared between inserts.
        self.scratch_text.clear();
        self.scratch_offsets.clear();
        // Monomorphized — direct, inlined call into the macro-generated body.
        T::extract_searchable(entity, &mut self.scratch_text, &mut self.scratch_offsets);
        self.scratch_text.make_ascii_lowercase();  // single in-place pass

        let record = EntityRecord {
            internal_id,
            searchable_text: self.scratch_text.as_str().into(),  // Box<str>
            field_offsets: self.scratch_offsets.clone(),
            char_bitmap: compute_char_bitmap(&self.scratch_text),
        };

        if internal_id as usize >= self.entities.len() {
            self.entities.resize_with(internal_id as usize + 1, EntityRecord::placeholder);
        }
        self.entities[internal_id as usize] = record;

        for token in tokenize(&self.scratch_text) {
            self.exact.entry(token.into())
                .or_default()
                .insert(internal_id);
        }

        self.live.insert(internal_id);
    }
}
```

Note that tokenization runs once on the concatenated `scratch_text` — the
U+001F separators are token boundaries, so tokens never span fields, but we
also never call the extractor multiple times to walk fields individually.

If the id already exists, this is an update — the old record's exact tokens must
be removed first. See `update`.

### Remove

```rust
pub(crate) fn remove(&mut self, id: &T::Id) {
    if let Some(internal_id) = self.interner.lookup(id) {
        self.live.remove(internal_id);
        self.dead_count += 1;

        if self.should_compact() {
            self.compact();
        }
    }
}
```

We do not eagerly clean posting lists in `exact`. The `& &self.live` mask in
queries filters dead ids out. This makes delete O(1).

### Update

```rust
pub(crate) fn update(&mut self, entity: &T) {
    let id = entity.typed_id();
    if let Some(internal_id) = self.interner.lookup(&id) {
        let old_record = &self.entities[internal_id as usize];
        for old_token in tokens_of(old_record) {
            if let Some(bitmap) = self.exact.get_mut(old_token) {
                bitmap.remove(internal_id);
            }
        }
    }
    self.insert(entity);
}
```

### Reactive integration

**No new plumbing.** The macro-generated `Search<T>` report subscribes to the
typed entity store using the existing report-subscription pattern — the same
mechanism that backs `GetAllTargets`, `GetTargetsByQuery`, etc. When a typed
store mutation fires, the report re-runs through the existing hyphae pipeline.

The index itself doesn't expose change signals; it just maintains its state. The
glue is in `compute`:

```rust
impl ReportHandler for SearchTargets {
    type Output = SearchResult<Target>;

    fn compute(
        &self,
        ctx: ReportContext,
    ) -> impl #krate::hyphae::MaterializeDefinite<Arc<Self::Output>> {
        // depend on the typed store's change signal — same pattern as GetAllTargets
        ctx.search_index::<Target>()
            .reactive_search(&self.query, self.options())
    }
}
```

`reactive_search` reads the current index snapshot under whatever lock pattern
the typed store uses, returns a hyphae cell that tracks dependencies normally.
No special hooks.

**Re-export convention.** Macro-emitted code references hyphae through
`#krate::hyphae::*` (i.e. `myko::hyphae::*`), never `::hyphae::*` directly.
`myko-core` re-exports the hyphae crate at `myko::hyphae` (`core/src/lib.rs:112`)
specifically so downstream consumers don't have to add `hyphae` to their own
`Cargo.toml`. Any new macro emissions in this work — including the `compute`
impl above and the cell type returned by `reactive_search` — must follow that
convention. Existing precedent: `HasForeignKey`, `IdFor`, `IdType`,
`Cell<...>`, `MapExt` are all referenced as `#krate::hyphae::...` in the
`#[myko_item]` macro today (`macros/src/item.rs:529-659`,
`macros/src/command.rs:116`).

### Tombstone compaction

```rust
fn should_compact(&self) -> bool {
    let total = self.entities.len();
    total > 1000 && (self.dead_count as f32 / total as f32) > self.compaction_threshold
}

pub(crate) fn compact(&mut self) {
    let live_ids: Vec<u32> = self.live.iter().collect();
    let mut new_entities = Vec::with_capacity(live_ids.len());
    let mut id_remap: HashMap<u32, u32> = HashMap::with_capacity(live_ids.len());

    for (new_id, &old_id) in live_ids.iter().enumerate() {
        let new_id = new_id as u32;
        id_remap.insert(old_id, new_id);
        let mut record = std::mem::replace(
            &mut self.entities[old_id as usize],
            EntityRecord::placeholder(),
        );
        record.internal_id = new_id;
        new_entities.push(record);
    }

    for bitmap in self.exact.values_mut() {
        let new_bitmap: RoaringBitmap = bitmap.iter()
            .filter_map(|old| id_remap.get(&old).copied())
            .collect();
        *bitmap = new_bitmap;
    }
    self.exact.retain(|_, b| !b.is_empty());

    self.interner.rewrite_with(&id_remap);

    self.entities = new_entities;
    self.live = (0..live_ids.len() as u32).collect();
    self.dead_count = 0;
}
```

Compaction is O(live + posting lists). On 50k entities with 50% dead, expect
single-digit milliseconds. Triggered automatically; can be deferred via a
"no-compact-during-query-burst" guard if needed.

## Concurrency

- Queries take `&self`. Multiple concurrent queries are safe.
- Mutations take `&mut self`. Wrap in `Arc<RwLock<SearchIndex<T>>>` at the call
  site if you need concurrent reads + occasional writes. The expected pattern:
  one writer (the entity store sync loop), many readers.
- The internal `par_iter` calls use rayon's global thread pool. If myko already
  has a custom pool, expose a `ThreadPool` parameter on `SearchIndex::new`.
- Nucleo matchers are **not** `Sync`. We use `thread_local!` via the
  `thread_local` crate so each rayon worker gets its own matcher with reusable
  scratch buffers.

## Performance targets

Measured on a single 4-core machine, 50k entities per type, 5 fields per entity,
average 30 chars per field:

|Operation               |Target  |Hard ceiling|
|------------------------|--------|------------|
|Exact query (cache hit) |< 50 µs |200 µs      |
|Subsequence query (cold)|< 5 ms  |15 ms       |
|Typo query (cold)       |< 8 ms  |25 ms       |
|Combined query          |< 10 ms |30 ms       |
|Insert                  |< 20 µs |100 µs      |
|Update                  |< 50 µs |200 µs      |
|Remove                  |< 10 µs |50 µs       |
|Compact (50% dead)      |< 20 ms |100 ms      |

Insert/update targets are tighter than the original spec because the typed
extractor path skips serde entirely. Targets assume the index fits in L3. At
50k × ~500 bytes per record, that's ~25 MB.

## Migration from tantivy

The cutover lives behind the existing `search` cargo feature. Both the legacy
`search/index.rs` (tantivy) and the new `search/typed/` module compile under the
same gate during phases 1-3; phase 4 deletes the tantivy half. Consumers that
currently compile without the `search` feature continue to get the no-op stub
(`search/mod.rs:46-74`).

1. **Phase 1: parallel deployment.** Ship the typed index alongside the existing
   tantivy index. Mirror writes to both via a wrapper `SearchIndex` in
   `myko-core` that fans out. Queries still go to tantivy. Compare result sets
   in CI / staging.

1. **Phase 2: shadow read.** Queries hit both, return tantivy results, log
   discrepancies. Tune scoring tiers and tokenization until discrepancy rate is
   acceptable for non-trivial queries. This is a *behavioral* comparison —
   "does the new index return the hits a user would expect" — not a perf
   bake-off. Expect to spend more time here than on the implementation.

1. **Phase 3: cutover.** Flip the read path to the typed index. Keep tantivy
   writes as a rollback safety net for one release cycle.

1. **Phase 4: removal.** Delete the tantivy dependency, the schema definition
   in `index.rs`, and the JSON `extract_searchable_text` path. Reclaim startup
   time and binary size.

### Call sites to migrate

The fan-out wrapper in phase 1 keeps `search_index.index_item(...)` /
`remove_entity(...)` signatures unchanged so the following call sites do not
need to be touched until phase 4:

- `libs/myko/core/src/server/context.rs`: lines 184, 215, 226, 251-252, 404, 449,
  500, 562, 615, 659, 709, 753, 797, 940, 950, 1686
- `libs/myko/server/src/lib.rs`: lines 186, 267, 300, 358, 422, 445, 558
- `libs/myko/core/tests/query_cache_leak_test.rs`: line 31

Phase 4 deletes `extract_searchable_text` (`search/mod.rs:100`) and the JSON
path; the typed-extractor registrations from the macro replace it.

## Testing

### Unit tests

- `interner.rs`: insert/lookup/recycle, dense slot reuse, idempotent intern.
- `matchers/exact.rs`: token equality, case insensitivity, separator splitting.
- `matchers/subsequence.rs`: known fzf cases ("fma" → "FullMetalAlchemist"),
  word-boundary scoring, smart-case behavior.
- `matchers/typo.rs`: distance-1 and distance-2 hits, length prefilter
  correctness, no false positives at distance 3.
- `score.rs`: tier ordering invariants — every Exact > every Prefix > every
  Subsequence > every Typo.

### Property tests (proptest)

- For any insert/remove sequence, `len()` matches the live set.
- After `compact()`, all queries return the same results as before.
- Subsequence and exact matchers never disagree about whether a token literally
  appears in an entity.
- Update is observably equivalent to remove + insert.

### Benchmarks (criterion)

- Query latency at 1k, 10k, 50k, 200k entities.
- Insert throughput cold (empty index) and warm (10k existing entities).
- Compaction latency at varying dead ratios (10%, 30%, 50%, 70%).
- Comparison against the existing tantivy implementation on the same workload.
- **Specifically: insert latency with vs without typed extractors** to quantify
  the serde elimination win.

### Integration tests

- Use real `#[myko_item]` entities (per `CLAUDE.md`'s "Use real entities with
  macros in tests"). `BenchItem` in `libs/myko/core/src/bench_entities.rs` is a
  ready-made fixture; add a `#[searchable]` field to it.
- Full index lifecycle: build from a fixture entity set, run a battery of
  queries (exact, prefix, subsequence, typo, mixed), assert expected hit sets.
- Concurrent query + mutation workload via `Arc<RwLock<_>>`, no panics, no torn
  reads.
- Reactive: subscribe via the generated `Search<T>` report through the standard
  `ReportContext` plumbing, mutate via the typed store, observe the report
  re-fires (validates that we are using the existing subscription path, not a
  bespoke index hook).

### Fuzz

- Query string fuzzing with `cargo-fuzz`. Goal: never panic on any byte sequence
  as input.
- Mutation sequence fuzzing: random insert/update/remove sequences, periodic
  `compact()`, assert invariants after each operation.

## Implementation phases

**Phase A — core (1-2 days):**

- Module scaffolding at `libs/myko/core/src/search/typed/`, dependency adds in
  `myko-core/Cargo.toml` (nucleo, roaring, strsim, rayon, thread_local,
  smallvec) gated behind the existing `search` feature.
- `IdInterner`, `EntityRecord`, basic `SearchIndex<T>` with insert/remove.
- Macro changes in `myko-macros` to emit `TypedSearchableRegistration<T>`
  alongside the existing string registration (both coexist during migration).
- Exact match only.
- Unit tests for the above against a real `BenchItem`-style entity.

**Phase B — subsequence (1 day):**

- Nucleo integration, thread-local matcher pool.
- Parallel scan via rayon.
- Tier merge logic.
- fzf-corpus tests.

**Phase C — typo (1 day):**

- Levenshtein scan with length prefilter.
- Three-tier merge.
- Property tests for tier invariants.

**Phase D — generated reports + reactivity (2 days):**

- Macro emits `Search{T}` and `SearchResult<T>` per entity that has any
  `#[searchable]` field.
- `compute` impl uses the existing report subscription pattern against the
  typed store.
- `EntitySearch` shim rewired to delegate through the typed registry.
- Concurrent test suite.
- Cross-language codegen verification (`cargo flux run gen`).

**Phase E — migration plumbing (variable):**

- Fan-out wrapper for shadow reads.
- Discrepancy logging.
- Tuning loop on real workloads.
- Tantivy removal.

Total: roughly one engineer-week to a deployable v1, plus migration tail.

## Dependencies

Added to `libs/myko/core/Cargo.toml` as optional dependencies under the
existing `search` feature:

```toml
[dependencies]
nucleo = { version = "0.5", optional = true }
roaring = { version = "0.10", optional = true }
strsim = { version = "0.11", optional = true }
rayon = { version = "1.10", optional = true }
thread_local = { version = "1.1", optional = true }
smallvec = { version = "1.13", optional = true }
# tantivy stays here through phase 3; removed in phase 4
tantivy = { version = "0.22", optional = true }

[features]
search = [
    "dep:tantivy",
    "dep:nucleo", "dep:roaring", "dep:strsim",
    "dep:rayon", "dep:thread_local", "dep:smallvec",
]
```

`inventory` and `hyphae` are already workspace deps in `myko-core` and need no
changes.

Optional, gated behind an additional `search-simd` feature flag:

```toml
triple_accel = { version = "0.4", optional = true }
```

In phase 4, `tantivy` and the legacy `search/index.rs` path are removed; the
remaining `search` feature gates only the typed implementation.

## Open questions

1. **Field weighting.** Should hits in a "name" field outrank hits in a "tags"
   field for the same score tier? v1 answer: no, all fields equal. Revisit
   after migration if users complain.
1. **Cross-type search.** Pulse and optik both want a global "search anything"
   command palette. Approach: the existing stringly-typed `EntitySearch` shim
   already federates per-type indexes at query time via the registry. For the
   command palette, fan out across types in parallel and merge tier-by-tier.
1. **Tag normalization.** Tags often arrive as `kebab-case`, `snake_case`, or
   `CamelCase`. Should the tokenizer split on case boundaries (so "lightingCue"
   tokenizes as "lighting" + "cue")? Inclined to say yes for the subsequence
   haystack but no for exact match, since exact match should preserve user
   intent.
1. **Per-entity-type configuration.** Some types (file paths, namespaced ids)
   benefit from `nucleo::Config::DEFAULT.match_paths()`. Mechanism: add a
   `#[searchable(path)]` attribute variant the macro picks up. Not needed v1.
1. **Memory footprint at higher scale.** If a single myko instance ends up with
   5M+ entities across types, we'll hit the FST + overlay tier discussed
   previously. Add a feature flag rather than a rewrite.

-----

End of spec. Implementer should treat the macro-generated reports as the public
contract; the internal `SearchIndex<T>`, record layout, and scan parallelism can
be revised as long as performance targets are met and the generated report
surface is stable.
