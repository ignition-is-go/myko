//! Microbenchmarks isolating the runtime cost of myko's `Arc<dyn AnyItem>`
//! storage vs. typed `Arc<T>` storage. The intent is *not* to bench myko
//! end-to-end — it's to attribute the per-item cost on the dyn boundary
//! so the planned typed-store refactor can be sized.
//!
//! Scenarios:
//!   `predicate_*`   — per-item predicate cost (the hot loop in
//!                     `QueryFactory::cell_factory`'s `select`).
//!   `materialize_*` — full hyphae select + materialize over a populated
//!                     store, dyn vs. typed.
//!   `insert_many_*` — write throughput at the store boundary
//!                     (the `apply_event_batch` hot path on `CellMap::insert_many`).
//!   `serialize_*`   — single-item serialize via `erased_serde` (dyn) vs.
//!                     typed `serde_json`.
//!   `arc_clone_*`   — Arc<dyn>.`clone()` vs. Arc<T>.`clone()` (vtable carry).

use std::{hint::black_box, sync::Arc};

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use hyphae::{CellMap, MapEntriesExt, MapQuery, SelectExt};
use myko::{
    bench_entities::{BenchItem, BenchTreeItem},
    core::item::{AnyItem, downcast_any_item_arc},
    wire::ErasedWrappedItem,
};

const N_ITEMS: usize = 10_000;

fn make_items(n: usize) -> Vec<(Arc<str>, BenchItem)> {
    (0..n)
        .map(|i| {
            let id: Arc<str> = format!("item-{i}").into();
            let item = BenchItem {
                id: id.clone().into(),
                name: format!("name-{i}"),
                category: if i.is_multiple_of(4) {
                    "hot".into()
                } else {
                    "cold".into()
                },
                value: i64::try_from(i).unwrap_or(i64::MAX).rem_euclid(100),
            };
            (id, item)
        })
        .collect()
}

fn make_typed_arcs(n: usize) -> Vec<(Arc<str>, Arc<BenchItem>)> {
    make_items(n)
        .into_iter()
        .map(|(k, v)| (k, Arc::new(v)))
        .collect()
}

fn make_dyn_arcs(n: usize) -> Vec<(Arc<str>, Arc<dyn AnyItem>)> {
    make_items(n)
        .into_iter()
        .map(|(k, v)| {
            let arc: Arc<dyn AnyItem> = Arc::new(v);
            (k, arc)
        })
        .collect()
}

// ──────────────────────────────────────────────────────────────────────────
// Predicate-only: this is the *exact* per-item cost difference between
// `select(|item: &Arc<BenchItem>| item.value > 50)` and the current
// `select(|item_any: &Arc<dyn AnyItem>| { let item = downcast_any_item_arc::<BenchItem>(item_any, "..."); item.value > 50 })`.
// Isolated from hyphae, dashmap, allocations.
// ──────────────────────────────────────────────────────────────────────────

fn bench_predicate(c: &mut Criterion) {
    let typed = make_typed_arcs(N_ITEMS);
    let dynd = make_dyn_arcs(N_ITEMS);

    let mut g = c.benchmark_group("predicate_filter");
    g.throughput(criterion::Throughput::Elements(
        u64::try_from(N_ITEMS).unwrap_or(u64::MAX),
    ));

    g.bench_function("typed", |b| {
        b.iter(|| {
            let mut hits = 0usize;
            for (_, item) in &typed {
                if black_box(item.value) > 50 {
                    hits = hits.saturating_add(1);
                }
            }
            black_box(hits)
        });
    });

    g.bench_function("dyn_with_downcast", |b| {
        b.iter(|| {
            let mut hits = 0usize;
            for (_, item_any) in &dynd {
                let Some(item) = downcast_any_item_arc::<BenchItem>(item_any, "bench") else {
                    continue;
                };
                if black_box(item.value) > 50 {
                    hits = hits.saturating_add(1);
                }
                black_box(item);
            }
            black_box(hits)
        });
    });

    // Variant: skip the Arc clone in the downcast path — pure as_any().downcast_ref::<T>().
    // This is the floor for what a "smart" downcast helper could cost.
    g.bench_function("dyn_downcast_ref_only", |b| {
        b.iter(|| {
            let mut hits = 0usize;
            for (_, item_any) in &dynd {
                let Some(item) = item_any.as_any().downcast_ref::<BenchItem>() else {
                    continue;
                };
                if black_box(item.value) > 50 {
                    hits = hits.saturating_add(1);
                }
            }
            black_box(hits)
        });
    });

    g.finish();
}

// ──────────────────────────────────────────────────────────────────────────
// Full hyphae pipeline: build a CellMap, populate, then run select+materialize.
// This catches subscription/diff machinery cost on top of predicate cost.
// ──────────────────────────────────────────────────────────────────────────

fn bench_materialize(c: &mut Criterion) {
    let mut g = c.benchmark_group("materialize_select");
    g.throughput(criterion::Throughput::Elements(
        u64::try_from(N_ITEMS).unwrap_or(u64::MAX),
    ));

    g.bench_function("typed_store_select_materialize", |b| {
        let entries = make_typed_arcs(N_ITEMS);
        b.iter_batched(
            || {
                let m = CellMap::<Arc<str>, Arc<BenchItem>>::new();
                m.insert_many(entries.clone());
                m
            },
            |store| {
                let result = MapQuery::materialize(
                    (store.lock()).select(|item: &Arc<BenchItem>| item.value > 50),
                );
                black_box(result.snapshot().len())
            },
            BatchSize::SmallInput,
        );
    });

    g.bench_function("dyn_store_select_materialize", |b| {
        let entries = make_dyn_arcs(N_ITEMS);
        b.iter_batched(
            || {
                let m = CellMap::<Arc<str>, Arc<dyn AnyItem>>::new();
                m.insert_many(entries.clone());
                m
            },
            |store| {
                let result = MapQuery::materialize((store.lock()).clone().select(
                    |item_any: &Arc<dyn AnyItem>| {
                        downcast_any_item_arc::<BenchItem>(item_any, "bench")
                            .is_some_and(|item| item.value > 50)
                    },
                ));
                black_box(result.snapshot().len())
            },
            BatchSize::SmallInput,
        );
    });

    g.finish();
}

// ──────────────────────────────────────────────────────────────────────────
// Insert throughput on the store. This is the apply_event_batch shape:
// 10k items already parsed, dropped into a CellMap.
// ──────────────────────────────────────────────────────────────────────────

fn bench_insert_many(c: &mut Criterion) {
    let mut g = c.benchmark_group("insert_many");
    g.throughput(criterion::Throughput::Elements(
        u64::try_from(N_ITEMS).unwrap_or(u64::MAX),
    ));

    g.bench_function("typed", |b| {
        let entries = make_typed_arcs(N_ITEMS);
        b.iter_batched(
            || (CellMap::<Arc<str>, Arc<BenchItem>>::new(), entries.clone()),
            |(m, e)| {
                m.insert_many(e);
                black_box(m.snapshot().len())
            },
            BatchSize::SmallInput,
        );
    });

    g.bench_function("dyn", |b| {
        let entries = make_dyn_arcs(N_ITEMS);
        b.iter_batched(
            || {
                (
                    CellMap::<Arc<str>, Arc<dyn AnyItem>>::new(),
                    entries.clone(),
                )
            },
            |(m, e)| {
                m.insert_many(e);
                black_box(m.snapshot().len())
            },
            BatchSize::SmallInput,
        );
    });

    g.finish();
}

// ──────────────────────────────────────────────────────────────────────────
// Serialization. erased_serde::serialize (dyn AnyItem) vs typed serde.
// Wire-emit hot path on the server. 1k items per iter to amortize criterion overhead.
// ──────────────────────────────────────────────────────────────────────────

fn bench_serialize(c: &mut Criterion) {
    let typed = make_typed_arcs(1000);
    let dynd = make_dyn_arcs(1000);

    {
        let mut json_group = c.benchmark_group("serialize_to_json_bytes");
        json_group.throughput(criterion::Throughput::Elements(1000));

        json_group.bench_function("typed", |b| {
            b.iter(|| {
                let mut total = 0usize;
                for (_, item) in &typed {
                    if let Ok(bytes) = serde_json::to_vec(item.as_ref()) {
                        total = total.saturating_add(bytes.len());
                        black_box(bytes);
                    }
                }
                black_box(total)
            });
        });

        json_group.bench_function("dyn_erased_serde", |b| {
            b.iter(|| {
                let mut total = 0usize;
                for (_, item_any) in &dynd {
                    // serde::Serialize is implemented for `dyn AnyItem` via erased_serde.
                    if let Ok(bytes) = serde_json::to_vec(item_any.as_ref()) {
                        total = total.saturating_add(bytes.len());
                        black_box(bytes);
                    }
                }
                black_box(total)
            });
        });

        json_group.finish();
    }

    // The actual wire emit path: ErasedWrappedItem holding `Arc<dyn AnyItem>`,
    // serialized inside a larger JSON struct. Pre-fix went through
    // `&dyn erased_serde::Serialize`; post-fix dispatches to the typed shim
    // emitted by `myko_item` for human-readable serializers, falling back to
    // erased_serde for unregistered types and non-human-readable formats.
    let wrapped: Vec<ErasedWrappedItem> = dynd
        .iter()
        .map(|(_, item)| ErasedWrappedItem {
            item: item.clone(),
            item_type: "BenchItem".into(),
        })
        .collect();

    {
        let mut wrapped_group = c.benchmark_group("wrapped_item_serialize");
        wrapped_group.throughput(criterion::Throughput::Elements(1000));

        wrapped_group.bench_function("json_via_typed_registration", |b| {
            b.iter(|| black_box(serde_json::to_vec(&wrapped)));
        });

        // Force the erased_serde fallback by giving the wrapper a bogus item_type
        // so the registry lookup misses and `ErasedWrappedItem::serialize` falls
        // through to the old `&dyn erased_serde::Serialize` path. Same items,
        // same JSON output shape, only the code path differs.
        let wrapped_unregistered: Vec<ErasedWrappedItem> = dynd
            .iter()
            .map(|(_, item)| ErasedWrappedItem {
                item: item.clone(),
                item_type: "_unregistered_".into(),
            })
            .collect();

        wrapped_group.bench_function("json_via_erased_serde_fallback", |b| {
            b.iter(|| black_box(serde_json::to_vec(&wrapped_unregistered)));
        });

        wrapped_group.finish();
    }

    // CBOR is the production wire format for downstream consumers — measure
    // the erased_serde tax on the path that actually matters. Today there's
    // no typed shim for CBOR (ciborium has no RawValue / raw-embed
    // mechanism), so all paths still go through erased_serde; the
    // typed_baseline shows the floor we'd be chasing if we add a CBOR fix.
    let mut cbor_group = c.benchmark_group("cbor_serialize");
    cbor_group.throughput(criterion::Throughput::Elements(1000));

    cbor_group.bench_function("typed_baseline", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(64 * 1000);
            for (_, item) in &typed {
                if ciborium::ser::into_writer(item.as_ref(), &mut buf).is_err() {
                    break;
                }
            }
            black_box(buf.len())
        });
    });

    cbor_group.bench_function("dyn_via_erased_serde", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(64 * 1000);
            for (_, item_any) in &dynd {
                if ciborium::ser::into_writer(item_any.as_ref(), &mut buf).is_err() {
                    break;
                }
            }
            black_box(buf.len())
        });
    });

    cbor_group.bench_function("wrapped_item_via_erased_serde", |b| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(128 * 1000);
            let result = ciborium::ser::into_writer(&wrapped, &mut buf);
            let _ = black_box(result);
            black_box(buf.len())
        });
    });

    cbor_group.finish();
}

// ──────────────────────────────────────────────────────────────────────────
// Arc clone: dyn fat pointer (data + vtable) vs. thin pointer.
// Every diff fanout copies an Arc per subscriber.
// ──────────────────────────────────────────────────────────────────────────

fn bench_arc_clone(c: &mut Criterion) {
    let typed: Arc<BenchItem> = Arc::new(BenchItem {
        id: "x".into(),
        name: "x".into(),
        category: "x".into(),
        value: 0,
    });
    let dynd: Arc<dyn AnyItem> = typed.clone();

    let mut g = c.benchmark_group("arc_clone");

    g.bench_function("typed_arc_clone_x1000", |b| {
        b.iter(|| {
            let mut v: Vec<Arc<BenchItem>> = Vec::with_capacity(1000);
            for _ in 0..1000 {
                v.push(typed.clone());
            }
            black_box(v)
        });
    });

    g.bench_function("dyn_arc_clone_x1000", |b| {
        b.iter(|| {
            let mut v: Vec<Arc<dyn AnyItem>> = Vec::with_capacity(1000);
            for _ in 0..1000 {
                v.push(dynd.clone());
            }
            black_box(v)
        });
    });

    g.finish();
}

// ──────────────────────────────────────────────────────────────────────────
// Lineage walk: a filtered-tree view's pattern. Inside a typed view's
// project closure, walk N parent_id hops by `target_store.get(&pid)` +
// `as_any().downcast_ref::<Target>()`. This is the dyn-boundary cost that
// the earlier benches missed — every parent hop is a fresh downcast.
//
// 10k items in a 5-level forest; for each non-root item we walk up to 3 hops.
// ──────────────────────────────────────────────────────────────────────────

const TREE_TOTAL: usize = 10_000;
const TREE_LEVELS: usize = 5;
const TREE_PER_LEVEL: usize = TREE_TOTAL / TREE_LEVELS;
const LINEAGE_DEPTH: usize = 3;

fn make_tree() -> Vec<BenchTreeItem> {
    (0..TREE_TOTAL)
        .map(|i| {
            let level = i.checked_div(TREE_PER_LEVEL).unwrap_or_default();
            let parent_id = if level == 0 {
                None
            } else {
                let parent_idx = i
                    .saturating_sub(TREE_PER_LEVEL)
                    .checked_rem(TREE_PER_LEVEL)
                    .unwrap_or_default()
                    .saturating_add(level.saturating_sub(1).saturating_mul(TREE_PER_LEVEL));
                Some(format!("tree-{parent_idx}").into())
            };
            BenchTreeItem {
                id: format!("tree-{i}").into(),
                name: format!("node-{i}"),
                parent_id,
                depth: i64::try_from(level).unwrap_or(i64::MAX),
            }
        })
        .collect()
}

fn bench_lineage_walk(c: &mut Criterion) {
    let tree = make_tree();

    let typed_entries: Vec<(Arc<str>, Arc<BenchTreeItem>)> = tree
        .iter()
        .map(|t| (t.id.0.clone(), Arc::new(t.clone())))
        .collect();
    let dyn_entries: Vec<(Arc<str>, Arc<dyn AnyItem>)> = tree
        .iter()
        .map(|t| {
            let arc: Arc<dyn AnyItem> = Arc::new(t.clone());
            (t.id.0.clone(), arc)
        })
        .collect();

    let typed_store_mut = CellMap::<Arc<str>, Arc<BenchTreeItem>>::new();
    typed_store_mut.insert_many(typed_entries);
    let typed_store = typed_store_mut.lock();

    let dyn_store_mut = CellMap::<Arc<str>, Arc<dyn AnyItem>>::new();
    dyn_store_mut.insert_many(dyn_entries);
    let dyn_store = dyn_store_mut.lock();

    let mut g = c.benchmark_group("lineage_walk");
    g.throughput(criterion::Throughput::Elements(
        u64::try_from(TREE_TOTAL).unwrap_or(u64::MAX),
    ));

    g.bench_function("typed_store_get_value", |b| {
        b.iter(|| {
            let mut acc = 0i64;
            for entry in typed_store.snapshot() {
                let (_, mut current) = entry;
                for _ in 0..LINEAGE_DEPTH {
                    let Some(pid) = current.parent_id.clone() else {
                        break;
                    };
                    let Some(parent) = typed_store.get_value(&pid) else {
                        break;
                    };
                    acc = acc.saturating_add(parent.depth);
                    current = parent;
                }
            }
            black_box(acc)
        });
    });

    g.bench_function("dyn_store_get_value_with_downcast", |b| {
        b.iter(|| {
            let mut acc = 0i64;
            for entry in dyn_store.snapshot() {
                let (_, item_any) = entry;
                let Some(item) = item_any.as_any().downcast_ref::<BenchTreeItem>() else {
                    continue;
                };
                let mut current_pid = item.parent_id.clone();
                for _ in 0..LINEAGE_DEPTH {
                    let Some(pid) = current_pid.as_ref() else {
                        break;
                    };
                    let Some(parent_any) = dyn_store.get_value(pid) else {
                        break;
                    };
                    let Some(parent) = parent_any.as_any().downcast_ref::<BenchTreeItem>() else {
                        break;
                    };
                    acc = acc.saturating_add(parent.depth);
                    current_pid.clone_from(&parent.parent_id);
                }
            }
            black_box(acc)
        });
    });

    g.finish();
}

// ──────────────────────────────────────────────────────────────────────────
// View chain fanout: a filtered-tree view's shape. The outer pipeline
// is typed (operating on Arc<BenchTreeItem>); the variable is whether the
// inner cross-store lookup is typed or dyn. This isolates the question
// "if `registry.typed::<T>()` were available inside project transforms,
// how much faster does fanout get?"
//
// Stages:
//   source: CellMap<Arc<str>, Arc<BenchTreeItem>>  (the "view's source")
//   .select(depth >= 2)                            (typed predicate)
//   .project(walk LINEAGE_DEPTH hops via inner)    (variable: typed or dyn)
//
// Materialize per iter to capture the full subscribe-and-fan-in cost.
// ──────────────────────────────────────────────────────────────────────────

fn bench_view_chain_fanout(c: &mut Criterion) {
    let tree = make_tree();
    let typed_entries: Vec<(Arc<str>, Arc<BenchTreeItem>)> = tree
        .iter()
        .map(|t| (t.id.0.clone(), Arc::new(t.clone())))
        .collect();
    let dyn_entries: Vec<(Arc<str>, Arc<dyn AnyItem>)> = tree
        .iter()
        .map(|t| {
            let arc: Arc<dyn AnyItem> = Arc::new(t.clone());
            (t.id.0.clone(), arc)
        })
        .collect();

    let mut g = c.benchmark_group("view_chain_fanout");
    g.throughput(criterion::Throughput::Elements(
        u64::try_from(TREE_TOTAL).unwrap_or(u64::MAX),
    ));

    // Post-refactor: outer typed + typed inner store.
    g.bench_function("typed_outer_typed_inner", |b| {
        b.iter_batched(
            || {
                let outer = CellMap::<Arc<str>, Arc<BenchTreeItem>>::new();
                outer.insert_many(typed_entries.clone());
                let inner = CellMap::<Arc<str>, Arc<BenchTreeItem>>::new();
                inner.insert_many(typed_entries.clone());
                (outer, inner)
            },
            |(outer, inner)| {
                let inner_for_proj = inner;
                let result = MapQuery::materialize(
                    outer
                        .lock()
                        .select(|item: &Arc<BenchTreeItem>| item.depth >= 2)
                        .filter_map_entries(move |k, item| {
                            let mut total = item.depth;
                            let mut current_pid = item.parent_id.clone();
                            for _ in 0..LINEAGE_DEPTH {
                                let Some(pid) = current_pid.as_ref() else {
                                    break;
                                };
                                let Some(parent) = inner_for_proj.get_value(pid) else {
                                    break;
                                };
                                total = total.saturating_add(parent.depth);
                                current_pid.clone_from(&parent.parent_id);
                            }
                            Some((k.clone(), total))
                        }),
                );
                black_box(result.snapshot().len())
            },
            BatchSize::SmallInput,
        );
    });

    // Current world: outer typed + dyn inner store. The filtered-tree view
    // shape — `target_store.get(&pid)` returns `Arc<dyn AnyItem>` and project
    // downcasts on every hop.
    g.bench_function("typed_outer_dyn_inner", |b| {
        b.iter_batched(
            || {
                let outer = CellMap::<Arc<str>, Arc<BenchTreeItem>>::new();
                outer.insert_many(typed_entries.clone());
                let inner = CellMap::<Arc<str>, Arc<dyn AnyItem>>::new();
                inner.insert_many(dyn_entries.clone());
                (outer, inner)
            },
            |(outer, inner)| {
                let inner_for_proj = inner.clone();
                let result = MapQuery::materialize(
                    outer
                        .lock()
                        .select(|item: &Arc<BenchTreeItem>| item.depth >= 2)
                        .filter_map_entries(move |k, item| {
                            let mut total = item.depth;
                            let mut current_pid = item.parent_id.clone();
                            for _ in 0..LINEAGE_DEPTH {
                                let Some(pid) = current_pid.as_ref() else {
                                    break;
                                };
                                let Some(parent_any) = inner_for_proj.get_value(pid) else {
                                    break;
                                };
                                let Some(parent) =
                                    parent_any.as_any().downcast_ref::<BenchTreeItem>()
                                else {
                                    break;
                                };
                                total = total.saturating_add(parent.depth);
                                current_pid.clone_from(&parent.parent_id);
                            }
                            Some((k.clone(), total))
                        }),
                );
                black_box(result.snapshot().len())
            },
            BatchSize::SmallInput,
        );
    });

    g.finish();
}

// ──────────────────────────────────────────────────────────────────────────
// Hashmap hasher comparison: SipHash (default) vs AHash. Every store get,
// every cache lookup, every registry dispatch hits a HashMap or DashMap; if
// AHash is meaningfully faster on Arc<str> keys, it compounds across the
// system.
// ──────────────────────────────────────────────────────────────────────────

fn bench_hashmap_hasher(c: &mut Criterion) {
    use std::collections::HashMap;

    use dashmap::DashMap;

    let keys: Vec<Arc<str>> = (0..N_ITEMS).map(|i| format!("entity-{i}").into()).collect();

    // Build pre-populated maps so the bench measures lookup + insert mix on
    // realistic Arc<str> entity keys, not empty-map cost.
    let mut g = c.benchmark_group("hashmap_lookup");
    g.throughput(criterion::Throughput::Elements(
        u64::try_from(N_ITEMS).unwrap_or(u64::MAX),
    ));

    g.bench_function("std_hashmap_default_siphash", |b| {
        let mut map: HashMap<Arc<str>, i64> = HashMap::new();
        for (i, k) in keys.iter().enumerate() {
            map.insert(k.clone(), i64::try_from(i).unwrap_or(i64::MAX));
        }
        b.iter(|| {
            let mut acc = 0i64;
            for k in &keys {
                acc = acc.saturating_add(map.get(k).copied().unwrap_or(0));
            }
            black_box(acc)
        });
    });

    g.bench_function("std_hashmap_ahash", |b| {
        let mut map: HashMap<Arc<str>, i64, ahash::RandomState> =
            HashMap::with_hasher(ahash::RandomState::new());
        for (i, k) in keys.iter().enumerate() {
            map.insert(k.clone(), i64::try_from(i).unwrap_or(i64::MAX));
        }
        b.iter(|| {
            let mut acc = 0i64;
            for k in &keys {
                acc = acc.saturating_add(map.get(k).copied().unwrap_or(0));
            }
            black_box(acc)
        });
    });

    g.bench_function("dashmap_default_siphash", |b| {
        let map: DashMap<Arc<str>, i64> = DashMap::new();
        for (i, k) in keys.iter().enumerate() {
            map.insert(k.clone(), i64::try_from(i).unwrap_or(i64::MAX));
        }
        b.iter(|| {
            let mut acc = 0i64;
            for k in &keys {
                acc = acc.saturating_add(map.get(k).map_or(0, |r| *r.value()));
            }
            black_box(acc)
        });
    });

    g.bench_function("dashmap_ahash", |b| {
        let map: DashMap<Arc<str>, i64, ahash::RandomState> =
            DashMap::with_hasher(ahash::RandomState::new());
        for (i, k) in keys.iter().enumerate() {
            map.insert(k.clone(), i64::try_from(i).unwrap_or(i64::MAX));
        }
        b.iter(|| {
            let mut acc = 0i64;
            for k in &keys {
                acc = acc.saturating_add(map.get(k).map_or(0, |r| *r.value()));
            }
            black_box(acc)
        });
    });

    g.finish();
}

// ──────────────────────────────────────────────────────────────────────────
// Parse paths: bytes → Value → T (current) vs bytes → T direct. Measures
// the headroom for changing MEvent's wire shape and `Eventable::parse` to
// take raw bytes instead of `serde_json::Value`.
// ──────────────────────────────────────────────────────────────────────────

fn bench_parse_paths(c: &mut Criterion) {
    let item = BenchItem {
        id: "x".into(),
        name: "an item with a moderately long name field".into(),
        category: "category-with-some-content".into(),
        value: 12345,
    };
    let Ok(bytes) = serde_json::to_vec(&item) else {
        return;
    };

    let mut g = c.benchmark_group("parse_item");
    g.throughput(criterion::Throughput::Elements(1));

    g.bench_function("from_slice_to_typed_direct", |b| {
        b.iter(|| black_box(serde_json::from_slice::<BenchItem>(&bytes)));
    });

    g.bench_function("from_slice_to_value_then_from_value", |b| {
        b.iter(|| {
            let value = serde_json::from_slice::<serde_json::Value>(&bytes);
            black_box(value.and_then(serde_json::from_value::<BenchItem>))
        });
    });

    // Today's path includes a Value::clone before from_value (`parse_item`
    // at server/context.rs:361 does `parse(json.clone())`).
    g.bench_function("from_value_with_clone", |b| {
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
            return;
        };
        b.iter(|| {
            let cloned = value.clone();
            black_box(serde_json::from_value::<BenchItem>(cloned))
        });
    });

    // Post-fix path: parse_item now takes owned Value, so the clone is gone.
    // Each iter rebuilds the Value to simulate the apply_event_batch loop
    // where each event is moved; the bench measures `from_value` alone.
    g.bench_function("from_value_no_clone", |b| {
        b.iter_batched(
            || serde_json::from_slice::<serde_json::Value>(&bytes),
            |value| black_box(value.and_then(serde_json::from_value::<BenchItem>)),
            BatchSize::SmallInput,
        );
    });

    g.finish();
}

criterion_group!(
    benches,
    bench_predicate,
    bench_materialize,
    bench_insert_many,
    bench_serialize,
    bench_arc_clone,
    bench_lineage_walk,
    bench_view_chain_fanout,
    bench_hashmap_hasher,
    bench_parse_paths,
);
criterion_main!(benches);
