//! No write, read or delivery copies an entity. `Blob`'s `Clone` counts
//! while counting is on, so a deep clone of a `BPart` anywhere on these
//! paths shows up as a count.

#![allow(clippy::panic, clippy::missing_panics_doc, clippy::unwrap_used)]

use std::{
    sync::{Arc, atomic::Ordering::Relaxed},
    time::Duration,
};

use myko::request::RequestContext;
use myko_bench_suite::{
    entities::{BPart, BPartId, Blob, GetBPartById, GetBPartsByIds, blob},
    harness::{Bench, Wire},
};

fn part(index: usize, revision: u64) -> BPart {
    BPart {
        id: BPartId(Arc::from(format!("part-{index}"))),
        name: Arc::from("part"),
        revision,
        data: Blob(myko::serde_json::Value::Null),
    }
}

fn settle(bench: &Bench) {
    bench
        .sink
        .quiesce(Duration::from_millis(30), Duration::from_secs(10));
}

/// Deep clones of `BPart` made while `op` runs and its effects settle.
fn clones(bench: &Bench, op: impl FnOnce()) -> u64 {
    settle(bench);
    blob::CLONES.store(0, Relaxed);
    blob::COUNTING.store(true, Relaxed);
    op();
    settle(bench);
    blob::COUNTING.store(false, Relaxed);
    blob::CLONES.load(Relaxed)
}

#[test]
fn writes_reads_and_deliveries_never_deep_clone_an_entity() {
    let bench = Bench::new(Wire::Json, 16, 1);
    let request = Arc::new(RequestContext::from_client(
        Arc::from("tx-clones"),
        Arc::from("client-clones"),
        bench.ctx.host_id,
    ));
    for index in 0..10 {
        bench.ctx.set(part(index, 0)).unwrap();
    }

    assert_eq!(
        clones(&bench, || bench.ctx.set(part(1, 1)).unwrap()),
        0,
        "a write of an existing row"
    );

    let mut client = bench.client("subscriber");
    client.sub_query(&GetBPartsByIds {
        ids: vec![BPartId(Arc::from("part-2"))],
    });
    client.sub_report(&GetBPartById {
        id: BPartId(Arc::from("part-2")),
    });
    let live = bench.ctx.query_map_untyped(
        GetBPartsByIds {
            ids: vec![BPartId(Arc::from("part-2"))],
        },
        request.clone(),
    );
    assert_eq!(
        clones(&bench, || bench.ctx.set(part(2, 1)).unwrap()),
        0,
        "a write delivered to a live by-ids query, a by-id report and a session"
    );
    assert_eq!(live.snapshot().len(), 1);

    assert_eq!(
        clones(&bench, || {
            let rows = bench.ctx.query_snapshot(
                GetBPartsByIds {
                    ids: vec![BPartId(Arc::from("part-3"))],
                },
                request.clone(),
            );
            assert_eq!(rows.len(), 1);
        }),
        0,
        "a query_snapshot"
    );

    assert_eq!(
        clones(&bench, || {
            use myko::hyphae::Gettable;
            let report = bench.ctx.report(
                GetBPartById {
                    id: BPartId(Arc::from("part-4")),
                },
                request.clone(),
            );
            assert!(report.get().is_some());
        }),
        0,
        "a by-id read"
    );
    drop((client, live));
}
