use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

pub use myko::*;

use myko::{
    prelude::*,
    report::export_tree::{ChildKind, ChildRelation, walk_tree},
    store::StoreRegistry,
};

#[myko_item]
pub struct ExportTreeNode {
    pub parent_id: Option<Arc<str>>,
}

static EXTRACTED: AtomicUsize = AtomicUsize::new(0);

#[test]
fn tree_walk_indexes_each_foreign_key_once() -> anyhow::Result<()> {
    let count = std::env::var("MYKO_EXPORT_BENCH_ROWS")
        .ok()
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(5_000);
    let registry = StoreRegistry::new();
    let store = registry.get_or_create(ExportTreeNode::ENTITY_NAME_STATIC);
    for index in 0..count {
        let item = ExportTreeNode {
            id: index.to_string().into(),
            parent_id: (index > 0).then(|| Arc::from(((index - 1) / 4).to_string())),
        };
        store.insert(item.id.as_ref().into(), Arc::new(item));
    }
    let adjacency = HashMap::from([(
        ExportTreeNode::ENTITY_NAME_STATIC,
        vec![ChildRelation {
            child_type: ExportTreeNode::ENTITY_NAME_STATIC,
            kind: ChildKind::BelongsTo {
                extract_fk: |item| {
                    EXTRACTED.fetch_add(1, Ordering::Relaxed);
                    item.downcast_ref::<ExportTreeNode>()?.parent_id.clone()
                },
            },
        }],
    )]);
    EXTRACTED.store(0, Ordering::Relaxed);
    let started = Instant::now();
    let rows = walk_tree(
        ExportTreeNode::ENTITY_NAME_STATIC,
        "0",
        &registry,
        &adjacency,
    );
    eprintln!(
        "export {count} rows: {:?}, {} FK extractions",
        started.elapsed(),
        EXTRACTED.load(Ordering::Relaxed)
    );
    anyhow::ensure!(rows.len() == count, "every descendant must be exported");
    anyhow::ensure!(
        EXTRACTED.load(Ordering::Relaxed) == count,
        "each FK must be extracted once"
    );

    let ensured = HashMap::from([(
        ExportTreeNode::ENTITY_NAME_STATIC,
        vec![ChildRelation {
            child_type: ExportTreeNode::ENTITY_NAME_STATIC,
            kind: ChildKind::EnsureFor {
                extract_fk: |item| item.downcast_ref::<ExportTreeNode>()?.parent_id.clone(),
            },
        }],
    )]);
    let ensured_rows = walk_tree(ExportTreeNode::ENTITY_NAME_STATIC, "0", &registry, &ensured);
    anyhow::ensure!(
        rows == ensured_rows,
        "EnsureFor must follow the same descendants"
    );
    Ok(())
}

#[test]
fn tree_walk_deduplicates_owned_cycles_and_skips_missing_children() {
    let registry = StoreRegistry::new();
    for id in ["root", "child"] {
        registry
            .get_or_create(ExportTreeNode::ENTITY_NAME_STATIC)
            .insert(
                Arc::from(id),
                Arc::new(ExportTreeNode {
                    id: id.into(),
                    parent_id: None,
                }),
            );
    }
    let adjacency = HashMap::from([(
        ExportTreeNode::ENTITY_NAME_STATIC,
        vec![ChildRelation {
            child_type: ExportTreeNode::ENTITY_NAME_STATIC,
            kind: ChildKind::OwnsMany {
                extract_ids: |_| Some(vec!["root".into(), "child".into(), "missing".into()]),
            },
        }],
    )]);
    let rows = walk_tree(
        ExportTreeNode::ENTITY_NAME_STATIC,
        "root",
        &registry,
        &adjacency,
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter()
            .map(|row| row.data.get("id").and_then(serde_json::Value::as_str))
            .collect::<Vec<_>>(),
        vec![Some("root"), Some("child")]
    );
    assert!(
        walk_tree(
            ExportTreeNode::ENTITY_NAME_STATIC,
            "absent",
            &registry,
            &adjacency
        )
        .is_empty()
    );
}
