use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::{item::Eventable, server::relationship_manager::EnsureForLookup};

static FK_EXTRACTIONS: AtomicUsize = AtomicUsize::new(0);

fn counted_left(item: &dyn std::any::Any) -> Option<Arc<str>> {
    FK_EXTRACTIONS.fetch_add(1, Ordering::Relaxed);
    item.downcast_ref::<NodePair>()
        .map(|pair| pair.left_id.0.clone())
}

fn counted_right(item: &dyn std::any::Any) -> Option<Arc<str>> {
    FK_EXTRACTIONS.fetch_add(1, Ordering::Relaxed);
    item.downcast_ref::<NodePair>()
        .map(|pair| pair.right_id.0.clone())
}

fn loaded_pairs(n: usize) -> (MykoServerContext, Arc<StoreRegistry>, RelationshipManager) {
    let (ctx, registry) = make_ctx();
    let nodes = registry.get_or_create(Node::ENTITY_NAME_STATIC);
    let pairs = registry.get_or_create(NodePair::ENTITY_NAME_STATIC);
    for i in 0..n {
        let node = Node {
            id: format!("startup-node-{i}").into(),
            name: i.to_string(),
        };
        nodes.insert(node.id.0.clone(), Arc::new(node));
        for j in 0..n {
            let pair = NodePair {
                id: format!("startup-pair-{i}-{j}").into(),
                left_id: format!("startup-node-{i}").into(),
                right_id: format!("startup-node-{j}").into(),
            };
            pairs.insert(pair.id.0.clone(), Arc::new(pair));
        }
    }
    (ctx, registry, RelationshipManager::new())
}

#[test]
fn startup_reads_each_existing_dependency_tuple_once() {
    let _serial = scheduler_test_serial();
    let (ctx, registry, mut manager) = loaded_pairs(32);
    let lookup = manager
        .ensure_for_by_dependency
        .values()
        .flatten()
        .find(|lookup| lookup.local_type == NodePair::ENTITY_NAME_STATIC)
        .cloned();
    assert!(lookup.is_some());
    let Some(mut lookup) = lookup else { return };
    assert_eq!(lookup.dependencies.len(), 2);
    let Some(left) = lookup.dependencies.first_mut() else {
        return;
    };
    left.extract_fk = counted_left;
    let Some(right) = lookup.dependencies.get_mut(1) else {
        return;
    };
    right.extract_fk = counted_right;
    manager.ensure_for_by_dependency.clear();
    manager
        .ensure_for_by_dependency
        .insert(Node::ENTITY_NAME_STATIC, vec![lookup]);

    for _ in 0..2 {
        FK_EXTRACTIONS.store(0, Ordering::Relaxed);
        assert!(manager.initialize_ensure_for(&ctx).is_ok());
        assert_eq!(FK_EXTRACTIONS.load(Ordering::Relaxed), 2048);
        assert_eq!(node_pairs(&registry).len(), 1024);
    }
}

#[test]
fn startup_fills_missing_pairs_and_preserves_historical_ids_and_duplicates() {
    let _serial = scheduler_test_serial();
    let (ctx, registry, manager) = loaded_pairs(3);
    let pairs = registry.get_or_create(NodePair::ENTITY_NAME_STATIC);
    pairs.remove(&Arc::from("startup-pair-1-2"));
    let duplicate = NodePair {
        id: "historical-duplicate".into(),
        left_id: "startup-node-0".into(),
        right_id: "startup-node-0".into(),
    };
    pairs.insert(duplicate.id.0.clone(), Arc::new(duplicate));
    let original_ids: std::collections::HashSet<_> =
        pairs.snapshot().into_iter().map(|(id, _)| id).collect();

    for _ in 0..2 {
        assert!(manager.establish_relations(&ctx).is_ok());
        let rows = node_pairs(&registry);
        assert_eq!(rows.len(), 10);
        let ids: std::collections::HashSet<_> = rows.iter().map(|row| row.id.0.clone()).collect();
        assert!(original_ids.is_subset(&ids));
        assert_eq!(
            rows.iter()
                .filter(|row| row.left_id.as_ref() == "startup-node-1"
                    && row.right_id.as_ref() == "startup-node-2")
                .count(),
            1
        );
        let tuples: std::collections::HashSet<_> = rows
            .iter()
            .map(|row| (row.left_id.clone(), row.right_id.clone()))
            .collect();
        assert_eq!(tuples.len(), 9);
    }
}

#[test]
fn startup_with_empty_dependency_store_creates_no_pairs() {
    let _serial = scheduler_test_serial();
    let (ctx, registry, manager) = loaded_pairs(0);
    assert!(manager.initialize_ensure_for(&ctx).is_ok());
    assert!(node_pairs(&registry).is_empty());
}

#[test]
#[ignore = "manual scaling fixture; run the benchmark-startup-relationships Flux task"]
fn benchmark_startup_ensure_membership() {
    use std::time::Instant;

    let _serial = scheduler_test_serial();
    for n in [16, 32, 64] {
        let (ctx, registry, manager) = loaded_pairs(n);
        let lookup: Option<&EnsureForLookup> = manager
            .ensure_for_by_dependency
            .values()
            .flatten()
            .find(|lookup| lookup.local_type == NodePair::ENTITY_NAME_STATIC);
        assert!(lookup.is_some());
        let Some(lookup) = lookup else { return };
        let start = Instant::now();
        let combinations =
            RelationshipManager::get_dependency_combinations(&ctx, &lookup.dependencies);
        let store = registry.get_or_create(NodePair::ENTITY_NAME_STATIC);
        let existing = store.snapshot();
        for combo in &combinations {
            assert!(
                RelationshipManager::find_ensure_for_entity_in(
                    &existing,
                    &lookup.dependencies,
                    combo
                )
                .is_some()
            );
        }
        let scanned = start.elapsed();
        let start = Instant::now();
        assert!(manager.initialize_ensure_for(&ctx).is_ok());
        let indexed = start.elapsed();
        assert_eq!(node_pairs(&registry).len(), combinations.len());
        eprintln!(
            "{} existing pairs: scan={scanned:?}, indexed startup={indexed:?}",
            combinations.len()
        );
    }
}
