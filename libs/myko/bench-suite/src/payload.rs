//! Row builders. Payload sizes are the serialized JSON size of the nested
//! `data` value: 128 B (small), 512 B (the load test's
//! `structured_payload_bytes`) and 4 KB (large).

use std::sync::Arc;

use myko::{
    core::item::AnyItem,
    serde_json::{self, Map, Value, json},
};

use super::entities::*;
use crate::compat::pulse::{BPulse, BPulseId};

/// A nested JSON value whose serialized size is close to `bytes`.
pub fn blob(bytes: usize, salt: u64) -> Blob {
    let mut params = Map::new();
    let mut root = json!({
        "position": {"x": 1.5, "y": -2.25, "z": 0.0},
        "transform": {"scale": [1.0, 1.0, 1.0], "rotation": [0.0, 0.0, 0.0, 1.0]},
        "salt": salt,
        "labels": ["alpha", "beta"],
    });
    let base = serde_json::to_string(&root).map_or(0, |text| text.len());
    // Roughly two thirds numeric parameters, the rest a note string, which is
    // the mix a binding node or emitter schema carries.
    let mut used = base + 24;
    let mut index = 0u32;
    while used + 28 < bytes.saturating_mul(2) / 3 {
        params.insert(
            format!("param_{index:04}"),
            json!({"v": f64::from(index) * 0.125 + 0.5, "on": index % 2 == 0}),
        );
        used += 44;
        index += 1;
    }
    if let Value::Object(map) = &mut root {
        map.insert("params".into(), Value::Object(params));
        let note = bytes.saturating_sub(used + 12);
        map.insert("note".into(), Value::String("n".repeat(note)));
    }
    Blob(root)
}

pub fn blob_len(blob: &Blob) -> usize {
    serde_json::to_string(&blob.0).map_or(0, |text| text.len())
}

pub fn scene_id(index: usize) -> BSceneId {
    BSceneId(Arc::from(format!("sc-{index:06}")))
}

pub fn scene(index: usize, projects: usize) -> BScene {
    BScene {
        id: scene_id(index),
        name: format!("Scene {index}"),
        project_id: Arc::from(format!("project-{}", index % projects.max(1))),
        color: format!("#{:06x}", index * 2654435 % 0xff_ffff),
        tags: vec![Arc::from("show"), Arc::from("bench")],
        armed: index % 2 == 0,
        revision: 0,
        settings: blob(256, index as u64),
    }
}

pub fn node_id(index: usize) -> BNodeId {
    BNodeId(Arc::from(format!("n-{index:07}")))
}

/// Builds nodes cheaply from one template per payload size.
pub struct NodeMaker {
    template: Blob,
    scenes: usize,
    categories: usize,
    types: Vec<Arc<str>>,
    anchors: Vec<Arc<str>>,
}

impl NodeMaker {
    pub fn new(payload_bytes: usize, scenes: usize, categories: usize) -> Self {
        Self {
            template: blob(payload_bytes, 0),
            scenes: scenes.max(1),
            categories: categories.max(1),
            types: [
                "speed",
                "multiply",
                "add",
                "offset",
                "switch",
                "comp-element",
            ]
            .into_iter()
            .map(Arc::from)
            .collect(),
            anchors: ["in", "out", "exec"].into_iter().map(Arc::from).collect(),
        }
    }

    pub fn payload_len(&self) -> usize {
        blob_len(&self.template)
    }

    pub fn scene_of(&self, index: usize) -> usize {
        index % self.scenes
    }

    pub fn node(&self, index: usize, tick: u64, seq: u64, writer: u32) -> BNode {
        BNode {
            id: node_id(index),
            scene_id: scene_id(self.scene_of(index)),
            node_type: self.types[index % self.types.len()].clone(),
            display_name: (index % 3 == 0).then(|| format!("Node {index}")),
            category: Arc::from(format!("cat-{}", index % self.categories)),
            anchors: self.anchors.clone(),
            enabled: true,
            x: index as f64 + seq as f64 * 0.5,
            y: seq as f64,
            tick,
            seq,
            writer,
            // Not `Blob::clone`: the harness's own copy must not be counted.
            data: Blob(self.template.0.clone()),
        }
    }

    pub fn item(&self, index: usize, tick: u64, seq: u64, writer: u32) -> Arc<dyn AnyItem> {
        Arc::new(self.node(index, tick, seq, writer))
    }
}

pub fn target_id(index: usize) -> BTargetId {
    BTargetId(Arc::from(format!("t-{index:06}")))
}

pub fn target(index: usize, revision: u64) -> BTarget {
    BTarget {
        id: target_id(index),
        name: format!("Target {index}"),
        parent_targets: if index % 4 == 0 {
            vec![]
        } else {
            vec![target_id(index - index % 4)]
        },
        service_id: format!("service-{}", index % 16),
        category: format!("category-{}", index % 8),
        managed: (index % 5 == 0).then_some(true),
        tags: vec![Arc::from("bench")],
        revision,
        metadata: blob(256, index as u64),
    }
}

pub fn emitter_id(index: usize) -> BEmitterId {
    BEmitterId(Arc::from(format!("e-{index:06}")))
}

pub fn emitter(index: usize, target: usize, revision: u64) -> BEmitter {
    BEmitter {
        id: emitter_id(index),
        name: format!("Emitter {index}"),
        category: Some(Arc::from("sensor")),
        target_id: target_id(target),
        service_id: format!("service-{}", target % 16),
        revision,
        schema: Some(blob(256, index as u64)),
    }
}

pub fn gauge(index: usize, revision: u64) -> BGauge {
    BGauge {
        id: BGaugeId(Arc::from(format!("g-{index:06}"))),
        emitter_id: emitter_id(index),
        revision,
        level: revision as f64,
    }
}

/// One pulse row per emitter, overwritten by each pulse, as in rship.
pub fn pulse(emitter: usize, seq: u64, writer: u32, data: &Blob) -> BPulse {
    BPulse {
        id: BPulseId(Arc::from(format!("p-{emitter:06}"))),
        emitter_id: emitter_id(emitter),
        seq,
        writer,
        data: Blob(data.0.clone()),
    }
}

pub fn probe(seq: u64) -> BProbe {
    BProbe {
        id: BProbeId(Arc::from("probe-0")),
        name: Arc::from("probe"),
        seq,
        writer: 7,
        value: seq as f64,
    }
}

pub fn part_id(index: usize) -> BPartId {
    BPartId(Arc::from(format!("part-{index:06}")))
}

pub fn part(index: usize) -> BPart {
    BPart {
        id: part_id(index),
        name: Arc::from(format!("Part {index}")),
        revision: 0,
        data: blob(128, index as u64),
    }
}

pub fn bundle(index: usize, parts_each: usize, scenes: usize) -> BBundle {
    BBundle {
        id: BBundleId(Arc::from(format!("b-{index:06}"))),
        scope_id: scene_id(index % scenes.max(1)),
        name: Arc::from(format!("Bundle {index}")),
        part_ids: (0..parts_each)
            .map(|part| part_id(index * parts_each + part))
            .collect(),
    }
}
