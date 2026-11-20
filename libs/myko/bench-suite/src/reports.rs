//! Reports modelled on rship's: a keyed lookup with a map, a report that
//! reads another report, and a grouped report over one scene's nodes.

use std::sync::Arc;

use myko::{
    prelude::*,
    report::{ReportContext, ReportHandler},
};

use super::entities::*;

#[myko_report_output]
pub struct NodeValueOut {
    pub node_id: Arc<str>,
    pub tick: u64,
    pub seq: u64,
    pub writer: u32,
    pub magnitude: f64,
    pub label: String,
}

/// One node's derived value: a lookup by id and a map.
#[myko_report(NodeValueOut)]
pub struct NodeValue {
    pub node_id: BNodeId,
}

impl ReportHandler for NodeValue {
    type Output = NodeValueOut;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        let node_id = self.node_id.0.clone();
        ctx.query_map(GetBNodesByIds {
            ids: vec![self.node_id.clone()],
        })
        .items()
        .map(move |items| {
            let node = items.first();
            Arc::new(NodeValueOut {
                node_id: node_id.clone(),
                tick: node.map_or(super::harness::NONE, |node| node.tick),
                seq: node.map_or(0, |node| node.seq),
                writer: node.map_or(0, |node| node.writer),
                magnitude: node.map_or(0.0, |node| node.x.hypot(node.y)),
                label: node.map_or_else(String::new, |node| {
                    format!("{}:{}", node.node_type, node.category)
                }),
            })
        })
        .materialize()
    }
}

#[myko_report_output]
pub struct ChainOut {
    pub node_id: Arc<str>,
    pub depth: u32,
    pub seq: u64,
    pub total: f64,
}

/// A report that reads the report one level below it. Depth 0 reads the node.
#[myko_report(ChainOut)]
pub struct Chain {
    pub node_id: BNodeId,
    pub depth: u32,
}

impl ReportHandler for Chain {
    type Output = ChainOut;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        let node_id = self.node_id.0.clone();
        let depth = self.depth;
        if depth == 0 {
            return ctx
                .query_map(GetBNodesByIds {
                    ids: vec![self.node_id.clone()],
                })
                .items()
                .map(move |items| {
                    let node = items.first();
                    Arc::new(ChainOut {
                        node_id: node_id.clone(),
                        depth: 0,
                        seq: node.map_or(0, |node| node.seq),
                        total: node.map_or(0.0, |node| node.x),
                    })
                })
                .materialize();
        }
        ctx.report(Self {
            node_id: self.node_id.clone(),
            depth: depth - 1,
        })
        .map(move |below| {
            Arc::new(ChainOut {
                node_id: node_id.clone(),
                depth,
                seq: below.seq,
                total: below.total + 1.0,
            })
        })
        .materialize()
    }
}

#[myko_report_output]
pub struct SceneStatsOut {
    pub scene_id: Arc<str>,
    pub count: usize,
    pub max_seq: u64,
    pub sum_x: f64,
}

/// Grouped report: an aggregate over the nodes of one scene.
#[myko_report(SceneStatsOut)]
pub struct SceneStats {
    pub scene_id: BSceneId,
}

impl ReportHandler for SceneStats {
    type Output = SceneStatsOut;

    fn compute(&self, ctx: ReportContext) -> impl Materialize<Arc<Self::Output>, Definite> {
        let scene_id = self.scene_id.0.clone();
        ctx.query_map(GetBNodesByQuery(BNodeQuery {
            scene_id: Some(IdFilter::Eq(self.scene_id.clone())),
            ..Default::default()
        }))
        .items()
        .map(move |items| {
            Arc::new(SceneStatsOut {
                scene_id: scene_id.clone(),
                count: items.len(),
                max_seq: items.iter().map(|node| node.seq).max().unwrap_or(0),
                sum_x: items.iter().map(|node| node.x).sum(),
            })
        })
        .materialize()
    }
}
