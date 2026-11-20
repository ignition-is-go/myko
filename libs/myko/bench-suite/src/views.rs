//! Views: a two-way join on a foreign key and a three-way join used by the
//! atomic-batch scenario.

use std::sync::Arc;

use myko::{
    hyphae::{InnerJoinExt as _, MapEntriesExt as _, MapQuery},
    prelude::*,
};

use super::entities::*;

#[allow(clippy::derive_partial_eq_without_eq)]
#[myko_view_item]
pub struct NodeWithScene {
    pub id: Arc<str>,
    pub scene_id: BSceneId,
    pub scene_name: String,
    pub scene_color: String,
    pub node_type: Arc<str>,
    pub x: f64,
    pub y: f64,
    pub tick: u64,
    pub seq: u64,
    pub writer: u32,
}

/// Nodes of every scene in one project, joined with their scene.
#[myko_view(NodeWithScene)]
pub struct GetNodesWithScene {
    pub project_id: Arc<str>,
}

impl ViewHandler for GetNodesWithScene {
    fn build_cell(
        ctx: ViewBuildArgs<Self>,
    ) -> impl MapQuery<Key = Arc<str>, Value = Arc<Self::Item>> {
        let scenes = ctx
            .view_context
            .query_map_by_str(GetBScenesByQuery(BSceneQuery {
                project_id: Some(StringFilter::Eq(ctx.view.project_id.clone())),
                ..Default::default()
            }));
        let nodes = ctx.view_context.query_map_by_str(GetAllBNodes {});
        scenes
            .inner_join_fk::<BNodeSceneIdRelation, _>(nodes)
            .map_entries(|(_scene_id, node_id), (scene, node)| {
                (
                    node_id.clone(),
                    Arc::new(NodeWithScene {
                        id: node_id.clone(),
                        scene_id: node.scene_id.clone(),
                        scene_name: scene.name.clone(),
                        scene_color: scene.color.clone(),
                        node_type: node.node_type.clone(),
                        x: node.x,
                        y: node.y,
                        tick: node.tick,
                        seq: node.seq,
                        writer: node.writer,
                    }),
                )
            })
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[myko_view_item]
pub struct Trio {
    pub id: Arc<str>,
    pub target_revision: u64,
    pub emitter_revision: u64,
    pub gauge_revision: u64,
}

/// Gauge joined to its emitter joined to that emitter's target. One mutation
/// bumps the revision of all three, so a row whose revisions differ shows a
/// partially applied mutation.
#[myko_view(Trio)]
pub struct GetTrios {}

impl ViewHandler for GetTrios {
    fn build_cell(
        ctx: ViewBuildArgs<Self>,
    ) -> impl MapQuery<Key = Arc<str>, Value = Arc<Self::Item>> {
        let gauges = ctx.view_context.query_map_by_str(GetAllBGauges {});
        let emitters = ctx.view_context.query_map_by_str(GetAllBEmitters {});
        let targets = ctx.view_context.query_map_by_str(GetAllBTargets {});
        gauges
            .inner_join_by(
                emitters,
                |_, gauge: &Arc<BGauge>| gauge.emitter_id.0.clone(),
                |emitter_id: &Arc<str>, _| emitter_id.clone(),
            )
            .inner_join_by(
                targets,
                |_, (_, emitter): &(Arc<BGauge>, Arc<BEmitter>)| emitter.target_id.0.clone(),
                |target_id: &Arc<str>, _| target_id.clone(),
            )
            .map_entries(|((gauge_id, _), _), ((gauge, emitter), target)| {
                (
                    gauge_id.clone(),
                    Arc::new(Trio {
                        id: gauge_id.clone(),
                        target_revision: target.revision,
                        emitter_revision: emitter.revision,
                        gauge_revision: gauge.revision,
                    }),
                )
            })
    }
}
