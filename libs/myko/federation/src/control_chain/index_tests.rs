use super::*;
use crate::{
    LogPosition, NodeId,
    control_quorum::{ControlVote, HEAD_HASHES},
};

#[test]
fn adjacent_accepts_reuse_only_an_identical_slot_and_value_hash() -> Result<(), String> {
    let slot = ControlSlot {
        realm: ScopeId::new("index-hashes"),
        epoch: ControlEpochId([1; 32]),
        predecessor: ControlHead([2; 32]),
    };
    let successor_slot = ControlSlot {
        epoch: ControlEpochId([3; 32]),
        ..slot.clone()
    };
    let value = ControlValue(vec![4; 4096]);
    let changed = ControlValue(vec![5; 4096]);
    let cases = [
        (slot.clone(), value),
        (slot.clone(), changed.clone()),
        (successor_slot, changed),
    ];
    let node = NodeId::new();
    let mut history = Vec::new();
    let mut expected = Vec::new();
    let ballot = ControlBallot {
        counter: 1,
        proposer: ControllerId([6; 32]),
    };
    for (slot, value) in cases {
        expected.push(slot.head_for(&value).map_err(|error| error.to_string())?);
        for controller in [7, 8] {
            let position = LogPosition::new(
                u64::try_from(history.len())
                    .map_err(|error| error.to_string())?
                    .saturating_add(1),
            );
            history.push(EventEnvelope {
                position,
                origin: EventId::new(node, position),
                recorded_at: chrono::Utc::now(),
                event: NodeEvent::FrameworkControl(FrameworkControlEvent::ControlVote(
                    SignedControlVote {
                        message: ControlVote {
                            slot: slot.clone(),
                            ballot,
                            controller: ControllerId([controller; 32]),
                            vote: ControlVoteKind::Accept {
                                value: value.clone(),
                            },
                        },
                        signature: [0; 64],
                    },
                )),
            });
        }
    }
    for _ in 0..2 {
        let before = HEAD_HASHES.with(std::cell::Cell::get);
        let evidence = ControlEvidence::index(&history, &slot.realm)?;
        let hashes = HEAD_HASHES
            .with(std::cell::Cell::get)
            .saturating_sub(before);
        if hashes != 3 {
            return Err(format!(
                "identical accept payloads were hashed again: {hashes} hashes instead of 3"
            ));
        }
        if evidence.accepts.len() != 3 {
            return Err("different slots or values shared an accept bucket".to_owned());
        }
        for head in &expected {
            if evidence.accepts.get(&(head.0, ballot)).map(Vec::len) != Some(2) {
                return Err("accept bucket lost a controller's vote".to_owned());
            }
        }
    }
    Ok(())
}
