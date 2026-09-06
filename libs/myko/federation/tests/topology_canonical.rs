use myko_federation::ScopeTopology;

#[test]
fn topology_bytes_are_canonical_across_insertion_order_and_replay()
-> Result<(), Box<dyn std::error::Error>> {
    let names = (0..32)
        .map(|index| format!("scope-{index:02}"))
        .collect::<Vec<_>>();
    let parents = names
        .iter()
        .skip(1)
        .map(|name| format!("\"{name}\":\"scope-00\""))
        .collect::<Vec<_>>();
    let canonical = format!(
        "{{\"parents\":{{{}}},\"known\":{}}}",
        parents.join(","),
        serde_json::to_string(&names)?
    );
    let reversed = format!(
        "{{\"parents\":{{{}}},\"known\":{}}}",
        parents.into_iter().rev().collect::<Vec<_>>().join(","),
        serde_json::to_string(&names.into_iter().rev().collect::<Vec<_>>())?
    );
    for input in [&canonical, &reversed] {
        let topology: ScopeTopology = serde_json::from_str(input)?;
        let encoded = serde_json::to_string(&topology)?;
        if encoded != canonical {
            return Err(format!("topology serialization is not canonical: {encoded}").into());
        }
        let replayed: ScopeTopology = serde_json::from_str(&encoded)?;
        if serde_json::to_string(&replayed)? != encoded || replayed != topology {
            return Err("topology bytes or meaning changed during replay".into());
        }
    }
    Ok(())
}
