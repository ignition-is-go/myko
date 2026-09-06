use std::error::Error;

use myko_federation::{CommandState, EventEnvelope, NodeEvent};
use redb::{ReadOnlyDatabase, ReadableDatabase as _, ReadableTable as _, TableDefinition};

fn main() -> Result<(), Box<dyn Error>> {
    let path = std::env::args_os().nth(1).ok_or("expected journal path")?;
    let database = ReadOnlyDatabase::open(path)?;
    let read = database.begin_read()?;
    let events = read.open_table(TableDefinition::<u64, &[u8]>::new("myko_events"))?;
    let mut prepared = 0_u64;
    let mut mismatched = 0_u64;
    for entry in events.iter()? {
        let (_, encoded) = entry?;
        let envelope: EventEnvelope = serde_json::from_slice(encoded.value())?;
        if let NodeEvent::CommandLifecycle(command) = envelope.event
            && let CommandState::AuthorizationPrepared { effect } = command.state
        {
            prepared = prepared.saturating_add(1);
            if effect.validate_digest().is_err() {
                mismatched = mismatched.saturating_add(1);
            }
        }
    }
    println!("prepared={prepared} digest_mismatches={mismatched}");
    if mismatched != 0 {
        return Err("retained prepared effects require raw-byte recovery analysis".into());
    }
    Ok(())
}
