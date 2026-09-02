//! Apple Bonjour transport for Myko LAN discovery.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_dnssd::{BrowsedFlags, RegisterData, TxtRecord};
use futures_util::StreamExt;
use tokio::sync::watch;

use super::{
    LAN_DISCOVERY_PROTOCOL_VERSION, LanAdvertisement, LanDiscovery, LanPacket, LanRoster,
    MYKO_BONJOUR_SERVICE_TYPE, publish_roster,
};

const TXT_CHUNK_VALUE_BYTES: usize = 248;
const MAX_BONJOUR_PAYLOAD_BYTES: usize = 8 * 1024;
const TXT_VERSION_KEY: &[u8] = b"v";
const TXT_CHUNK_COUNT_KEY: &[u8] = b"n";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ServiceKey {
    interface: u32,
    name: String,
    reg_type: String,
    domain: String,
}

impl ServiceKey {
    fn from_result(result: &async_dnssd::BrowseResult) -> Self {
        Self {
            interface: result.interface.into_raw(),
            name: result.service_name.clone(),
            reg_type: result.reg_type.clone(),
            domain: result.domain.clone(),
        }
    }
}

pub fn start(
    advertisement: &LanAdvertisement,
    announce_interval: Duration,
    expiry: Duration,
) -> Result<LanDiscovery, String> {
    if announce_interval.is_zero() || expiry < announce_interval {
        return Err("LAN discovery timing is invalid".to_owned());
    }

    advertisement
        .descriptor
        .validate()
        .map_err(|error| format!("invalid Bonjour advertisement descriptor: {error}"))?;
    let txt = encode_advertisement(advertisement)?;
    let service_name = advertisement.descriptor.endpoint.id.to_string();
    let local_endpoint = advertisement.descriptor.endpoint.id;

    let roster = Arc::new(Mutex::new(LanRoster::default()));
    let (updates, _) = watch::channel(Vec::new());
    let (shutdown, mut shutdown_rx) = watch::channel(false);
    let task_roster = Arc::clone(&roster);
    let task_updates = updates.clone();
    let task = tokio::spawn(async move {
        let register_data = RegisterData {
            name: Some(&service_name),
            txt: txt.rdata(),
            ..RegisterData::default()
        };
        let Ok(registration) =
            async_dnssd::register_extended(MYKO_BONJOUR_SERVICE_TYPE, 0, register_data)
        else {
            return;
        };
        let Ok((_registration, _registered)) = registration.await else {
            return;
        };

        let mut browser = async_dnssd::browse(MYKO_BONJOUR_SERVICE_TYPE);
        let mut services = BTreeMap::<ServiceKey, String>::new();
        loop {
            tokio::select! {
                result = browser.next() => {
                    let Some(Ok(result)) = result else { break };
                    let key = ServiceKey::from_result(&result);
                    if result.flags.contains(BrowsedFlags::ADD) {
                        let mut resolution = result.resolve();
                        let Ok(Some(Ok(resolved))) = tokio::time::timeout(
                            Duration::from_secs(3),
                            resolution.next(),
                        ).await else { continue };
                        let Ok(advertisement) = decode_advertisement(&resolved.txt) else { continue };
                        if advertisement.descriptor.endpoint.id == local_endpoint
                            || advertisement.descriptor.validate().is_err()
                        {
                            continue;
                        }
                        let endpoint = advertisement.descriptor.endpoint.id.to_string();
                        let changed = task_roster.lock().is_ok_and(|mut roster| {
                            roster.observe(&advertisement, std::time::Instant::now())
                        });
                        services.insert(key, endpoint);
                        if changed {
                            publish_roster(&task_roster, &task_updates);
                        }
                    } else if let Some(endpoint) = services.remove(&key) {
                        let still_visible = services.values().any(|visible| visible == &endpoint);
                        let changed = !still_visible && task_roster.lock().is_ok_and(|mut roster| {
                            roster.remove_endpoint(&endpoint)
                        });
                        if changed {
                            publish_roster(&task_roster, &task_updates);
                        }
                    }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });

    Ok(LanDiscovery {
        roster,
        updates,
        shutdown,
        task: Mutex::new(Some(task)),
    })
}

fn encode_advertisement(advertisement: &LanAdvertisement) -> Result<TxtRecord, String> {
    let payload = serde_json::to_vec(&LanPacket {
        version: LAN_DISCOVERY_PROTOCOL_VERSION,
        advertisement: advertisement.clone(),
    })
    .map_err(|error| format!("could not encode Bonjour advertisement: {error}"))?;
    if payload.len() > MAX_BONJOUR_PAYLOAD_BYTES {
        return Err(format!(
            "Bonjour advertisement is too large: {} bytes",
            payload.len()
        ));
    }

    let chunks = payload.chunks(TXT_CHUNK_VALUE_BYTES).collect::<Vec<_>>();
    let mut txt = TxtRecord::with_capacity(payload.len().saturating_add(64));
    txt.set_value(
        TXT_VERSION_KEY,
        LAN_DISCOVERY_PROTOCOL_VERSION.to_string().as_bytes(),
    )
    .map_err(|error| format!("could not encode Bonjour TXT version: {error:?}"))?;
    txt.set_value(TXT_CHUNK_COUNT_KEY, chunks.len().to_string().as_bytes())
        .map_err(|error| format!("could not encode Bonjour TXT chunk count: {error:?}"))?;
    for (index, chunk) in chunks.into_iter().enumerate() {
        let key = format!("p{index:02}");
        txt.set_value(key.as_bytes(), chunk)
            .map_err(|error| format!("could not encode Bonjour TXT payload: {error:?}"))?;
    }
    Ok(txt)
}

fn decode_advertisement(encoded: &[u8]) -> Result<LanAdvertisement, String> {
    let txt =
        TxtRecord::parse(encoded).ok_or_else(|| "Bonjour TXT record is malformed".to_owned())?;
    let version = txt_value(&txt, TXT_VERSION_KEY)?;
    if version != LAN_DISCOVERY_PROTOCOL_VERSION.to_string().as_bytes() {
        return Err("Bonjour TXT protocol version is unsupported".to_owned());
    }
    let count = std::str::from_utf8(txt_value(&txt, TXT_CHUNK_COUNT_KEY)?)
        .map_err(|_| "Bonjour TXT chunk count is not UTF-8".to_owned())?
        .parse::<usize>()
        .map_err(|_| "Bonjour TXT chunk count is invalid".to_owned())?;
    if count == 0 || count > (MAX_BONJOUR_PAYLOAD_BYTES / TXT_CHUNK_VALUE_BYTES) + 1 {
        return Err("Bonjour TXT chunk count is out of range".to_owned());
    }

    let mut payload = Vec::new();
    for index in 0..count {
        let key = format!("p{index:02}");
        payload.extend_from_slice(txt_value(&txt, key.as_bytes())?);
        if payload.len() > MAX_BONJOUR_PAYLOAD_BYTES {
            return Err("Bonjour TXT payload is too large".to_owned());
        }
    }
    let packet = serde_json::from_slice::<LanPacket>(&payload)
        .map_err(|error| format!("Bonjour advertisement is invalid: {error}"))?;
    if packet.version != LAN_DISCOVERY_PROTOCOL_VERSION {
        return Err("Bonjour advertisement protocol version is unsupported".to_owned());
    }
    Ok(packet.advertisement)
}

fn txt_value<'a>(txt: &'a TxtRecord, key: &[u8]) -> Result<&'a [u8], String> {
    txt.get(key).flatten().ok_or_else(|| {
        format!(
            "Bonjour TXT field {} is missing",
            String::from_utf8_lossy(key)
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use myko_federation::NodeId;
    use myko_iroh::{EndpointAddr, NativeNodeDescriptor, SecretKey};

    fn advertisement() -> LanAdvertisement {
        let endpoint = SecretKey::generate().public();
        LanAdvertisement::full_node(
            NativeNodeDescriptor::new(NodeId::new(), EndpointAddr::new(endpoint)),
            "Bonjour test node",
        )
    }

    #[test]
    fn bonjour_txt_round_trips_binary_chunks() -> Result<(), String> {
        let advertisement = advertisement();
        let txt = encode_advertisement(&advertisement)?;
        let decoded = decode_advertisement(txt.rdata())?;
        if decoded != advertisement {
            return Err("Bonjour TXT round trip changed the advertisement".to_owned());
        }
        Ok(())
    }

    #[test]
    fn bonjour_txt_rejects_missing_chunks() -> Result<(), String> {
        let advertisement = advertisement();
        let mut txt = encode_advertisement(&advertisement)?;
        txt.remove(b"p00");
        if decode_advertisement(txt.rdata()).is_ok() {
            return Err("Bonjour TXT decoding accepted a missing chunk".to_owned());
        }
        Ok(())
    }
}
