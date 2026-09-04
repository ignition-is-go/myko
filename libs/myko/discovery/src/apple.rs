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
    LanAdvertisement, LanDiscovery, LanRoster, MYKO_BONJOUR_SERVICE_TYPE,
    bonjour::{BonjourTxt, service_port},
    publish_roster,
};

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
    let fields = BonjourTxt::encode(advertisement)?;
    let mut txt = TxtRecord::new();
    for (key, value) in fields.iter() {
        txt.set_value(key.as_bytes(), value)
            .map_err(|error| format!("could not encode Bonjour TXT field: {error:?}"))?;
    }
    let service_name = advertisement.descriptor.node_id.to_string();
    let port = service_port(advertisement);
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
            async_dnssd::register_extended(MYKO_BONJOUR_SERVICE_TYPE, port, register_data)
        else {
            tracing::error!("could not register Apple Bonjour advertisement");
            return;
        };
        let Ok((_registration, _registered)) = registration.await else {
            tracing::error!("Apple Bonjour advertisement registration failed");
            return;
        };

        tracing::debug!(
            %service_name,
            endpoint_id = %local_endpoint,
            service_type = MYKO_BONJOUR_SERVICE_TYPE,
            "Apple Bonjour advertisement and browser started"
        );

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
                        ).await else {
                            tracing::trace!(service = %result.service_name, "Bonjour service resolution failed or timed out");
                            continue;
                        };
                        let Some(txt) = TxtRecord::parse(&resolved.txt) else { continue };
                        let mut fields = BonjourTxt::default();
                        for (key, value) in &txt {
                            let Some(value) = value else { continue };
                            fields.insert(String::from_utf8_lossy(key), value);
                        }
                        let Ok(advertisement) = fields.decode() else { continue };
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
                            tracing::debug!(
                                node_id = %advertisement.descriptor.node_id,
                                endpoint_id = %advertisement.descriptor.endpoint.id,
                                display_name = %advertisement.display_name,
                                "discovered LAN node"
                            );
                            publish_roster(&task_roster, &task_updates);
                        }
                    } else if let Some(endpoint) = services.remove(&key) {
                        let still_visible = services.values().any(|visible| visible == &endpoint);
                        let changed = !still_visible && task_roster.lock().is_ok_and(|mut roster| {
                            roster.remove_endpoint(&endpoint)
                        });
                        if changed {
                            tracing::debug!(endpoint_id = %endpoint, "LAN node advertisement disappeared");
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
        tracing::debug!("Apple Bonjour driver stopped");
    });

    Ok(LanDiscovery {
        roster,
        updates,
        shutdown,
        task: Mutex::new(Some(task)),
    })
}
