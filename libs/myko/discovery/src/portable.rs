//! Portable mDNS/DNS-SD transport for non-Apple platforms.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo, TxtProperty};
use tokio::sync::watch;

use super::{
    LanAdvertisement, LanDiscovery, LanRoster, MYKO_BONJOUR_SERVICE_TYPE,
    bonjour::{BonjourTxt, service_port},
    publish_roster,
};

#[cfg_attr(
    any(target_os = "ios", target_os = "macos"),
    allow(
        dead_code,
        reason = "compiled on Apple only to type-check the portable backend in tests"
    )
)]
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
        .map_err(|error| format!("invalid DNS-SD advertisement descriptor: {error}"))?;

    let service_type = format!("{MYKO_BONJOUR_SERVICE_TYPE}.local.");
    let service_name = advertisement.descriptor.node_id.to_string();
    let host_name = format!("{service_name}.local.");
    let local_endpoint = advertisement.descriptor.endpoint.id;
    let properties = BonjourTxt::encode(advertisement)?
        .iter()
        .map(|(key, value)| TxtProperty::from((key, value)))
        .collect::<Vec<_>>();

    let daemon =
        ServiceDaemon::new().map_err(|error| format!("could not start DNS-SD daemon: {error}"))?;
    let service = ServiceInfo::new(
        &service_type,
        &service_name,
        &host_name,
        (),
        service_port(advertisement),
        properties,
    )
    .map_err(|error| format!("could not create DNS-SD service: {error}"))?
    .enable_addr_auto();
    let fullname = service.get_fullname().to_owned();
    daemon
        .register(service)
        .map_err(|error| format!("could not register DNS-SD service: {error}"))?;
    let browser = daemon
        .browse(&service_type)
        .map_err(|error| format!("could not browse DNS-SD services: {error}"))?;
    tracing::debug!(
        node_id = %advertisement.descriptor.node_id,
        endpoint_id = %local_endpoint,
        service_type,
        "portable DNS-SD advertisement and browser started"
    );

    let roster = Arc::new(Mutex::new(LanRoster::default()));
    let (updates, _) = watch::channel(Vec::new());
    let (shutdown, mut shutdown_rx) = watch::channel(false);
    let task_roster = Arc::clone(&roster);
    let task_updates = updates.clone();
    let task = tokio::spawn(async move {
        let mut services = BTreeMap::<String, String>::new();
        loop {
            tokio::select! {
                event = browser.recv_async() => {
                    let Ok(event) = event else { break };
                    observe_event(
                        event,
                        &task_roster,
                        &task_updates,
                        &mut services,
                        local_endpoint,
                    );
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
            }
        }

        let _stopped = daemon.stop_browse(&service_type);
        if let Ok(unregistered) = daemon.unregister(&fullname) {
            let _status =
                tokio::time::timeout(Duration::from_secs(1), unregistered.recv_async()).await;
        }
        if let Ok(stopped) = daemon.shutdown() {
            let _status = tokio::time::timeout(Duration::from_secs(1), stopped.recv_async()).await;
        }
        tracing::debug!("portable DNS-SD driver stopped");
    });

    Ok(LanDiscovery {
        roster,
        updates,
        shutdown,
        task: Mutex::new(Some(task)),
    })
}

fn observe_event(
    event: ServiceEvent,
    roster: &Mutex<LanRoster>,
    updates: &watch::Sender<Vec<super::DiscoveredNode>>,
    services: &mut BTreeMap<String, String>,
    local_endpoint: myko_iroh::EndpointId,
) {
    match event {
        ServiceEvent::ServiceResolved(service) => {
            let mut fields = BonjourTxt::default();
            for property in service.get_properties().iter() {
                let Some(value) = property.val() else {
                    continue;
                };
                fields.insert(property.key(), value);
            }
            let Ok(advertisement) = fields.decode() else {
                tracing::trace!(
                    fullname = service.get_fullname(),
                    "ignored invalid DNS-SD advertisement"
                );
                return;
            };
            if advertisement.descriptor.endpoint.id == local_endpoint
                || advertisement.descriptor.validate().is_err()
            {
                return;
            }
            let endpoint = advertisement.descriptor.endpoint.id.to_string();
            let changed = roster
                .lock()
                .is_ok_and(|mut roster| roster.observe(&advertisement, Instant::now()));
            services.insert(service.get_fullname().to_owned(), endpoint);
            if changed {
                tracing::debug!(
                    node_id = %advertisement.descriptor.node_id,
                    endpoint_id = %advertisement.descriptor.endpoint.id,
                    display_name = %advertisement.display_name,
                    "discovered LAN node"
                );
                publish_roster(roster, updates);
            }
        }
        ServiceEvent::ServiceRemoved(_, fullname) => {
            let Some(endpoint) = services.remove(&fullname) else {
                return;
            };
            let still_visible = services.values().any(|visible| visible == &endpoint);
            let changed = !still_visible
                && roster
                    .lock()
                    .is_ok_and(|mut roster| roster.remove_endpoint(&endpoint));
            if changed {
                tracing::debug!(endpoint_id = %endpoint, "LAN node advertisement disappeared");
                publish_roster(roster, updates);
            }
        }
        _ => {}
    }
}
