//! Transport-neutral DNS-SD TXT wire representation.

use std::collections::BTreeMap;

use super::{LAN_DISCOVERY_PROTOCOL_VERSION, LanAdvertisement, LanPacket};

const TXT_CHUNK_VALUE_BYTES: usize = 248;
const MAX_BONJOUR_PAYLOAD_BYTES: usize = 8 * 1024;
const TXT_VERSION_KEY: &str = "v";
const TXT_CHUNK_COUNT_KEY: &str = "n";
const FALLBACK_DNS_SD_PORT: u16 = 9;

#[derive(Debug, Clone, Default)]
pub struct BonjourTxt(BTreeMap<String, Vec<u8>>);

impl BonjourTxt {
    pub fn encode(advertisement: &LanAdvertisement) -> Result<Self, String> {
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
        let mut fields = Self::default();
        fields.insert(
            TXT_VERSION_KEY,
            LAN_DISCOVERY_PROTOCOL_VERSION.to_string().into_bytes(),
        );
        fields.insert(TXT_CHUNK_COUNT_KEY, chunks.len().to_string().into_bytes());
        for (index, chunk) in chunks.into_iter().enumerate() {
            fields.insert(format!("p{index:02}"), chunk.to_vec());
        }
        Ok(fields)
    }

    pub fn decode(&self) -> Result<LanAdvertisement, String> {
        let version = self.value(TXT_VERSION_KEY)?;
        if version != LAN_DISCOVERY_PROTOCOL_VERSION.to_string().as_bytes() {
            return Err("Bonjour TXT protocol version is unsupported".to_owned());
        }
        let count = std::str::from_utf8(self.value(TXT_CHUNK_COUNT_KEY)?)
            .map_err(|_| "Bonjour TXT chunk count is not UTF-8".to_owned())?
            .parse::<usize>()
            .map_err(|_| "Bonjour TXT chunk count is invalid".to_owned())?;
        if count == 0 || count > (MAX_BONJOUR_PAYLOAD_BYTES / TXT_CHUNK_VALUE_BYTES) + 1 {
            return Err("Bonjour TXT chunk count is out of range".to_owned());
        }

        let mut payload = Vec::new();
        for index in 0..count {
            payload.extend_from_slice(self.value(&format!("p{index:02}"))?);
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

    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) {
        self.0.insert(key.into(), value.into());
    }

    #[cfg(test)]
    fn remove(&mut self, key: &str) {
        self.0.remove(key);
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.0
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_slice()))
    }

    fn value(&self, key: &str) -> Result<&[u8], String> {
        self.0
            .get(key)
            .map(Vec::as_slice)
            .ok_or_else(|| format!("Bonjour TXT field {key} is missing"))
    }
}

/// Returns a meaningful SRV port when the Iroh descriptor has one.
///
/// Port 9 keeps address-less placeholder services resolvable by Apple's
/// DNS-SD stack; Myko connections always use the authenticated descriptor.
pub fn service_port(advertisement: &LanAdvertisement) -> u16 {
    advertisement
        .descriptor
        .endpoint
        .ip_addrs()
        .next()
        .map_or(FALLBACK_DNS_SD_PORT, std::net::SocketAddr::port)
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
        let decoded = BonjourTxt::encode(&advertisement)?.decode()?;
        if decoded != advertisement {
            return Err("Bonjour TXT round trip changed the advertisement".to_owned());
        }
        Ok(())
    }

    #[test]
    fn bonjour_txt_rejects_missing_chunks() -> Result<(), String> {
        let mut txt = BonjourTxt::encode(&advertisement())?;
        txt.remove("p00");
        if txt.decode().is_ok() {
            return Err("Bonjour TXT decoding accepted a missing chunk".to_owned());
        }
        Ok(())
    }
}
