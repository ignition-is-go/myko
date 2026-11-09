//! Host-configured OAuth protected-resource discovery for MCP HTTP clients.
use crate::AuthError;
use serde_json::{Value, json};
use url::{Host, Url};

/// Discovery metadata only. The authenticator must validate resource-bound tokens.
#[derive(Clone, Debug)]
pub struct OAuthResource {
    resource: String,
    authorization_servers: Vec<String>,
    scopes: Vec<String>,
    metadata_url: String,
    metadata_path: String,
}
fn endpoint(value: &str) -> Result<Url, AuthError> {
    let url = Url::parse(value).map_err(|_| AuthError::Unavailable)?;
    let loopback = match url.host() {
        Some(Host::Domain("localhost")) => true,
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
        _ => false,
    };
    if value.len() > 4096
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
        || url.host().is_none()
        || !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(AuthError::Unavailable);
    }
    Ok(url)
}
impl OAuthResource {
    /// # Errors
    /// Rejects invalid URLs, non-loopback HTTP, or missing authorization servers.
    pub fn new(
        resource: impl Into<String>,
        authorization_servers: Vec<String>,
    ) -> Result<Self, AuthError> {
        let resource = resource.into();
        let mut url = endpoint(&resource)?;
        if authorization_servers.is_empty() || authorization_servers.len() > 16 {
            return Err(AuthError::Unavailable);
        }
        for issuer in &authorization_servers {
            endpoint(issuer)?;
        }
        let metadata_path = format!(
            "/.well-known/oauth-protected-resource{}",
            url.path().trim_end_matches('/')
        );
        url.set_path(&metadata_path);
        Ok(Self {
            resource,
            authorization_servers,
            scopes: Vec::new(),
            metadata_url: url.into(),
            metadata_path,
        })
    }
    /// # Errors
    /// Rejects invalid OAuth scope tokens or an oversized challenge.
    pub fn with_scopes(mut self, scopes: Vec<String>) -> Result<Self, AuthError> {
        if scopes.iter().any(|scope| {
            scope.is_empty()
                || !scope
                    .bytes()
                    .all(|b| b == 0x21 || (0x23..=0x5b).contains(&b) || (0x5d..=0x7e).contains(&b))
        }) || scopes
            .iter()
            .map(String::len)
            .sum::<usize>()
            .saturating_add(scopes.len())
            > 4096
        {
            return Err(AuthError::Unavailable);
        }
        self.scopes = scopes;
        Ok(self)
    }
    #[must_use]
    pub fn resource(&self) -> &str {
        &self.resource
    }
    #[must_use]
    pub fn authorization_servers(&self) -> &[String] {
        &self.authorization_servers
    }
    #[must_use]
    pub fn metadata_path(&self) -> &str {
        &self.metadata_path
    }
    #[must_use]
    pub fn metadata(&self) -> Value {
        let mut value =
            json!({"resource":self.resource, "authorization_servers":self.authorization_servers});
        if !self.scopes.is_empty()
            && let Some(object) = value.as_object_mut()
        {
            object.insert("scopes_supported".into(), json!(self.scopes));
        }
        value
    }
    #[must_use]
    pub fn challenge(&self) -> String {
        let mut header = format!("Bearer resource_metadata=\"{}\"", self.metadata_url);
        if !self.scopes.is_empty() {
            header.push_str(", scope=\"");
            header.push_str(&self.scopes.join(" "));
            header.push('"');
        }
        header
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[allow(
        clippy::panic_in_result_fn,
        reason = "assertions describe discovery contract failures"
    )]
    fn discovery_uses_resource_path_and_rejects_unsafe_configuration() -> Result<(), AuthError> {
        let resource = OAuthResource::new(
            "https://api.example/myko/mcp",
            vec!["https://login.example/".into()],
        )?
        .with_scopes(vec!["mcp:use".into()])?;
        assert_eq!(
            resource.metadata_path(),
            "/.well-known/oauth-protected-resource/myko/mcp"
        );
        assert_eq!(
            resource.metadata().get("resource"),
            Some(&json!("https://api.example/myko/mcp"))
        );
        assert_eq!(
            resource.challenge(),
            "Bearer resource_metadata=\"https://api.example/.well-known/oauth-protected-resource/myko/mcp\", scope=\"mcp:use\""
        );
        for url in [
            "http://remote.example/mcp",
            "https://u:p@example/mcp",
            "https://example/mcp?q=1",
            "https://example/mcp#x",
        ] {
            assert!(OAuthResource::new(url, vec!["https://login.example".into()]).is_err());
        }
        assert!(
            OAuthResource::new(
                "http://127.0.0.1:5155/myko/mcp",
                vec!["http://[::1]:8000/".into()]
            )
            .is_ok()
        );
        assert!(resource.with_scopes(vec!["bad\"\r\n".into()]).is_err());
        Ok(())
    }
}
