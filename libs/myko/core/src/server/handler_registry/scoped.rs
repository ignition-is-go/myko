use std::sync::Arc;

use crate::ServiceTypeId;

use super::AMap;

/// Global handlers have their own namespace, not a fallback into any service.
pub(super) struct ScopedHandlers<T> {
    global: AMap<Arc<str>, T>,
    services: AMap<Arc<str>, AMap<Arc<str>, T>>,
}

impl<T> Default for ScopedHandlers<T> {
    fn default() -> Self {
        Self {
            global: AMap::default(),
            services: AMap::default(),
        }
    }
}

impl<T> ScopedHandlers<T> {
    pub(super) fn insert(&mut self, service: Option<ServiceTypeId>, id: Arc<str>, value: T) {
        let handlers = match service {
            Some(service) => self.services.entry(service.into()).or_default(),
            None => &mut self.global,
        };
        handlers.insert(id, value);
    }

    pub(super) fn get(&self, service: Option<&str>, id: &str) -> Option<&T> {
        match service {
            Some(service) => self.services.get(service)?.get(id),
            None => self.global.get(id),
        }
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &T> {
        self.global.values().chain(
            self.services
                .values()
                .flat_map(|handlers| handlers.values()),
        )
    }

    pub(super) fn len(&self) -> usize {
        self.values().count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_and_owned_handlers_are_distinct_namespaces() {
        let mut handlers = ScopedHandlers::default();
        handlers.insert(None, "Same".into(), "global");
        handlers.insert(Some(ServiceTypeId::new("left")), "Same".into(), "left");
        handlers.insert(Some(ServiceTypeId::new("right")), "Same".into(), "right");
        assert_eq!(handlers.get(None, "Same"), Some(&"global"));
        assert_eq!(handlers.get(Some("left"), "Same"), Some(&"left"));
        assert_eq!(handlers.get(Some("right"), "Same"), Some(&"right"));
        assert_eq!(handlers.get(Some("missing"), "Same"), None);
        assert_eq!(handlers.len(), 3);
        assert_eq!(handlers.values().count(), 3);
    }
}
