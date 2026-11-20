use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::delegated::DelegatedOptions;
use crate::error::{ApiError, ErrorKind};
use crate::util::lock;

pub(crate) const DEFAULT_RATE_LIMIT: u64 = 100;

pub(crate) const DEFAULT_MAX_EPOCH_GAP: u64 = 1;

#[derive(Clone, serde::Serialize)]
pub(crate) struct RegistryConfig {
    pub(crate) epoch_size_sec: u64,
    pub(crate) max_epoch_gap: u64,
    #[serde(skip)]
    pub(crate) rate_limit: u64,
    #[serde(skip)]
    pub(crate) allocator: Option<DelegatedOptions>,
}

pub(crate) struct ModuleConfig {
    pub(crate) epoch_size_sec: u64,
    pub(crate) max_epoch_gap: u64,
    pub(crate) registries: Vec<String>,
    pub(crate) per_registry: std::collections::BTreeMap<String, RegistryConfig>,
}

static CONFIG: Mutex<Option<ModuleConfig>> = Mutex::new(None);

pub(crate) fn install(config: ModuleConfig) {
    *lock(&CONFIG) = Some(config);
}

pub(crate) fn registries_snapshot() -> (Vec<String>, BTreeMap<String, RegistryConfig>) {
    match lock(&CONFIG).as_ref() {
        Some(c) => (c.registries.clone(), c.per_registry.clone()),
        None => (Vec::new(), BTreeMap::new()),
    }
}

pub(crate) fn epoch_params_for(registry_canonical: &str) -> Result<(u64, u64), ApiError> {
    let cfg = lock(&CONFIG);
    let c = cfg.as_ref().ok_or_else(|| {
        ApiError::new(
            ErrorKind::NotReady,
            "start() has not configured epoch_size_sec; rate limiting is not ready",
        )
    })?;
    Ok(match c.per_registry.get(registry_canonical) {
        Some(r) => (r.epoch_size_sec, r.max_epoch_gap),
        None => (c.epoch_size_sec, c.max_epoch_gap),
    })
}

#[cfg(test)]
pub(crate) fn epoch_size() -> Result<u64, ApiError> {
    epoch_params_for("").map(|(size, _)| size)
}

pub(crate) fn epoch_binding_warning(
    hash: &str,
    registry: &str,
    bound: u64,
    configured: u64,
) -> String {
    format!(
        "membership start: entry {hash} on {registry} allocations are bound to \
         epoch_size_sec={bound}, configured size is {configured}; \
         generate_proof for it will fail permanent"
    )
}

pub(crate) fn allocator_for(registry_canonical: &str) -> Option<DelegatedOptions> {
    lock(&CONFIG)
        .as_ref()
        .and_then(|c| c.per_registry.get(registry_canonical))
        .and_then(|c| c.allocator.clone())
}

#[cfg(test)]
pub(crate) fn reset_config_for_test() {
    *lock(&CONFIG) = None;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_binding_warning_names_the_registry_size() {
        let msg = epoch_binding_warning("abc123", "logos:local:d", 300, 600);
        assert!(msg.contains("on logos:local:d"), "got: {msg}");
        assert!(msg.contains("epoch_size_sec=300"), "got: {msg}");
        assert!(msg.contains("configured size is 600"), "got: {msg}");
    }
}
