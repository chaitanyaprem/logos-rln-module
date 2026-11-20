#![deny(unsafe_code)]

use std::sync::{Arc, Mutex};

mod config;
mod delegated;
mod ensure;
mod error;
mod keychain;
mod lifecycle;
mod membership_api;
mod nullifier_log;
mod panic_hook;
mod path_cache;
mod poller;
mod proof;
mod proof_api;
mod provider;
mod rate_limit;
mod register;
mod registry_id;
mod roots;
mod scope;
mod sealed_store;
mod select;
#[cfg(test)]
mod test_support;
mod tree;
mod util;
mod views;
mod wallet_home;
mod worker;

mod generated {
    #![allow(warnings)]
    #![allow(clippy::all)]
    #![allow(unsafe_code)]
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/generated/provider_gen.rs"
    ));
}
use generated::*;
pub(crate) use util::lock;

use config::{
    epoch_binding_warning, ModuleConfig, RegistryConfig, DEFAULT_MAX_EPOCH_GAP, DEFAULT_RATE_LIMIT,
};
use delegated::DelegatedOptions;
use error::{ApiError, ErrorKind};
use membership_api::{
    ensure_membership_impl, get_membership_state_impl, get_memberships_impl, select_membership_impl,
};
use proof_api::{
    generate_proof_impl, generate_proof_with, get_epoch_quota_impl, get_registry_parameters_impl,
    validate_proof_impl, validate_proof_with, SignalArg,
};
use register::register_impl;
use scope::{backend_of, parse_registry};
use sealed_store::store::Store;
use zeroize::Zeroize;

pub(crate) fn reply(result: Result<serde_json::Value, ApiError>) -> String {
    match result {
        Ok(value) => value.to_string(),
        Err(e) => e.to_json(),
    }
}

pub(crate) fn reply_result(
    result: Result<serde_json::Value, ApiError>,
) -> Result<serde_json::Value, String> {
    result.map_err(|e| e.body().to_string())
}

fn ok_json<T: serde::Serialize>(v: T) -> Result<serde_json::Value, ApiError> {
    serde_json::to_value(v).map_err(|e| ApiError::internal(format!("serialize reply: {e}")))
}

static LIFECYCLE: Mutex<()> = Mutex::new(());

fn start_impl(config_json: &str) -> Result<serde_json::Value, ApiError> {
    let _lifecycle = lock(&LIFECYCLE);
    panic_hook::install_once();
    let cfg: serde_json::Value = if config_json.trim().is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_str(config_json)
            .map_err(|e| ApiError::new(ErrorKind::InvalidArgument, format!("config_json: {e}")))?
    };

    let epoch_size_sec = cfg
        .get("epoch_size_sec")
        .and_then(|x| x.as_u64())
        .filter(|n| *n > 0)
        .ok_or_else(|| {
            ApiError::new(
                ErrorKind::InvalidArgument,
                "config_json.epoch_size_sec is required (a positive integer all \
                 validators of the application share)",
            )
        })?;
    let max_epoch_gap = cfg
        .get("max_epoch_gap")
        .and_then(|x| x.as_u64())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_MAX_EPOCH_GAP);

    let default_rate = cfg
        .get("rate_limit")
        .and_then(serde_json::Value::as_u64)
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_RATE_LIMIT);
    let default_allocator =
        DelegatedOptions::from_allocator_block(cfg.get("allocator"), "config_json.allocator")?;

    let (mut registries, mut per_registry) = config::registries_snapshot();
    let mut track = |raw: &str, set: Option<RegistryConfig>| {
        if let Ok(registry) = registry_id::parse(raw) {
            roots::track(&registry);
            match set {
                Some(c) => {
                    per_registry.insert(registry.canonical.clone(), c);
                }
                None if per_registry.contains_key(&registry.canonical) => {}
                None => {
                    per_registry.insert(
                        registry.canonical.clone(),
                        RegistryConfig {
                            epoch_size_sec,
                            max_epoch_gap,
                            rate_limit: default_rate,
                            allocator: default_allocator.clone(),
                        },
                    );
                }
            }
            if !registries.contains(&registry.canonical) {
                registries.push(registry.canonical);
            }
        }
    };
    if let Some(arr) = cfg.get("registries").and_then(|x| x.as_array()) {
        for entry in arr {
            match entry {
                serde_json::Value::String(raw) => track(raw, None),
                serde_json::Value::Object(o) => {
                    let raw = o
                        .get("registry_id")
                        .and_then(|x| x.as_str())
                        .unwrap_or_default();
                    let positive =
                        |key: &str| o.get(key).and_then(|x| x.as_u64()).filter(|n| *n > 0);
                    let allocator = match o.get("allocator") {
                        None => default_allocator.clone(),
                        Some(v) => DelegatedOptions::from_allocator_block(
                            Some(v),
                            &format!("registries[{raw}].allocator"),
                        )?,
                    };
                    let set = RegistryConfig {
                        epoch_size_sec: positive("epoch_size_sec").unwrap_or(epoch_size_sec),
                        max_epoch_gap: positive("max_epoch_gap").unwrap_or(max_epoch_gap),
                        rate_limit: positive("rate_limit").unwrap_or(default_rate),
                        allocator,
                    };
                    track(raw, Some(set));
                }
                _ => {}
            }
        }
    }

    let overrides_view = (!per_registry.is_empty())
        .then(|| serde_json::to_value(&per_registry).unwrap_or(serde_json::Value::Null));
    if let Some(store) = sealed_store::store::current() {
        for record in store.epoch_size_bindings() {
            let registry = &record.identity.registry_id;
            let parsed = registry_id::parse(registry);
            let canonical = parsed.as_ref().map_or(registry, |r| &r.canonical);
            let bound = record.alloc.epoch_size_sec;
            let configured = per_registry
                .get(canonical)
                .map(|c| c.epoch_size_sec)
                .unwrap_or(epoch_size_sec);
            if bound != 0 && bound != configured {
                eprintln!(
                    "{}",
                    epoch_binding_warning(&record.hash, canonical, bound, configured)
                );
            }
        }
    }
    let warm_roots = !registries.is_empty();
    let provision = cfg
        .get("provision")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    let provision_registries: Vec<ensure::Provision> = if provision {
        registries
            .iter()
            .map(|r| {
                let c = per_registry.get(r);
                ensure::Provision {
                    registry: r.clone(),
                    rate_limit: c.map(|c| c.rate_limit).unwrap_or(default_rate),
                    allocator: c.and_then(|c| c.allocator.clone()),
                }
            })
            .collect()
    } else {
        Vec::new()
    };
    let selection_order = registries.clone();
    let reply = ok_json(views::StartReply::new(
        epoch_size_sec,
        max_epoch_gap,
        overrides_view,
        &registries,
    ));
    config::install(ModuleConfig {
        epoch_size_sec,
        max_epoch_gap,
        registries,
        per_registry,
    });
    let ticket = ensure::supersede();
    worker::start(
        move || {
            provider::select_networks(&selection_order);
            if warm_roots {
                roots::refresh_all();
            }
            poller::refresh_paths();
            if !provision_registries.is_empty() {
                ensure::run_with(provision_registries, ticket);
            }
        },
        &[poller::ensure_running, roots::ensure_refresher],
    );

    reply
}

fn stop_impl() -> Result<serde_json::Value, ApiError> {
    let _lifecycle = lock(&LIFECYCLE);
    worker::stop();
    ensure::supersede();
    ok_json(views::StopReply::new())
}

#[derive(Default)]
struct RlnModuleImpl {}

impl RlnModuleImpl {
    fn store(&self) -> Result<Arc<Store>, ApiError> {
        sealed_store::store::current_or_cause()
    }
}

impl RlnModule for RlnModuleImpl {
    fn on_context_ready(&self, ctx: &RustModuleContext) {
        panic_hook::install_once();
        provider::init_client();
        if ctx.instance_persistence_path.is_empty() {
            eprintln!(
                "membership module: host provided no instance_persistence_path — keystore ops will fail"
            );
            sealed_store::store::close_current(
                "no instance persistence path from the host — keystore ops are \
                 disabled (no silent cwd fallback; see README)"
                    .to_string(),
            );
        } else {
            sealed_store::store::open_current(std::path::PathBuf::from(
                &ctx.instance_persistence_path,
            ));
            if let Some(store) = sealed_store::store::current() {
                if store.has_pending() {
                    poller::ensure_running();
                }
                keychain::lazy_auto_unlock();
            }
        }
    }

    fn unlock_keystore(&self, mut password: String) -> String {
        let result = self
            .store()
            .and_then(|s| s.unlock(&password))
            .and_then(|count| ok_json(views::UnlockKeystoreReply::new(count as u64)));
        password.zeroize();
        reply(result)
    }

    fn lock_keystore(&self) -> String {
        reply(self.store().map(|s| {
            s.lock();
            serde_json::json!({ "locked": true })
        }))
    }

    fn provision_wallet_home(&self, options_json: String) -> String {
        reply(wallet_home::provision_impl(&options_json))
    }

    fn unlock_keystore_auto(&self) -> String {
        reply(keychain::auto_unlock_impl())
    }

    fn remember_keystore_password(&self) -> String {
        reply(keychain::remember_impl())
    }

    fn register_membership(
        &self,
        registry_id: String,
        rln_identifier_hex: String,
        options_json: String,
    ) -> String {
        reply(register_impl(
            self.store(),
            &registry_id,
            &rln_identifier_hex,
            &options_json,
        ))
    }

    fn ensure_membership(&self, registry_id: String, options_json: String) -> String {
        reply(ensure_membership_impl(
            self.store(),
            &registry_id,
            &options_json,
        ))
    }

    fn get_membership_state(&self, registry_id: String, rln_identifier_hex: String) -> String {
        reply(get_membership_state_impl(
            self.store(),
            &registry_id,
            &rln_identifier_hex,
        ))
    }

    fn get_memberships(&self, registry_id: String) -> String {
        reply(get_memberships_impl(self.store(), &registry_id))
    }

    fn select_membership(
        &self,
        registry_id: String,
        rln_identifier_hex: String,
        selector_json: String,
    ) -> String {
        reply(select_membership_impl(
            self.store(),
            &registry_id,
            &rln_identifier_hex,
            &selector_json,
        ))
    }

    fn get_merkle_proof(&self, registry_id: String, leaf_index: i64) -> String {
        reply((|| {
            let registry = parse_registry(&registry_id)?;
            let backend = backend_of(&registry)?;
            if leaf_index < 0 {
                return Err(ApiError::new(
                    ErrorKind::InvalidArgument,
                    "leaf_index must be non-negative",
                ));
            }
            Ok(backend
                .tree
                .merkle_proof(&registry, leaf_index as u64)?
                .to_wire())
        })())
    }

    fn get_valid_roots(&self, registry_id: String) -> String {
        reply((|| {
            let registry = parse_registry(&registry_id)?;
            let backend = backend_of(&registry)?;
            let roots: Vec<String> = backend
                .tree
                .valid_roots(&registry)?
                .lifted()
                .iter()
                .map(|r| registry_id::bytes_to_hex(r))
                .collect();
            Ok(serde_json::json!({ "valid_roots": roots }))
        })())
    }

    fn generate_proof(
        &self,
        registry_id: String,
        rln_identifier_hex: String,
        signal_hex: String,
        timestamp: String,
    ) -> Result<serde_json::Value, String> {
        reply_result(generate_proof_impl(
            self.store(),
            &registry_id,
            &rln_identifier_hex,
            &signal_hex,
            &timestamp,
        ))
    }

    fn validate_proof(
        &self,
        registry_id: String,
        rln_identifier_hex: String,
        signal_hex: String,
        timestamp: String,
        proof_json: String,
    ) -> Result<serde_json::Value, String> {
        reply_result(validate_proof_impl(
            &registry_id,
            &rln_identifier_hex,
            &signal_hex,
            &timestamp,
            &proof_json,
        ))
    }

    fn generate_proof_prehashed(
        &self,
        registry_id: String,
        rln_identifier_hex: String,
        signal_hash_hex: String,
        timestamp: String,
    ) -> Result<serde_json::Value, String> {
        reply_result(generate_proof_with(
            self.store(),
            &registry_id,
            &rln_identifier_hex,
            SignalArg::Prehashed(&signal_hash_hex),
            &timestamp,
        ))
    }

    fn validate_proof_prehashed(
        &self,
        registry_id: String,
        rln_identifier_hex: String,
        signal_hash_hex: String,
        timestamp: String,
        proof_json: String,
    ) -> Result<serde_json::Value, String> {
        reply_result(validate_proof_with(
            &registry_id,
            &rln_identifier_hex,
            SignalArg::Prehashed(&signal_hash_hex),
            &timestamp,
            &proof_json,
        ))
    }

    fn get_epoch_quota(
        &self,
        registry_id: String,
        rln_identifier_hex: String,
        timestamp: String,
    ) -> Result<serde_json::Value, String> {
        reply_result(get_epoch_quota_impl(
            self.store(),
            &registry_id,
            &rln_identifier_hex,
            &timestamp,
        ))
    }

    fn get_registry_parameters(
        &self,
        registry_id: String,
        rln_identifier_hex: String,
    ) -> Result<serde_json::Value, String> {
        reply_result(get_registry_parameters_impl(
            &registry_id,
            &rln_identifier_hex,
        ))
    }

    fn start(&self, config_json: String) -> Result<serde_json::Value, String> {
        reply_result(start_impl(&config_json))
    }

    fn stop(&self) -> Result<serde_json::Value, String> {
        reply_result(stop_impl())
    }
}

#[allow(
    unsafe_code,
    reason = "the scaffold contract's one #[no_mangle] install symbol"
)]
#[no_mangle]
pub extern "Rust" fn logos_module_install() {
    install::<RlnModuleImpl>();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{allocator_for, epoch_params_for, reset_config_for_test};
    use crate::register::parse_registry_options;
    use crate::test_support::TEST_GLOBAL_LOCK;

    #[test]
    fn error_envelope_shape() {
        let e = ApiError::new(ErrorKind::UnknownRegistry, "no provider for namespace");
        assert_eq!(
            e.to_json(),
            r#"{"error":{"class":"permanent","kind":"unknown_registry","message":"no provider for namespace"}}"#
        );
        assert_eq!(ErrorKind::Locked.class(), "not_ready");
        assert_eq!(ErrorKind::NotReady.class(), "not_ready");
        assert_eq!(ErrorKind::ProviderFailure.class(), "transient");
        assert_eq!(ErrorKind::BudgetExhausted.class(), "budget_exhausted");
        assert_eq!(ErrorKind::InvalidArgument.class(), "permanent");
    }

    #[test]
    fn result_dialect_error_carries_typed_body() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let imp = RlnModuleImpl::default();
        let err = imp
            .validate_proof(
                "not-caip10".into(),
                "ef".repeat(32),
                "00".into(),
                "0".into(),
                "{}".into(),
            )
            .unwrap_err();
        assert_eq!(
            err,
            r#"{"class":"permanent","kind":"invalid_argument","message":"registry_id must be namespace:reference:account_address (CAIP-10), got 1 segment(s)"}"#
        );
        let ok = imp.stop().unwrap();
        assert_eq!(ok, serde_json::json!({ "stopped": true }));
        worker::reset_for_test();
    }

    #[test]
    fn start_configures_epoch_and_stop_tears_down() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let out = start_impl(r#"{"epoch_size_sec": 600, "max_epoch_gap": 3}"#).unwrap();
        assert_eq!(out["started"], serde_json::json!(true));
        assert_eq!(out["epoch_size_sec"], serde_json::json!(600));
        assert_eq!(out["max_epoch_gap"], serde_json::json!(3));
        assert_eq!(epoch_params_for("").unwrap(), (600, 3));
        assert!(!worker::is_stopped());

        let out = stop_impl().unwrap();
        assert_eq!(out["stopped"], serde_json::json!(true));
        assert!(worker::is_stopped());

        let out = start_impl(r#"{"epoch_size_sec": 600}"#).unwrap();
        assert!(!worker::is_stopped());
        assert_eq!(out["epoch_size_sec"], serde_json::json!(600));
        assert_eq!(
            out["max_epoch_gap"],
            serde_json::json!(DEFAULT_MAX_EPOCH_GAP)
        );
        let err = start_impl(r#"{"max_epoch_gap": 2}"#).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument);
        assert!(
            err.message.contains("epoch_size_sec"),
            "got: {}",
            err.message
        );
    }

    #[test]
    fn start_per_registry_overrides_select_epoch_config() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        reset_config_for_test();
        let reg_a = format!("logos:local:{}", "ab".repeat(32));
        let reg_b = format!("logos:local:{}", "cd".repeat(32));
        let cfg = serde_json::json!({
            "epoch_size_sec": 600,
            "max_epoch_gap": 2,
            "registries": [
                reg_a,
                {"registry_id": reg_b, "epoch_size_sec": 60, "max_epoch_gap": 5},
            ],
        })
        .to_string();
        let out = start_impl(&cfg).unwrap();
        assert_eq!(
            out["overrides"].to_string(),
            format!(
                r#"{{"{reg_a}":{{"epoch_size_sec":600,"max_epoch_gap":2}},"{reg_b}":{{"epoch_size_sec":60,"max_epoch_gap":5}}}}"#
            ),
            "got: {out}"
        );
        assert_eq!(out["registries"], serde_json::json!([reg_a, reg_b]));
        assert_eq!(epoch_params_for(&reg_a).unwrap(), (600, 2));
        assert_eq!(epoch_params_for(&reg_b).unwrap(), (60, 5));
        assert_eq!(epoch_params_for("logos:local:other").unwrap(), (600, 2));

        reset_config_for_test();
        let out = start_impl(r#"{"epoch_size_sec": 600}"#).unwrap();
        assert!(
            out.get("overrides").is_none(),
            "no overrides key when none are set: {out}"
        );
        reset_config_for_test();
    }

    #[test]
    fn start_keeps_registries_it_does_not_name() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        reset_config_for_test();
        let deliv = format!("logos:local:{}", "ab".repeat(32));
        let mix = format!("logos:local:{}", "cd".repeat(32));
        let host = serde_json::json!({
            "epoch_size_sec": 600,
            "registries": [deliv, {"registry_id": mix, "epoch_size_sec": 10, "max_epoch_gap": 3}],
        })
        .to_string();
        start_impl(&host).unwrap();
        assert_eq!(epoch_params_for(&mix).unwrap(), (10, 3));

        let out = start_impl(
            &serde_json::json!({"epoch_size_sec": 600, "registries": [deliv]}).to_string(),
        )
        .unwrap();
        assert_eq!(
            epoch_params_for(&mix).unwrap(),
            (10, 3),
            "the mix registry lost its epoch: {out}"
        );
        assert_eq!(
            epoch_params_for(&deliv).unwrap(),
            (600, DEFAULT_MAX_EPOCH_GAP)
        );
        assert_eq!(
            out["registries"],
            serde_json::json!([deliv, mix]),
            "every registry in force is reported"
        );

        start_impl(
            &serde_json::json!({"epoch_size_sec": 10, "max_epoch_gap": 3, "registries": [mix]})
                .to_string(),
        )
        .unwrap();
        assert_eq!(epoch_params_for(&mix).unwrap(), (10, 3));
        assert_eq!(
            epoch_params_for(&deliv).unwrap(),
            (600, DEFAULT_MAX_EPOCH_GAP)
        );
        assert_eq!(epoch_params_for("logos:local:other").unwrap(), (10, 3));

        start_impl(
            &serde_json::json!({"epoch_size_sec": 600, "registries": [{"registry_id": mix}]})
                .to_string(),
        )
        .unwrap();
        assert_eq!(
            epoch_params_for(&mix).unwrap(),
            (600, DEFAULT_MAX_EPOCH_GAP)
        );
        reset_config_for_test();
    }

    #[test]
    fn keystore_ops_without_store_report_internal() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let imp = RlnModuleImpl::default();
        let out = imp.lock_keystore();
        assert!(out.contains(r#""kind":"internal""#), "got: {out}");
    }

    #[test]
    fn start_configures_the_allocator_per_registry() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let (a, b, c) = (
            format!("logos:local:{}", "a7".repeat(32)),
            format!("logos:local:{}", "b7".repeat(32)),
            format!("logos:local:{}", "c7".repeat(32)),
        );
        let cfg = serde_json::json!({
            "epoch_size_sec": 600, "provision": false,
            "allocator": { "peer_id": "16Uiu2Peer", "multiaddr": "/ip4/127.0.0.1/tcp/1",
                           "auth_type": "peer-any", "auth_payload": "026400000000000000" },
            "registries": [
                a,
                { "registry_id": b, "epoch_size_sec": 10,
                  "allocator": { "peer_id": "16Uiu2Other", "multiaddr": "/ip4/127.0.0.1/tcp/2" } },
                { "registry_id": c, "allocator": null },
            ],
        });
        let out = start_impl(&cfg.to_string()).unwrap();
        assert_eq!(out["started"], serde_json::json!(true), "got: {out}");
        let canonical = |r: &str| registry_id::parse(r).unwrap().canonical;
        let a_alloc = allocator_for(&canonical(&a)).expect("a takes the global allocator");
        assert_eq!(a_alloc.allocator_peer_id, "16Uiu2Peer");
        assert_eq!(a_alloc.auth_type.as_deref(), Some("peer-any"));
        assert_eq!(a_alloc.auth_payload.as_deref(), Some("026400000000000000"));
        let b_alloc = allocator_for(&canonical(&b)).expect("b has its own");
        assert_eq!(b_alloc.allocator_peer_id, "16Uiu2Other");
        assert!(
            b_alloc.auth_type.is_none(),
            "an own block does not inherit the global auth"
        );
        assert!(allocator_for(&canonical(&c)).is_none(), "null opts out");

        let (rate, flat) = parse_registry_options(
            &serde_json::json!([
                { "key": "rate_limit", "value": "42" },
                { "key": "delegated", "value": "true" },
                { "key": "allocator_peer_id", "value": "16Uiu2Peer" },
                { "key": "allocator_multiaddr", "value": "/ip4/127.0.0.1/tcp/1" },
                { "key": "auth_type", "value": "peer-any" },
                { "key": "auth_payload", "value": "026400000000000000" },
            ])
            .to_string(),
        )
        .unwrap();
        assert_eq!(rate, 42);
        assert_eq!(
            DelegatedOptions::from_registry_options(flat).unwrap(),
            Some(a_alloc)
        );

        for (bad, needle) in [
            (
                serde_json::json!({ "multiaddr": "/ip4/1/tcp/1" }),
                "go together",
            ),
            (
                serde_json::json!({ "discover": false }),
                "needs discover or peer_id and multiaddr",
            ),
            (
                serde_json::json!({ "discover": "yes" }),
                "discover must be a boolean",
            ),
            (
                serde_json::json!({ "peer_id": "p", "multiaddr": "/m", "auth_payload": "00" }),
                "need auth_type",
            ),
            (
                serde_json::json!({ "peer_id": "p", "multiaddr": "/m", "auth_type": "t", "auth_payload": "aa", "auth_provider": "m" }),
                "mutually exclusive",
            ),
            (
                serde_json::json!({ "peer_id": "p", "multiaddr": "/m", "auth_type": "t", "auth_payload": "zz" }),
                "hex",
            ),
            (
                serde_json::json!({ "peer_id": 7, "multiaddr": "/m" }),
                "must be a string",
            ),
            (serde_json::json!("16Uiu2Peer"), "must be an object"),
        ] {
            let cfg = serde_json::json!({ "epoch_size_sec": 600, "provision": false, "allocator": bad, "registries": [a] });
            let e = start_impl(&cfg.to_string()).unwrap_err();
            assert_eq!(e.kind, ErrorKind::InvalidArgument, "{bad}: {}", e.message);
            assert!(e.message.contains(needle), "{bad}: {}", e.message);
            assert!(
                e.message.starts_with("config_json.allocator"),
                "{bad}: {}",
                e.message
            );
        }
        stop_impl().unwrap();
    }
}
