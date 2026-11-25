use std::sync::{Arc, Mutex, Weak};

use crate::config::{epoch_params_for, DEFAULT_RATE_LIMIT};
use crate::delegated::{delegated_request_args, DelegatedOptions};
use crate::error::{ApiError, ErrorKind};
use crate::lifecycle::{self, note_granted_by, CacheState, MembershipState, StoredCredential};
use crate::poller;
use crate::proof;
use crate::proof_api::proof_error;
use crate::provider;
use crate::registry_id;
use crate::scope::{backend_of, parse_scope, records_for_registry, scope_matches};
use crate::sealed_store::{self, store::Store};
use crate::util::{lock, now_unix};
use crate::views;

pub(crate) fn parse_registry_options(
    options_json: &str,
) -> Result<(u64, serde_json::Value), ApiError> {
    let trimmed = options_json.trim();
    let entries = if trimmed.is_empty() {
        Vec::new()
    } else {
        match serde_json::from_str::<serde_json::Value>(trimmed) {
            Ok(serde_json::Value::Array(a)) => a,
            Ok(_) => return Err(ApiError::new(
                ErrorKind::InvalidArgument,
                "options_json must be a RegistryOptions array of {\"key\",\"value\"} string pairs",
            )),
            Err(e) => {
                return Err(ApiError::new(
                    ErrorKind::InvalidArgument,
                    format!("options_json: {e}"),
                ))
            }
        }
    };
    let mut map = serde_json::Map::new();
    for mut entry in entries {
        let key = match entry.get_mut("key").map(serde_json::Value::take) {
            Some(serde_json::Value::String(k)) if !k.is_empty() => k,
            _ => {
                return Err(ApiError::new(
                    ErrorKind::InvalidArgument,
                    "every RegistryOptions entry needs a string \"key\"",
                ))
            }
        };
        let value = match entry.get_mut("value").map(serde_json::Value::take) {
            Some(serde_json::Value::String(v)) => v,
            _ => {
                return Err(ApiError::new(
                    ErrorKind::InvalidArgument,
                    format!("RegistryOptions value for '{key}' must be a string (char* pair)"),
                ))
            }
        };
        if map.contains_key(&key) {
            return Err(ApiError::new(
                ErrorKind::InvalidArgument,
                format!("duplicate RegistryOptions key '{key}'"),
            ));
        }
        map.insert(key, serde_json::Value::String(value));
    }
    let raw = map.remove("rate_limit");
    let given = raw
        .as_ref()
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let rate_limit = match given {
        None => DEFAULT_RATE_LIMIT,
        Some(s) => s.parse::<u64>().ok().filter(|r| *r > 0).ok_or_else(|| {
            ApiError::new(
                ErrorKind::InvalidArgument,
                "rate_limit option must be a positive decimal string",
            )
        })?,
    };
    Ok((rate_limit, serde_json::Value::Object(map)))
}

pub(crate) fn is_retryable_submit_error(e: &ApiError) -> bool {
    matches!(
        e.kind,
        ErrorKind::Transient | ErrorKind::NotReady | ErrorKind::ProviderFailure
    )
}

pub(crate) const ALLOCATION_FAILED: &str = "allocation_failed";

#[derive(Debug, PartialEq, Eq)]
enum AllocationFailure {
    Unconfirmed(String),
    DiscoveryMiss(String),
    Failed(String),
}

impl AllocationFailure {
    fn of(reply: &mut serde_json::Value) -> Option<Self> {
        reply.get("kind")?;
        let error = reply.get_mut("error").map(serde_json::Value::take);
        let kind = reply
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let error = match error {
            Some(serde_json::Value::String(s)) => s,
            Some(other) => other.to_string(),
            None => kind.to_owned(),
        };
        let discovery = reply.get("discovery").and_then(serde_json::Value::as_str);
        Some(match (kind, discovery) {
            ("unconfirmed", _) => Self::Unconfirmed(error),
            ("no_provider", Some("not_joined" | "none_advertising")) => Self::DiscoveryMiss(error),
            _ => Self::Failed(error),
        })
    }
}

fn fail_pending(m: &mut CacheState, error: &str) {
    if m.state == MembershipState::Pending {
        m.state = MembershipState::Failed;
        m.failed_reason = Some(format!("{ALLOCATION_FAILED}: {error}"));
    }
}

pub(crate) fn register_refusal(reply: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(reply)
        .ok()?
        .get("error")?
        .as_str()
        .map(str::to_owned)
}

pub(crate) fn funded_submit_callback(
    store: Weak<Store>,
    hash: String,
) -> provider::RegisterCallback {
    Box::new(move |result| {
        let update = match store.upgrade() {
            Some(store) => store.update_cache(&hash, |m| match result {
                Ok(reply) => match register_refusal(&reply) {
                    Some(why) => m.record_async_submit_error(&why, false),
                    None if m.state != MembershipState::Pending => {}
                    None => {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&reply) {
                            if let Some(leaf) = v.get("leaf_index").and_then(|x| x.as_u64()) {
                                m.leaf_index = Some(leaf);
                            }
                        }
                        m.tx_result = Some(reply);
                    }
                },
                Err(e) => m.record_async_submit_error(&e.message, is_retryable_submit_error(&e)),
            }),
            None => Err(ApiError::internal(
                "store closed before the submit callback landed",
            )),
        };
        if let Err(e) = update {
            eprintln!("membership register callback: {}", e.message);
        }
    })
}

pub(crate) const WINDOW_ELAPSED: &str = "confirmation_window_elapsed";

pub(crate) fn delegated_confirm_deadline(submitted_at: u64) -> u64 {
    submitted_at
        + provider::ALLOCATION_REQUEST_TIMEOUT.as_secs()
        + lifecycle::CONFIRMATION_WINDOW_SECS
}

pub(crate) fn delegated_submit_callback(
    store: Weak<Store>,
    hash: String,
    registry_account: String,
) -> provider::RegisterCallback {
    Box::new(move |result| {
        let Some(store) = store.upgrade() else {
            eprintln!(
                "membership delegated callback: store closed before the submit callback landed"
            );
            return;
        };
        let before = store.membership(&hash);
        let mut reopened = false;
        let update = store.update_cache(&hash, |m| match result {
            Ok(reply) => match serde_json::from_str::<serde_json::Value>(&reply) {
                Ok(mut v) => {
                    let failure = AllocationFailure::of(&mut v).or_else(|| {
                        v.get("config_account")
                            .and_then(|x| x.as_str())
                            .filter(|got| !got.eq_ignore_ascii_case(&registry_account))
                            .map(|got| {
                                AllocationFailure::Failed(format!(
                                    "registered on {got}, not {registry_account}"
                                ))
                            })
                    });
                    match failure {
                        Some(AllocationFailure::Unconfirmed(msg)) => {
                            note_granted_by(&hash, &v);
                            eprintln!(
                                "membership delegated: {msg}; {hash} stays pending until the \
                                 registry shows it or its confirmation window ends"
                            );
                            return;
                        }
                        Some(AllocationFailure::DiscoveryMiss(msg)) => {
                            if m.state == MembershipState::Pending {
                                lifecycle::note_discovery_miss(&hash);
                            }
                            fail_pending(m, &msg);
                            return;
                        }
                        Some(AllocationFailure::Failed(msg)) => {
                            fail_pending(m, &msg);
                            return;
                        }
                        None => {}
                    }
                    note_granted_by(&hash, &v);
                    match m.state {
                        MembershipState::Pending => {}
                        MembershipState::Failed
                            if m.failed_reason.as_deref() == Some(WINDOW_ELAPSED) =>
                        {
                            m.state = MembershipState::Pending;
                            m.failed_reason = None;
                            m.retryable = None;
                            reopened = true;
                        }
                        _ => return,
                    }
                    if let Some(leaf) = v.get("leaf_index").and_then(|x| x.as_u64()) {
                        m.leaf_index = Some(leaf);
                    }
                    let tx = v.get("tx_hash").and_then(|x| x.as_str()).unwrap_or("");
                    let inner = serde_json::json!(
                        {"error": "", "secrets": [], "success": true, "tx_hash": tx})
                    .to_string();
                    m.tx_result = Some(
                        serde_json::json!(
                            {"leaf_index": m.leaf_index.unwrap_or(0), "pending": true, "tx_result": inner})
                        .to_string(),
                    );
                }
                Err(parse) => {
                    eprintln!(
                        "membership delegated callback: unparseable allocation reply ({parse}): {reply}"
                    );
                    m.tx_result = Some(reply);
                }
            },
            Err(e) => m.record_async_submit_error(&e.message, is_retryable_submit_error(&e)),
        });
        let prior = match update {
            Ok(prior) => prior,
            Err(e) => {
                eprintln!("membership delegated callback: {}", e.message);
                return;
            }
        };
        if reopened {
            poller::ensure_running();
        }
        if let (Some(before), Some(after)) = (before, store.membership(&hash)) {
            lifecycle::emit_transition(&hash, &before, prior, after.cache.state);
        }
    })
}

pub(crate) static REGISTER_IN_FLIGHT: Mutex<Option<std::collections::HashSet<(String, String)>>> =
    Mutex::new(None);

pub(crate) struct RegisterClaim((String, String));

impl RegisterClaim {
    pub(crate) fn take(registry: &str, rln_id_hex: &str) -> Result<RegisterClaim, ApiError> {
        let mut set = lock(&REGISTER_IN_FLIGHT);
        let set = set.get_or_insert_with(std::collections::HashSet::new);
        let key = (registry.to_string(), rln_id_hex.to_string());
        if set.contains(&key) {
            return Err(ApiError::new(
                ErrorKind::ProviderFailure,
                "a registration for this scope is already in flight — retry to observe its result",
            ));
        }
        set.insert(key.clone());
        Ok(RegisterClaim(key))
    }
}

impl Drop for RegisterClaim {
    fn drop(&mut self) {
        if let Some(set) = lock(&REGISTER_IN_FLIGHT).as_mut() {
            set.remove(&self.0);
        }
    }
}

pub(crate) fn register_impl(
    store: Result<Arc<Store>, ApiError>,
    registry_id_raw: &str,
    rln_identifier_hex: &str,
    options_json: &str,
) -> Result<serde_json::Value, ApiError> {
    let (registry, _, rln_id_hex) = parse_scope(registry_id_raw, rln_identifier_hex)?;
    let (rate_limit, opts) = parse_registry_options(options_json)?;
    let delegated = DelegatedOptions::from_registry_options(opts)?.map(|d| d.on_network(&registry));
    register_scoped(
        store,
        &registry,
        &rln_id_hex,
        rate_limit,
        delegated.as_ref(),
    )
}

pub(crate) fn register_scoped(
    store: Result<Arc<Store>, ApiError>,
    registry: &registry_id::CanonicalRegistryId,
    rln_id_hex: &str,
    rate_limit: u64,
    delegated: Option<&DelegatedOptions>,
) -> Result<serde_json::Value, ApiError> {
    let backend = backend_of(registry)?;
    let _claim = RegisterClaim::take(&registry.canonical, rln_id_hex)?;

    let store = store?;
    let records = records_for_registry(&store, registry);
    if let Some(rec) = records
        .iter()
        .find(|r| !r.quarantined && r.cache.state.is_live() && scope_matches(r, rln_id_hex))
    {
        let mismatch = rec.cache.rate_limit != Some(rate_limit);
        let bound = rec.alloc.epoch_size_sec;
        let epoch_mismatch = epoch_params_for(&registry.canonical)
            .is_ok_and(|(size, _)| bound != 0 && bound != size);
        return Ok(views::public_membership(
            &rec.hash,
            rec,
            false,
            mismatch,
            epoch_mismatch,
        ));
    }

    match backend.registry.get_registry_bounds(registry) {
        Ok(bounds) => {
            let max = bounds.get("max_rate_limit").and_then(|x| x.as_u64());
            let min = bounds.get("min_rate_limit").and_then(|x| x.as_u64());
            if max.is_some_and(|m| rate_limit > m) || min.is_some_and(|m| rate_limit < m) {
                return Err(ApiError::new(
                    ErrorKind::InvalidArgument,
                    format!(
                        "rate_limit {rate_limit} outside registry bounds [{}, {}]",
                        min.map(|m| m.to_string()).unwrap_or_else(|| "?".into()),
                        max.map(|m| m.to_string()).unwrap_or_else(|| "?".into()),
                    ),
                ));
            }
        }
        Err(e) => eprintln!("register: bounds pre-check skipped: {}", e.message),
    }

    let (commitment_hex, secret_hash_hex) = proof::generate_identity().map_err(proof_error)?;
    let credential = StoredCredential {
        identity_commitment: commitment_hex.clone(),
        identity_secret_hash: secret_hash_hex,
        registry_id: registry.canonical.clone(),
    };
    let commitment = registry_id::hex_to_bytes32(&commitment_hex)
        .ok_or_else(|| ApiError::internal("generated commitment is not 32 bytes"))?;
    let hash = registry_id::membership_hash(&registry.canonical, &commitment);

    let identity = sealed_store::format::IdentityBlock {
        registry_id: registry.canonical.clone(),
        rln_identifier: rln_id_hex.to_string(),
        identity_commitment: commitment_hex,
        submitted_at: now_unix(),
    };
    store.insert(&hash, identity, &credential, rate_limit)?;
    if delegated.is_some() {
        let deadline = delegated_confirm_deadline(now_unix());
        store.update_cache(&hash, |m| m.confirm_deadline = Some(deadline))?;
    }

    let submit = match delegated {
        Some(d) => provider::allocation_request_async(
            &delegated_request_args(
                d,
                &credential.identity_commitment,
                rate_limit,
                &registry.account,
            ),
            delegated_submit_callback(
                Arc::downgrade(&store),
                hash.clone(),
                registry.account.clone(),
            ),
        ),
        None => backend.registry.register_async(
            registry,
            &credential.identity_commitment,
            rate_limit,
            funded_submit_callback(Arc::downgrade(&store), hash.clone()),
        ),
    };
    if let Err(e) = submit {
        let retryable = is_retryable_submit_error(&e);
        store.update_cache(&hash, |m| m.mark_submit_failed(&e.message, retryable))?;
        return Err(e);
    }
    poller::ensure_running();

    let record = store
        .membership(&hash)
        .ok_or_else(|| ApiError::internal("record vanished after insert"))?;
    Ok(views::public_membership(
        &hash, &record, false, false, false,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::RlnModule;
    use crate::test_support::{imp_with_store, opts_arr, seed_membership, TEST_GLOBAL_LOCK};
    use crate::RlnModuleImpl;

    #[test]
    fn an_unconfirmed_allocation_stays_pending_for_the_poller() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir = std::env::temp_dir().join(format!("rln-unconfirmed-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (_imp, store) = imp_with_store(dir.clone());
        store.unlock("pw").unwrap();
        let registry = format!("logos:local:{}", "6f".repeat(32));
        let account = "6f".repeat(32);
        let seed = |hash: &str, commitment: &str, secret: &str| {
            seed_membership(
                &store,
                hash,
                &registry,
                &commitment.repeat(32),
                "",
                &secret.repeat(32),
                MembershipState::Pending,
                0,
                100,
            )
        };
        seed("hu", "49", "fb");
        delegated_submit_callback(Arc::downgrade(&store), "hu".into(), account.clone())(Ok(
            r#"{"kind":"unconfirmed","error":"p: registration submitted (tx ab), not on chain within 150s","allocatorPeerId":"p","discovered":true,"unconfirmed":true}"#.to_string(),
        ));
        let rec = store.membership("hu").unwrap();
        assert_eq!(
            rec.cache.state,
            MembershipState::Pending,
            "left for the poller: {:?}",
            rec.cache.failed_reason
        );
        assert!(rec.cache.failed_reason.is_none());
        assert_eq!(
            lifecycle::granted_by("hu").as_deref(),
            Some("p (discovered)")
        );

        seed("hr", "4a", "fc");
        delegated_submit_callback(Arc::downgrade(&store), "hr".into(), account.clone())(Ok(
            r#"{"kind":"refused","reason":"credential_used","error":"p: credential already used: x","allocatorPeerId":"p","discovered":true}"#
                .to_string(),
        ));
        let rec = store.membership("hr").unwrap();
        assert_eq!(rec.cache.state, MembershipState::Failed);
        assert_eq!(
            rec.cache.failed_reason.as_deref(),
            Some("allocation_failed: p: credential already used: x")
        );
        assert!(!lifecycle::take_discovery_miss("hr"));

        seed("hm", "4b", "fd");
        delegated_submit_callback(Arc::downgrade(&store), "hm".into(), account)(Ok(
            r#"{"kind":"no_provider","discovery":"none_advertising","error":"no allocator granted and none is configured"}"#
                .to_string(),
        ));
        let rec = store.membership("hm").unwrap();
        assert_eq!(rec.cache.state, MembershipState::Failed);
        assert_eq!(
            rec.cache.failed_reason.as_deref(),
            Some("allocation_failed: no allocator granted and none is configured")
        );
        assert!(lifecycle::take_discovery_miss("hm"));
        assert!(
            !lifecycle::take_discovery_miss("hm"),
            "the note is taken once"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn late_submit_replies_respect_the_registry_and_reopen_a_window_failed_allocation() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir = std::env::temp_dir().join(format!("rln-late-reply-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (_imp, store) = imp_with_store(dir.clone());
        store.unlock("pw").unwrap();
        let registry = format!("logos:local:{}", "6f".repeat(32));
        let account = "6f".repeat(32);
        seed_membership(
            &store,
            "ha",
            &registry,
            &"46".repeat(32),
            "",
            &"f8".repeat(32),
            MembershipState::Active,
            7,
            100,
        );
        funded_submit_callback(Arc::downgrade(&store), "ha".into())(Ok(
            r#"{"leaf_index":3,"pending":true,"tx_result":"{}"}"#.to_string(),
        ));
        let rec = store.membership("ha").unwrap();
        assert_eq!(
            (rec.cache.state, rec.cache.leaf_index),
            (MembershipState::Active, Some(7)),
            "funded estimate kept out"
        );
        delegated_submit_callback(Arc::downgrade(&store), "ha".into(), account.clone())(Ok(
            format!(r#"{{"leaf_index":3,"allocatorPeerId":"p","config_account":"{account}"}}"#),
        ));
        let rec = store.membership("ha").unwrap();
        assert_eq!(
            (rec.cache.state, rec.cache.leaf_index),
            (MembershipState::Active, Some(7)),
            "allocator leaf kept out"
        );

        seed_membership(
            &store,
            "hg",
            &registry,
            &"47".repeat(32),
            "",
            &"f9".repeat(32),
            MembershipState::Failed,
            0,
            100,
        );
        store
            .update_cache("hg", |m| {
                m.failed_reason = Some(WINDOW_ELAPSED.into());
                m.retryable = Some(true);
            })
            .unwrap();
        delegated_submit_callback(Arc::downgrade(&store), "hg".into(), account.clone())(Ok(
            format!(
                r#"{{"leaf_index":9,"allocatorPeerId":"p","discovered":true,"config_account":"{account}"}}"#
            ),
        ));
        let rec = store.membership("hg").unwrap();
        assert_eq!(
            rec.cache.state,
            MembershipState::Pending,
            "re-opened for the poller: {:?}",
            rec.cache.failed_reason
        );
        assert_eq!(rec.cache.leaf_index, Some(9));
        assert!(rec.cache.failed_reason.is_none() && rec.cache.retryable.is_none());
        assert_eq!(
            lifecycle::granted_by("hg").as_deref(),
            Some("p (discovered)")
        );

        seed_membership(
            &store,
            "hf",
            &registry,
            &"48".repeat(32),
            "",
            &"fa".repeat(32),
            MembershipState::Failed,
            0,
            100,
        );
        store
            .update_cache("hf", |m| {
                m.failed_reason = Some("allocation_failed: refused".into())
            })
            .unwrap();
        delegated_submit_callback(Arc::downgrade(&store), "hf".into(), account)(Ok(
            r#"{"leaf_index":9,"allocatorPeerId":"p"}"#.to_string(),
        ));
        assert_eq!(
            store.membership("hf").unwrap().cache.state,
            MembershipState::Failed
        );

        seed_membership(
            &store,
            "hp",
            &registry,
            &"49".repeat(32),
            "",
            &"fb".repeat(32),
            MembershipState::Pending,
            0,
            100,
        );
        let written = poller::write_if_still("hp", MembershipState::Pending, |m| {
            m.state = MembershipState::Active
        })
        .unwrap();
        assert_eq!(written, Some(MembershipState::Pending));
        let yielded = poller::write_if_still("hp", MembershipState::Pending, |m| {
            m.state = MembershipState::Failed
        })
        .unwrap();
        assert_eq!(
            yielded, None,
            "the snapshot was Pending, the record is Active: no write"
        );
        assert_eq!(
            store.membership("hp").unwrap().cache.state,
            MembershipState::Active
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn funded_reply_refusal_fails_a_pending_record_and_an_acceptance_records_its_estimate() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir =
            std::env::temp_dir().join(format!("rln-funded-refusal-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (_imp, store) = imp_with_store(dir.clone());
        store.unlock("pw").unwrap();
        let registry = format!("logos:local:{}", "6f".repeat(32));
        for (hash, seed) in [("hr", "4a"), ("hk", "4b")] {
            seed_membership(
                &store,
                hash,
                &registry,
                &seed.repeat(32),
                "",
                &"fc".repeat(32),
                MembershipState::Pending,
                0,
                100,
            );
        }
        funded_submit_callback(Arc::downgrade(&store), "hr".into())(Ok(
            r#"{"error":"insufficient_funds: x"}"#.to_string(),
        ));
        let rec = store.membership("hr").unwrap();
        assert_eq!(rec.cache.state, MembershipState::Failed);
        assert_eq!(
            rec.cache.failed_reason.as_deref(),
            Some("submit_failed: insufficient_funds: x")
        );
        assert_eq!(rec.cache.retryable, Some(false));

        let accepted = r#"{"leaf_index":3,"pending":true,"tx_result":"{}"}"#;
        funded_submit_callback(Arc::downgrade(&store), "hk".into())(Ok(accepted.to_string()));
        let rec = store.membership("hk").unwrap();
        assert_eq!(rec.cache.state, MembershipState::Pending);
        assert_eq!(rec.cache.leaf_index, Some(3));
        assert_eq!(rec.cache.tx_result.as_deref(), Some(accepted));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_allocation_failure_is_read_from_its_fields_not_its_text() {
        use AllocationFailure::*;
        let of = |mut reply: serde_json::Value| AllocationFailure::of(&mut reply);
        let miss = |discovery: &str| {
            of(serde_json::json!({ "kind": "no_provider", "discovery": discovery, "error": "e" }))
        };
        assert_eq!(miss("none_advertising"), Some(DiscoveryMiss("e".into())));
        assert_eq!(miss("not_joined"), Some(DiscoveryMiss("e".into())));
        assert_eq!(miss("unavailable"), Some(Failed("e".into())));
        assert_eq!(
            of(serde_json::json!({ "kind": "no_provider", "error": "A: dial failed" })),
            Some(Failed("A: dial failed".into())),
            "found, and none reachable"
        );
        assert_eq!(
            of(serde_json::json!({
                "kind": "refused", "reason": "auth_rejected",
                "error": "no allocator advertises R/peer-any", "allocatorPeerId": "p",
            })),
            Some(Failed("no allocator advertises R/peer-any".into()))
        );
        assert_eq!(
            of(serde_json::json!({ "kind": "not_submitted", "error": "discovery not joined" })),
            Some(Failed("discovery not joined".into()))
        );
        assert_eq!(
            of(serde_json::json!({ "kind": "unconfirmed", "unconfirmed": true, "error": "u" })),
            Some(Unconfirmed("u".into()))
        );
        assert_eq!(
            of(serde_json::json!({ "kind": "auth_failed" })),
            Some(Failed("auth_failed".into()))
        );
        assert_eq!(
            of(serde_json::json!({ "leaf_index": 3, "allocatorPeerId": "p", "discovered": false })),
            None
        );
    }

    #[test]
    fn register_validates_arguments_before_touching_anything() {
        let imp = RlnModuleImpl::default();
        let rln_id = "7a".repeat(32);

        let out = imp.register_membership("not-caip10".into(), rln_id.clone(), String::new());
        assert!(out.contains(r#""kind":"invalid_argument""#), "got: {out}");

        let out = imp.register_membership(
            "eip155:1:0xB9cd878C90E49F797B4431fBF4fb333108CB90e6".into(),
            rln_id.clone(),
            String::new(),
        );
        assert!(out.contains(r#""kind":"unknown_registry""#), "got: {out}");

        let logos = format!("logos:local:{}", "8b".repeat(32));
        let out = imp.register_membership(logos.clone(), rln_id, opts_arr(&[("rate_limit", "0")]));
        assert!(out.contains(r#""kind":"invalid_argument""#), "got: {out}");

        let out = imp.register_membership(logos, "xyz".into(), String::new());
        assert!(out.contains(r#""kind":"invalid_argument""#), "got: {out}");
    }

    #[test]
    fn register_defaults_rate_limit_when_option_absent() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir = std::env::temp_dir().join(format!("rln-ms-default-rate-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (imp, _store) = imp_with_store(dir.clone());
        assert!(imp
            .unlock_keystore("pw".into())
            .contains(r#""unlocked":true"#));
        let registry = format!("logos:local:{}", "ab".repeat(32));

        let out = imp.register_membership(registry.clone(), "ef".repeat(32), opts_arr(&[]));
        assert!(
            out.contains(r#""kind":"provider_failure""#),
            "dead transport: {out}"
        );
        let listed = imp.get_memberships(registry);
        assert!(
            listed.contains(&format!(r#""rate_limit":{DEFAULT_RATE_LIMIT}"#)),
            "absent rate_limit option must apply the module default: {listed}"
        );

        sealed_store::store::publish(None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn register_local_idempotency_and_dead_transport() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir = std::env::temp_dir().join(format!("rln-ms-lib-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (imp, store) = imp_with_store(dir.clone());
        assert!(imp
            .unlock_keystore("pw".into())
            .contains(r#""unlocked":true"#));
        let rln_id = "ef".repeat(32);

        let reg_a = format!("logos:local:{}", "ab".repeat(32));
        let funding = opts_arr(&[("rate_limit", "300")]);
        let out = imp.register_membership(reg_a.clone(), rln_id.clone(), funding);
        assert!(out.contains(r#""kind":"provider_failure""#), "got: {out}");
        let listed = imp.get_memberships(reg_a);
        assert!(
            listed.contains("membership_hash"),
            "a credential was generated and persisted"
        );
        assert!(
            listed.contains(r#""retryable":true"#),
            "a provider_failure dispatch is retryable (spec: a failed submission SHALL report whether it is retryable): {listed}"
        );

        let reg_b = format!("logos:local:{}", "12".repeat(32));
        let commitment = [0x34u8; 32];
        let hash = registry_id::membership_hash(&reg_b, &commitment);
        seed_membership(
            &store,
            &hash,
            &reg_b,
            &registry_id::bytes_to_hex(&commitment),
            &"ef".repeat(32),
            &"22".repeat(32),
            MembershipState::Active,
            7,
            300,
        );

        let out =
            imp.register_membership(reg_b.clone(), rln_id, opts_arr(&[("rate_limit", "250")]));
        assert!(
            !out.contains(r#""error""#),
            "idempotent short-circuit, no provider call: {out}"
        );
        assert!(out.contains(r#""state":"active""#), "got: {out}");
        assert!(
            out.contains(r#""rate_limit":300"#),
            "existing registration's rate wins: {out}"
        );
        assert!(out.contains(r#""rate_limit_mismatch":true"#), "got: {out}");
        assert!(
            out.contains(&format!(r#""membership_hash":"{hash}""#)),
            "got: {out}"
        );

        let funding = opts_arr(&[("rate_limit", "300")]);
        let out = imp.register_membership(reg_b.clone(), "aa".repeat(32), funding);
        assert!(out.contains(r#""kind":"provider_failure""#), "got: {out}");
        let listed = imp.get_memberships(reg_b);
        assert_eq!(
            listed.matches("membership_hash").count(),
            2,
            "distinct scope must mint its own membership: {listed}"
        );

        sealed_store::store::publish(None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn register_ignores_terminal_records_mints_fresh_credential() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir = std::env::temp_dir().join(format!("rln-ms-terminal-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (imp, store) = imp_with_store(dir.clone());
        assert!(imp
            .unlock_keystore("pw".into())
            .contains(r#""unlocked":true"#));
        let rln_id = "ef".repeat(32);
        let funding = opts_arr(&[("rate_limit", "300")]);

        let reg_expired = format!("logos:local:{}", "56".repeat(32));
        let expired_commitment = [0x9au8; 32];
        let expired_hash = registry_id::membership_hash(&reg_expired, &expired_commitment);
        seed_membership(
            &store,
            &expired_hash,
            &reg_expired,
            &registry_id::bytes_to_hex(&expired_commitment),
            &rln_id,
            &"99".repeat(32),
            MembershipState::Expired,
            3,
            300,
        );

        let out = imp.register_membership(reg_expired.clone(), rln_id.clone(), funding.clone());
        assert!(
            out.contains(r#""kind":"provider_failure""#),
            "no short-circuit — the fresh submit hits the dead transport: {out}"
        );
        let records = store.records_for(&reg_expired);
        let hashes: Vec<&str> = records.iter().map(|r| r.hash.as_str()).collect();
        assert_eq!(
            records.len(),
            2,
            "the expired record is retained AND a fresh one was minted: {hashes:?}"
        );
        assert!(
            records
                .iter()
                .any(|r| r.hash != expired_hash && r.cache.state == MembershipState::Failed),
            "a NEW credential (different membership_hash) was minted, then failed at the \
             dead transport: {hashes:?}"
        );

        let reg_erased = format!("logos:local:{}", "78".repeat(32));
        let erased_commitment = [0x9bu8; 32];
        let erased_hash = registry_id::membership_hash(&reg_erased, &erased_commitment);
        seed_membership(
            &store,
            &erased_hash,
            &reg_erased,
            &registry_id::bytes_to_hex(&erased_commitment),
            &rln_id,
            &"aa".repeat(32),
            MembershipState::Erased,
            5,
            300,
        );

        let out = imp.register_membership(reg_erased.clone(), rln_id, funding);
        assert!(out.contains(r#""kind":"provider_failure""#), "got: {out}");
        let records = store.records_for(&reg_erased);
        assert_eq!(
            records.len(),
            2,
            "the erased record is retained AND a fresh one was minted"
        );
        assert!(records
            .iter()
            .any(|r| r.hash != erased_hash && r.cache.state == MembershipState::Failed));

        sealed_store::store::publish(None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delegated_register_validates_then_fails_cleanly_on_dead_transport() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir =
            std::env::temp_dir().join(format!("rln-ms-delegated-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (imp, _store) = imp_with_store(dir.clone());
        assert!(imp
            .unlock_keystore("pw".into())
            .contains(r#""unlocked":true"#));
        let registry = format!("logos:local:{}", "ab".repeat(32));
        let rln_id = "ef".repeat(32);

        let out = imp.register_membership(
            registry.clone(),
            rln_id.clone(),
            opts_arr(&[("delegated", "true"), ("discover", "false")]),
        );
        assert!(out.contains(r#""kind":"invalid_argument""#), "got: {out}");
        assert_eq!(
            imp.get_memberships(registry.clone()),
            r#"{"memberships":[]}"#,
            "invalid delegated options must not mint a credential"
        );

        let opts = opts_arr(&[
            ("delegated", "true"),
            ("allocator_peer_id", "12D3KooWTest"),
            ("allocator_multiaddr", "/ip4/127.0.0.1/tcp/1"),
            ("auth_type", "keycard-attestation"),
            ("auth_provider", "keycard_capture_module"),
        ]);
        let out = imp.register_membership(registry.clone(), rln_id.clone(), opts);
        assert!(out.contains(r#""kind":"provider_failure""#), "got: {out}");
        assert!(
            imp.get_memberships(registry.clone())
                .contains("membership_hash"),
            "the credential was minted and the failed record retained"
        );

        let opts = opts_arr(&[
            ("delegated", "true"),
            ("allocator_peer_id", "12D3KooWTest"),
            ("allocator_multiaddr", "/ip4/127.0.0.1/tcp/1"),
            ("auth_type", "voucher-v1"),
            ("auth_payload", "deadbeef"),
        ]);
        let out = imp.register_membership(registry, rln_id, opts);
        assert!(out.contains(r#""kind":"provider_failure""#), "got: {out}");

        sealed_store::store::publish(None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_auth_type_alone_reaches_the_allocator_with_no_payload() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir = std::env::temp_dir().join(format!("rln-auth-type-alone-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (imp, _store) = imp_with_store(dir.clone());
        assert!(imp
            .unlock_keystore("pw".into())
            .contains(r#""unlocked":true"#));
        let sent = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&sent);
        crate::test_support::install_allocator(Box::new(move |method, args| {
            crate::lock(&log).push((method.to_owned(), args.clone()));
            Ok(serde_json::json!({
                "kind": "refused", "reason": "auth_type_not_served",
                "error": "p: peer-any not served", "allocatorPeerId": "p", "discovered": false,
            })
            .to_string())
        }));
        let out = imp.register_membership(
            format!("logos:local:{}", "5c".repeat(32)),
            "ef".repeat(32),
            opts_arr(&[
                ("delegated", "true"),
                ("allocator_peer_id", "12D3KooWTest"),
                ("allocator_multiaddr", "/ip4/127.0.0.1/tcp/1"),
                ("auth_type", "peer-any"),
            ]),
        );
        *crate::lock(&crate::test_support::INSTALLED_ALLOCATOR) = None;
        assert!(
            out.contains(r#""failed_reason":"allocation_failed: p: peer-any not served""#)
                && out.contains(r#""state":"failed""#),
            "the refusal lands on the record: {out}"
        );
        let sent = std::mem::take(&mut *crate::lock(&sent));
        let [(method, args)] = sent.as_slice() else {
            panic!("one call expected: {sent:?}");
        };
        assert_eq!(method, "request");
        assert_eq!(args["authType"], "peer-any", "got: {args}");
        for key in ["authPayload", "authProvider", "authArgs"] {
            assert!(args.get(key).is_none(), "{key} must stay absent: {args}");
        }
        assert_eq!(args["allocatorPeerId"], "12D3KooWTest", "got: {args}");
        assert_eq!(args["allocatorMultiaddr"], "/ip4/127.0.0.1/tcp/1");

        sealed_store::store::publish(None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn register_rejects_non_string_values_and_non_array_options() {
        let imp = RlnModuleImpl::default();
        let registry = format!("logos:local:{}", "9d".repeat(32));
        let rln_id = "6e".repeat(32);

        let out = imp.register_membership(
            registry.clone(),
            rln_id.clone(),
            r#"[{"key":"delegated","value":true}]"#.into(),
        );
        assert!(out.contains(r#""kind":"invalid_argument""#), "got: {out}");
        assert!(out.contains("'delegated' must be a string"), "got: {out}");

        let out = imp.register_membership(
            registry.clone(),
            rln_id.clone(),
            r#"{"delegated":"true"}"#.into(),
        );
        assert!(out.contains(r#""kind":"invalid_argument""#), "got: {out}");
        assert!(
            out.contains("must be a RegistryOptions array"),
            "got: {out}"
        );

        let out = imp.register_membership(
            registry,
            rln_id,
            r#"[{"key":"rate_limit","value":"5"},{"key":"rate_limit","value":"9"}]"#.into(),
        );
        assert!(
            out.contains("duplicate RegistryOptions key 'rate_limit'"),
            "got: {out}"
        );
    }

    #[test]
    fn delegated_register_validates_auth_options_before_minting() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir = std::env::temp_dir().join(format!("rln-ms-auth-validate-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (imp, _store) = imp_with_store(dir.clone());
        assert!(imp
            .unlock_keystore("pw".into())
            .contains(r#""unlocked":true"#));
        let registry = format!("logos:local:{}", "ab".repeat(32));
        let rln_id = "ef".repeat(32);
        let opts = |extra: &str| {
            format!(
                r#"[{{"key":"delegated","value":"true"}},
                    {{"key":"allocator_peer_id","value":"12D3KooWTest"}},
                    {{"key":"allocator_multiaddr","value":"/ip4/127.0.0.1/tcp/1"}},{extra}]"#
            )
        };
        let reg = |imp: &RlnModuleImpl, extra: &str| {
            imp.register_membership(registry.clone(), rln_id.clone(), opts(extra))
        };

        for (extra, expect) in [
            (
                r#"{"key":"auth_type","value":42}"#,
                "'auth_type' must be a string",
            ),
            (
                r#"{"key":"auth_payload","value":"deadbeef"}"#,
                "need auth_type",
            ),
            (
                r#"{"key":"auth_type","value":"voucher-v1"},{"key":"auth_payload","value":"deadbeef"},{"key":"auth_provider","value":"voucher_module"}"#,
                "mutually exclusive",
            ),
            (
                r#"{"key":"auth_type","value":"voucher-v1"},{"key":"auth_payload","value":"not-hex!"}"#,
                "must be hex",
            ),
        ] {
            let out = reg(&imp, extra);
            assert!(
                out.contains(r#""kind":"invalid_argument""#),
                "{extra} got: {out}"
            );
            assert!(out.contains(expect), "{extra} got: {out}");
        }

        let out = imp.register_membership(
            registry.clone(),
            rln_id.clone(),
            opts_arr(&[("auth_type", "voucher-v1"), ("auth_payload", "deadbeef")]),
        );
        assert!(out.contains(r#""kind":"invalid_argument""#), "got: {out}");
        assert!(out.contains("delegated registration only"), "got: {out}");

        assert_eq!(
            imp.get_memberships(registry),
            r#"{"memberships":[]}"#,
            "no rejection above may mint a credential"
        );

        sealed_store::store::publish(None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_allocator_block_without_a_pair_discovers() {
        let d = DelegatedOptions::from_allocator_block(Some(&serde_json::json!({})), "x")
            .unwrap()
            .unwrap();
        assert!(d.discover && d.allocator_peer_id.is_empty() && d.allocator_multiaddr.is_empty());
        let v: serde_json::Value =
            serde_json::from_str(&delegated_request_args(&d, "ab", 300, "cf")).unwrap();
        assert_eq!(v["discover"], true, "got: {v}");
        assert!(
            v.get("allocatorPeerId").is_none() && v.get("allocatorMultiaddr").is_none(),
            "got: {v}"
        );
        let flat = |pairs: serde_json::Value| {
            let (_, flat) = parse_registry_options(&pairs.to_string()).unwrap();
            DelegatedOptions::from_registry_options(flat).unwrap()
        };
        assert_eq!(
            flat(serde_json::json!([{ "key": "delegated", "value": "true" }])),
            Some(d)
        );

        let d = DelegatedOptions::from_allocator_block(
            Some(&serde_json::json!({ "discover": false, "peer_id": "p", "multiaddr": "/m" })),
            "x",
        )
        .unwrap()
        .unwrap();
        assert!(!d.discover);
        let v: serde_json::Value =
            serde_json::from_str(&delegated_request_args(&d, "ab", 300, "cf")).unwrap();
        assert_eq!(v["discover"], false, "got: {v}");
        assert_eq!(v["allocatorPeerId"], "p", "got: {v}");
        assert_eq!(
            flat(serde_json::json!([
                { "key": "delegated", "value": "true" },
                { "key": "discover", "value": "false" },
                { "key": "allocator_peer_id", "value": "p" },
                { "key": "allocator_multiaddr", "value": "/m" },
            ])),
            Some(d)
        );
        assert_eq!(
            flat(serde_json::json!([{ "key": "rate_limit", "value": "7" }])),
            None
        );
    }
}
