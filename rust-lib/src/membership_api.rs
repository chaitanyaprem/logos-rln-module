use std::sync::Arc;

use crate::ensure;
use crate::error::{ApiError, ErrorKind};
use crate::lifecycle::{self, MembershipRecord, MembershipState};
use crate::ok_json;
use crate::provider;
use crate::register::parse_registry_options;
use crate::scope::{
    backend_of, parse_registry, parse_scope, records_for_registry, resolve_scope_state, ScopeState,
};
use crate::sealed_store::store::Store;
use crate::select;
use crate::util::now_unix;
use crate::views;
use crate::worker;

pub(crate) fn provisioning_view(registry: &str) -> Option<views::ProvisioningView> {
    ensure::progress(registry).map(|p| {
        let view = views::ProvisioningView::new(p.step.as_str(), p.detail)
            .with_fallback_reason(p.fallback_reason);
        match p.funding {
            Some(f) => view.with_funding(f.payer, f.required, f.price, f.fee_reserve, f.balance),
            None => view,
        }
    })
}

pub(crate) fn get_membership_state_impl(
    store: Result<Arc<Store>, ApiError>,
    registry_id_raw: &str,
    rln_identifier_hex: &str,
) -> Result<serde_json::Value, ApiError> {
    let (registry, _, rln_id_hex) = parse_scope(registry_id_raw, rln_identifier_hex)?;
    let backend = backend_of(&registry)?;

    let Ok(store) = store else {
        return ok_json(
            views::MembershipStateView::unknown(&registry.canonical)
                .with_provisioning(provisioning_view(&registry.canonical)),
        );
    };
    let records = records_for_registry(&store, &registry);
    let record = match resolve_scope_state(&records, &rln_id_hex) {
        ScopeState::None => {
            return ok_json(
                views::MembershipStateView::unknown(&registry.canonical)
                    .with_provisioning(provisioning_view(&registry.canonical)),
            )
        }
        ScopeState::Ambiguous(why) => {
            return Err(ApiError::new(ErrorKind::AmbiguousSelection, why))
        }
        ScopeState::One(r)
            if !r.cache.state.is_live()
                && r.cache
                    .failed_reason
                    .as_deref()
                    .is_some_and(|w| w.starts_with(crate::register::ALLOCATION_FAILED))
                && ensure::is_running(&registry.canonical) =>
        {
            return ok_json(
                views::MembershipStateView::unknown(&registry.canonical)
                    .with_provisioning(provisioning_view(&registry.canonical)),
            )
        }
        ScopeState::One(r) => r,
    };
    let hash = &record.hash;

    let pm = backend
        .registry
        .get_membership(&registry, &record.identity.identity_commitment)?;
    let registry_state = pm.as_ref().map(|p| p.state);
    let merged = lifecycle::merge_state(Some(record), registry_state, now_unix());

    if merged != record.cache.state {
        let snapshot_state = record.cache.state;
        let persist = store.update_cache(hash, |m| {
            if m.state != snapshot_state {
                return;
            }
            m.state = merged;
            if let Some(pm) = &pm {
                if let Some(l) = pm.leaf_index {
                    m.leaf_index = Some(l);
                }
                m.rate_limit = Some(pm.rate_limit);
                m.failed_reason = None;
                m.retryable = None;
            } else if merged == MembershipState::Failed {
                m.failed_reason = Some("confirmation_window_elapsed".to_string());
                m.retryable = Some(true);
            } else if merged == MembershipState::Erased {
                m.failed_reason = Some("removed_from_registry".to_string());
            }
        });
        match persist {
            Err(e) => eprintln!("membership state persist: {}", e.message),
            Ok(prior) if prior == snapshot_state => {
                lifecycle::emit_transition(hash, record, prior, merged);
            }
            Ok(_) => {}
        }
    }

    let (leaf_index, rate_limit) = match &pm {
        Some(pm) => (
            pm.leaf_index.or(record.cache.leaf_index).unwrap_or(0),
            pm.rate_limit,
        ),
        None => (
            record.cache.leaf_index.unwrap_or(0),
            record.cache.rate_limit.unwrap_or(0),
        ),
    };
    let now = if merged.is_live() {
        None
    } else {
        store.membership(hash)
    };
    let mut view = views::MembershipStateView::resolved(
        hash,
        &registry.canonical,
        merged,
        leaf_index,
        rate_limit,
    );
    if let Some(now) = &now {
        view = view.with_failure(now.cache.failed_reason.as_deref(), now.cache.retryable);
    }
    ok_json(view)
}

pub(crate) fn select_membership_impl(
    store: Result<Arc<Store>, ApiError>,
    registry_id_raw: &str,
    rln_identifier_hex: &str,
    selector_json: &str,
) -> Result<serde_json::Value, ApiError> {
    let (registry, _, rln_identifier_hex) = parse_scope(registry_id_raw, rln_identifier_hex)?;
    let selector = select::parse_selector(selector_json)?;

    let store = store?;
    let records = records_for_registry(&store, &registry);
    let record = select::select(
        &records,
        (&registry.canonical, &rln_identifier_hex),
        &selector,
    )?;
    Ok(views::public_membership(
        &record.hash,
        record,
        false,
        false,
        false,
    ))
}

pub(crate) fn ensure_membership_impl(
    store: Result<Arc<Store>, ApiError>,
    registry_id_raw: &str,
    options_json: &str,
) -> Result<serde_json::Value, ApiError> {
    let registry = parse_registry(registry_id_raw)?;
    let (rate_limit, rest) = parse_registry_options(options_json)?;
    if let Some(key) = rest.as_object().and_then(|o| o.keys().next()) {
        return Err(ApiError::new(
            ErrorKind::InvalidArgument,
            format!(
                "ensure_membership takes only rate_limit; '{key}' selects a scoped \
                 registration, which is register_membership's"
            ),
        ));
    }
    let store = store?;
    let records = records_for_registry(&store, &registry);
    let live: Vec<&MembershipRecord> = records
        .iter()
        .map(Arc::as_ref)
        .filter(|r| !r.quarantined && r.cache.state.is_live())
        .collect();
    match live.as_slice() {
        [] => {}
        [record] => {
            return ok_json(views::MembershipStateView::resolved(
                &record.hash,
                &registry.canonical,
                record.cache.state,
                record.cache.leaf_index.unwrap_or(0),
                record.cache.rate_limit.unwrap_or(0),
            ));
        }
        _ => return Err(ApiError::new(
            ErrorKind::AmbiguousSelection,
            "multiple live memberships on this registry; use get_memberships / select_membership",
        )),
    }
    if worker::is_stopped() {
        return Err(ApiError::new(
            ErrorKind::NotReady,
            "module stopped; start() first",
        ));
    }
    if let Some(pass) = ensure::ActivePass::claim(&registry.canonical) {
        ensure::mark_requested(&registry.canonical);
        let canonical = registry.canonical.clone();
        let ticket = pass.ticket();
        std::thread::Builder::new()
            .name("rln-ensure".to_owned())
            .spawn(move || {
                let _claim = pass;
                provider::select_networks(std::slice::from_ref(&canonical));
                ensure::run_alongside(canonical, rate_limit, ticket);
            })
            .map_err(|e| ApiError::internal(format!("cannot spawn provisioning: {e}")))?;
    }
    ok_json(
        views::MembershipStateView::unknown(&registry.canonical)
            .with_provisioning(provisioning_view(&registry.canonical)),
    )
}

pub(crate) fn get_memberships_impl(
    store: Result<Arc<Store>, ApiError>,
    registry_id_raw: &str,
) -> Result<serde_json::Value, ApiError> {
    let registry = parse_registry(registry_id_raw)?;
    let store = store?;
    let records = records_for_registry(&store, &registry);
    let memberships: Vec<serde_json::Value> = records
        .iter()
        .map(|r| views::public_membership(&r.hash, r, r.quarantined, false, false))
        .collect();
    Ok(serde_json::json!({ "memberships": memberships }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::RlnModule;
    use crate::proof;
    use crate::proof_api::{generate_proof_impl, validate_proof_impl};
    use crate::register::{RegisterClaim, WINDOW_ELAPSED};
    use crate::registry_id;
    use crate::sealed_store;
    use crate::test_support::{
        imp_with_store, no_store, opts_arr, seed_membership, TEST_GLOBAL_LOCK,
    };

    #[test]
    fn a_terminal_record_reads_unknown_while_provisioning_runs() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir = std::env::temp_dir().join(format!(
            "rln-terminal-while-prov-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let (imp, store) = imp_with_store(dir.clone());
        store.unlock("pw").unwrap();
        let registry = format!("logos:local:{}", "6d".repeat(32));
        let canonical = registry_id::parse(&registry).unwrap().canonical;
        let rid = "b1".repeat(32);
        seed_membership(
            &store,
            "hf",
            &registry,
            &"44".repeat(32),
            "",
            &"f6".repeat(32),
            MembershipState::Failed,
            0,
            100,
        );
        store
            .update_cache("hf", |m| {
                m.failed_reason = Some("allocation_failed: dead allocator".into())
            })
            .unwrap();
        {
            let _pass = ensure::ActivePass::enter(&canonical);
            ensure::mark_requested(&canonical);
            let out = get_membership_state_impl(imp.store(), &registry, &rid).unwrap();
            assert_eq!(out["state"], serde_json::json!("unknown"), "got: {out}");
            assert!(
                out.get("provisioning").is_some(),
                "the pass's step rides along: {out}"
            );
        }
        assert!(get_membership_state_impl(imp.store(), &registry, &rid).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn only_an_allocator_refusal_is_masked_while_provisioning_runs() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir = std::env::temp_dir().join(format!("rln-mask-window-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (imp, store) = imp_with_store(dir.clone());
        store.unlock("pw").unwrap();
        let registry = format!("logos:local:{}", "6e".repeat(32));
        let canonical = registry_id::parse(&registry).unwrap().canonical;
        let rid = "b2".repeat(32);
        seed_membership(
            &store,
            "hw",
            &registry,
            &"45".repeat(32),
            "",
            &"f7".repeat(32),
            MembershipState::Failed,
            0,
            100,
        );
        store
            .update_cache("hw", |m| m.failed_reason = Some(WINDOW_ELAPSED.into()))
            .unwrap();
        let _pass = ensure::ActivePass::enter(&canonical);
        ensure::mark_requested(&canonical);
        assert!(
            get_membership_state_impl(imp.store(), &registry, &rid).is_err(),
            "window-failed: read through"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ensure_membership_after_stop_is_not_ready() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir =
            std::env::temp_dir().join(format!("rln-ensure-stopped-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (imp, _store) = imp_with_store(dir.clone());
        imp.stop().unwrap();
        let registry = format!("logos:local:{}", "7a".repeat(32));
        let e = ensure_membership_impl(imp.store(), &registry, "").unwrap_err();
        assert_eq!(e.kind, ErrorKind::NotReady, "{}", e.message);
        worker::reset_for_test();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn empty_scope_args_are_invalid_argument() {
        let registry = format!("logos:local:{}", "dd".repeat(32));
        let rln_id = "ef".repeat(32);

        let err = get_membership_state_impl(no_store(), "", "").unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument, "got: {}", err.message);
        let err = get_membership_state_impl(no_store(), &registry, "").unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument, "got: {}", err.message);
        let err = get_membership_state_impl(no_store(), "", &rln_id).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument, "got: {}", err.message);

        let err = validate_proof_impl("", "", "00", "0", "{}").unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument, "got: {}", err.message);
        let err = validate_proof_impl(&registry, "", "00", "0", "{}").unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument, "got: {}", err.message);
    }

    #[test]
    fn membership_state_reads_take_no_register_claim() {
        let registry = format!("logos:local:{}", "1a".repeat(32));
        let rln_id = "2b".repeat(32);
        let (canonical, _, rln_id_hex) = parse_scope(&registry, &rln_id).unwrap();

        let claim = RegisterClaim::take(&canonical.canonical, &rln_id_hex).ok();
        assert!(claim.is_some(), "the first claim must be granted");
        match RegisterClaim::take(&canonical.canonical, &rln_id_hex) {
            Ok(_) => panic!("the register claim must stay exclusive"),
            Err(e) => assert!(
                e.message.contains("already in flight"),
                "got: {}",
                e.message
            ),
        }

        for _ in 0..2 {
            let out = get_membership_state_impl(no_store(), &registry, &rln_id).unwrap();
            assert_eq!(out["state"], serde_json::json!("unknown"), "got: {out}");
        }
        drop(claim);

        let out = get_membership_state_impl(no_store(), &registry, &rln_id).unwrap();
        assert_eq!(out["state"], serde_json::json!("unknown"), "got: {out}");
        assert!(
            RegisterClaim::take(&canonical.canonical, &rln_id_hex).is_ok(),
            "a read must leave no claim behind"
        );
    }

    #[test]
    fn ensure_membership_validates_before_touching_the_store() {
        let registry = format!("logos:local:{}", "e1".repeat(32));
        let canonical = parse_registry(&registry).unwrap().canonical;

        let err = ensure_membership_impl(no_store(), "", "").unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument, "got: {}", err.message);

        let err =
            ensure_membership_impl(no_store(), &registry, &opts_arr(&[("delegated", "true")]))
                .unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument, "got: {}", err.message);
        assert!(
            err.message.contains("register_membership"),
            "got: {}",
            err.message
        );

        let err = ensure_membership_impl(no_store(), &registry, &opts_arr(&[("rate_limit", "0")]))
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument, "got: {}", err.message);

        let err = ensure_membership_impl(no_store(), &registry, "").unwrap_err();
        assert_eq!(err.kind, ErrorKind::Internal, "got: {}", err.message);
        assert!(!ensure::is_running(&canonical));
        assert!(
            ensure::progress(&canonical).is_none(),
            "nothing may be recorded for it"
        );
    }

    #[test]
    fn an_active_membership_without_a_leaf_keeps_the_stored_one() {
        use crate::provider::fake::{self, FakePayer, FakeRegistry, FakeTree};
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        crate::test_support::install_backend(fake::backend(
            FakeTree::default(),
            FakeRegistry {
                membership: Some(Some((MembershipState::Active, None, 300))),
                bounds: None,
            },
            FakePayer::default(),
        ));
        crate::start_impl(r#"{"epoch_size_sec": 600}"#).unwrap();
        let dir =
            std::env::temp_dir().join(format!("rln-no-leaf-read-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (imp, store) = imp_with_store(dir.clone());
        assert!(imp
            .unlock_keystore("pw".into())
            .contains(r#""unlocked":true"#));
        let rln_id_hex = "ef".repeat(32);

        let fresh = "fakens:local:fresh";
        let (commitment_hex, secret_hex) = proof::generate_identity().expect("test identity");
        let commitment = registry_id::hex_to_bytes32(&commitment_hex).unwrap();
        let hash = registry_id::membership_hash(fresh, &commitment);
        seed_membership(
            &store,
            &hash,
            fresh,
            &commitment_hex,
            &rln_id_hex,
            &secret_hex,
            MembershipState::Pending,
            0,
            300,
        );
        store.update_cache(&hash, |m| m.leaf_index = None).unwrap();
        let out = get_membership_state_impl(imp.store(), fresh, &rln_id_hex).unwrap();
        assert_eq!(out["state"], serde_json::json!("active"), "got: {out}");
        assert_eq!(out["leaf_index"], serde_json::json!(0), "got: {out}");
        let rec = store.membership(&hash).unwrap();
        assert_eq!(
            (rec.cache.state, rec.cache.leaf_index),
            (MembershipState::Active, None)
        );
        let err = generate_proof_impl(
            imp.store(),
            fresh,
            &rln_id_hex,
            "aa",
            &now_unix().to_string(),
        )
        .expect_err("no leaf, no proof");
        assert!(err.message.contains("leaf_index"), "got: {}", err.message);

        let known = "fakens:local:known";
        let (commitment_hex, secret_hex) = proof::generate_identity().expect("test identity");
        let commitment = registry_id::hex_to_bytes32(&commitment_hex).unwrap();
        let hash = registry_id::membership_hash(known, &commitment);
        seed_membership(
            &store,
            &hash,
            known,
            &commitment_hex,
            &rln_id_hex,
            &secret_hex,
            MembershipState::Pending,
            7,
            300,
        );
        let out = get_membership_state_impl(imp.store(), known, &rln_id_hex).unwrap();
        assert_eq!(out["leaf_index"], serde_json::json!(7), "got: {out}");
        let rec = store.membership(&hash).unwrap();
        assert_eq!(
            (rec.cache.state, rec.cache.leaf_index),
            (MembershipState::Active, Some(7))
        );

        sealed_store::store::publish(None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ensure_membership_reports_a_live_membership_of_any_scope_without_spawning() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir = std::env::temp_dir().join(format!("rln-ensure-live-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (imp, store) = imp_with_store(dir.clone());
        assert!(imp
            .unlock_keystore("pw".into())
            .contains(r#""unlocked":true"#));

        let registry = format!("logos:local:{}", "e2".repeat(32));
        let canonical = parse_registry(&registry).unwrap().canonical;
        let scoped_rln_id = "ef".repeat(32);
        let (commitment_hex, secret_hex) = proof::generate_identity().expect("test identity");
        let commitment =
            registry_id::hex_to_bytes32(&commitment_hex).expect("generated commitment is 32 bytes");
        let hash = registry_id::membership_hash(&registry, &commitment);
        seed_membership(
            &store,
            &hash,
            &registry,
            &commitment_hex,
            &scoped_rln_id,
            &secret_hex,
            MembershipState::Active,
            7,
            300,
        );

        let out = ensure_membership_impl(imp.store(), &registry, "").unwrap();
        assert_eq!(out["state"], serde_json::json!("active"), "got: {out}");
        assert_eq!(
            out["membership_hash"],
            serde_json::json!(hash),
            "got: {out}"
        );
        assert_eq!(out["leaf_index"], serde_json::json!(7), "got: {out}");
        assert_eq!(out["rate_limit"], serde_json::json!(300), "got: {out}");
        assert!(
            out.get("provisioning").is_none(),
            "a provisioned registry has no pass: {out}"
        );
        assert!(!ensure::is_running(&canonical));
        assert!(ensure::progress(&canonical).is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ensure_membership_spawns_one_pass_and_reports_its_step() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir =
            std::env::temp_dir().join(format!("rln-ensure-spawn-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (imp, _store) = imp_with_store(dir.clone());

        let busy = format!("logos:local:{}", "e3".repeat(32));
        let busy_canonical = parse_registry(&busy).unwrap().canonical;
        {
            let _held = ensure::ActivePass::enter(&busy_canonical);
            let out = ensure_membership_impl(imp.store(), &busy, "").unwrap();
            assert_eq!(out["state"], serde_json::json!("unknown"), "got: {out}");
            assert!(
                out.get("provisioning").is_none(),
                "a running pass is reported as it stands, not re-marked: {out}"
            );
            assert!(ensure::progress(&busy_canonical).is_none());
        }

        let fresh = format!("logos:local:{}", "e4".repeat(32));
        let fresh_canonical = parse_registry(&fresh).unwrap().canonical;
        let out = ensure_membership_impl(imp.store(), &fresh, &opts_arr(&[("rate_limit", "250")]))
            .unwrap();
        assert_eq!(out["state"], serde_json::json!("unknown"), "got: {out}");
        assert_eq!(
            out["registry_id"],
            serde_json::json!(fresh_canonical),
            "got: {out}"
        );
        assert!(
            out["provisioning"]["step"].is_string(),
            "the spawning reply carries the pass's first step: {out}"
        );
        assert!(ensure::progress(&fresh_canonical).is_some());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn select_membership_returns_public_view_without_releasing_secret() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir = std::env::temp_dir().join(format!("rln-ms-select-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (imp, store) = imp_with_store(dir.clone());
        assert!(imp
            .unlock_keystore("pw".into())
            .contains(r#""unlocked":true"#));

        let registry = format!("logos:local:{}", "cd".repeat(32));
        let commitment = [0x66u8; 32];
        let hash = registry_id::membership_hash(&registry, &commitment);
        seed_membership(
            &store,
            &hash,
            &registry,
            &registry_id::bytes_to_hex(&commitment),
            "",
            &"77".repeat(32),
            MembershipState::Active,
            3,
            300,
        );

        let rln_id = "ef".repeat(32);
        assert!(imp.lock_keystore().contains(r#""locked":true"#));
        let out = imp.select_membership(registry.clone(), rln_id, String::new());
        assert!(
            out.contains(&format!(r#""membership_hash":"{hash}""#)),
            "got: {out}"
        );
        assert!(
            !out.contains("identity_secret_hash"),
            "the identity secret must NEVER be released: {out}"
        );
        assert!(
            !out.contains(r#""kind":"locked""#),
            "no unlock required to select: {out}"
        );
        assert!(imp.get_memberships(registry).contains(&hash));

        sealed_store::store::publish(None);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
