use std::sync::Arc;

use crate::error::{ApiError, ErrorKind};
use crate::lifecycle::MembershipRecord;
use crate::provider;
use crate::registry_id;
use crate::sealed_store::store::Store;

pub(crate) fn parse_hex32(field: &str, hex: &str) -> Result<([u8; 32], String), ApiError> {
    let bytes = registry_id::hex_to_bytes32(hex).ok_or_else(|| {
        ApiError::new(
            ErrorKind::InvalidArgument,
            format!("{field} must be 32-byte hex"),
        )
    })?;
    let hex = registry_id::bytes_to_hex(&bytes);
    Ok((bytes, hex))
}

pub(crate) fn parse_registry(raw: &str) -> Result<registry_id::CanonicalRegistryId, ApiError> {
    registry_id::parse(raw).map_err(|e| ApiError::new(ErrorKind::InvalidArgument, e))
}

pub(crate) fn records_for_registry(
    store: &Store,
    registry: &registry_id::CanonicalRegistryId,
) -> Vec<Arc<MembershipRecord>> {
    store.records_for(&registry.canonical)
}

pub(crate) fn parse_scope(
    registry_id_raw: &str,
    rln_identifier_hex: &str,
) -> Result<(registry_id::CanonicalRegistryId, [u8; 32], String), ApiError> {
    let registry = parse_registry(registry_id_raw)?;
    let (bytes, hex) = parse_hex32("rln_identifier", rln_identifier_hex)?;
    Ok((registry, bytes, hex))
}

pub(crate) fn scope_matches(record: &MembershipRecord, rln_id_hex: &str) -> bool {
    record.identity.rln_identifier == rln_id_hex || record.identity.rln_identifier.is_empty()
}

#[cfg(test)]
pub(crate) fn scope_matches_for_test(record_rln_id: &str, scope_rln_id: &str) -> bool {
    record_rln_id == scope_rln_id || record_rln_id.is_empty()
}

pub(crate) fn scope_candidates<'a>(
    records: &'a [Arc<MembershipRecord>],
    rln_id_hex: &str,
) -> Vec<&'a MembershipRecord> {
    let usable = |r: &&MembershipRecord| !r.quarantined && r.cache.state.is_usable();
    let own: Vec<&MembershipRecord> = records
        .iter()
        .map(Arc::as_ref)
        .filter(usable)
        .filter(|r| r.identity.rln_identifier == rln_id_hex)
        .collect();
    if !own.is_empty() {
        return own;
    }
    let wide: Vec<&MembershipRecord> = records
        .iter()
        .map(Arc::as_ref)
        .filter(usable)
        .filter(|r| r.identity.rln_identifier.is_empty())
        .collect();
    if !wide.is_empty() {
        return wide;
    }
    records.iter().map(Arc::as_ref).collect()
}

pub(crate) enum ScopeState<'a> {
    One(&'a MembershipRecord),
    Ambiguous(&'static str),
    None,
}

pub(crate) fn resolve_scope_state<'a>(
    records: &'a [Arc<MembershipRecord>],
    rln_id_hex: &str,
) -> ScopeState<'a> {
    let own: Vec<&MembershipRecord> = records
        .iter()
        .map(Arc::as_ref)
        .filter(|r| !r.quarantined && r.identity.rln_identifier == rln_id_hex)
        .collect();
    let wide: Vec<&MembershipRecord> = records
        .iter()
        .map(Arc::as_ref)
        .filter(|r| !r.quarantined && r.identity.rln_identifier.is_empty())
        .collect();
    for rank in [own, wide] {
        if rank.is_empty() {
            continue;
        }
        let live: Vec<&&MembershipRecord> =
            rank.iter().filter(|r| r.cache.state.is_live()).collect();
        return match live.len() {
            1 => ScopeState::One(live[0]),
            0 => {
                let latest = rank
                    .iter()
                    .max_by_key(|r| r.identity.submitted_at)
                    .expect("invariant: an empty rank is skipped above");
                ScopeState::One(latest)
            }
            _ => ScopeState::Ambiguous(
                "multiple live memberships registered under this scope; use get_memberships / select_membership",
            ),
        };
    }
    let usable: Vec<&MembershipRecord> = records
        .iter()
        .map(Arc::as_ref)
        .filter(|r| !r.quarantined && r.cache.state.is_usable())
        .collect();
    match usable.len() {
        0 => ScopeState::None,
        1 => ScopeState::One(usable[0]),
        _ => ScopeState::Ambiguous(
            "multiple memberships back this scope; use get_memberships / select_membership",
        ),
    }
}

pub(crate) fn backend_of(
    registry: &registry_id::CanonicalRegistryId,
) -> Result<provider::Backend, ApiError> {
    provider::backend_for(&registry.namespace).ok_or_else(|| {
        ApiError::new(
            ErrorKind::UnknownRegistry,
            format!("no registry provider for namespace {}", registry.namespace),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::MembershipState;
    use crate::test_support::{imp_with_store, seed_membership, TEST_GLOBAL_LOCK};

    #[test]
    fn membership_state_resolves_its_record_by_scope_rank() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir = std::env::temp_dir().join(format!("rln-scope-rank-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (_imp, store) = imp_with_store(dir.clone());
        store.unlock("pw").unwrap();
        let registry = format!("logos:local:{}", "5c".repeat(32));
        let (r1, r2, r3) = ("a1".repeat(32), "a2".repeat(32), "a3".repeat(32));
        let state_of = |scope: &str| match resolve_scope_state(&store.records_for(&registry), scope)
        {
            ScopeState::One(r) => Ok(r.hash.clone()),
            ScopeState::Ambiguous(why) => Err(why.to_string()),
            ScopeState::None => Ok(String::new()),
        };

        assert_eq!(state_of(&r2).unwrap(), "");
        seed_membership(
            &store,
            "h1",
            &registry,
            &"11".repeat(32),
            &r1,
            &"f1".repeat(32),
            MembershipState::Active,
            1,
            100,
        );
        assert_eq!(state_of(&r1).unwrap(), "h1");
        assert_eq!(
            state_of(&r2).unwrap(),
            "h1",
            "a lone membership backs any application"
        );

        seed_membership(
            &store,
            "h2",
            &registry,
            &"22".repeat(32),
            &r2,
            &"f2".repeat(32),
            MembershipState::Pending,
            0,
            100,
        );
        assert_eq!(state_of(&r2).unwrap(), "h2");
        assert_eq!(state_of(&r1).unwrap(), "h1");
        assert_eq!(state_of(&r3).unwrap(), "h1");

        store
            .update_cache("h2", |m| {
                m.state = MembershipState::Failed;
                m.failed_reason = Some("allocation_failed: credential already used".into());
            })
            .unwrap();
        assert_eq!(
            state_of(&r2).unwrap(),
            "h2",
            "a failed scoped registration is the scope's answer"
        );
        assert_eq!(state_of(&r1).unwrap(), "h1");

        seed_membership(
            &store,
            "h2b",
            &registry,
            &"23".repeat(32),
            &r2,
            &"f3".repeat(32),
            MembershipState::Active,
            2,
            100,
        );
        assert_eq!(
            state_of(&r2).unwrap(),
            "h2b",
            "the live record beats the failed one"
        );
        seed_membership(
            &store,
            "h2c",
            &registry,
            &"24".repeat(32),
            &r2,
            &"f4".repeat(32),
            MembershipState::Pending,
            0,
            100,
        );
        assert!(state_of(&r2)
            .unwrap_err()
            .contains("registered under this scope"));
        assert!(state_of(&r3)
            .unwrap_err()
            .contains("multiple memberships back this scope"));

        seed_membership(
            &store,
            "hw",
            &registry,
            &"33".repeat(32),
            "",
            &"f5".repeat(32),
            MembershipState::Active,
            3,
            100,
        );
        assert_eq!(state_of(&r3).unwrap(), "hw");
        assert_eq!(
            state_of(&r1).unwrap(),
            "h1",
            "an own record still wins over the registry-wide one"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn proof_candidates_rank_own_then_registry_wide_then_any() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir = std::env::temp_dir().join(format!("rln-candidates-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (_imp, store) = imp_with_store(dir.clone());
        store.unlock("pw").unwrap();
        let registry = format!("logos:local:{}", "7b".repeat(32));
        let scope = "c1".repeat(32);
        seed_membership(
            &store,
            "own",
            &registry,
            &"51".repeat(32),
            &scope,
            &"e1".repeat(32),
            MembershipState::Active,
            1,
            100,
        );
        seed_membership(
            &store,
            "wide",
            &registry,
            &"52".repeat(32),
            "",
            &"e2".repeat(32),
            MembershipState::Active,
            2,
            100,
        );
        seed_membership(
            &store,
            "other",
            &registry,
            &"53".repeat(32),
            &"c2".repeat(32),
            &"e3".repeat(32),
            MembershipState::Active,
            3,
            100,
        );
        let canonical = registry_id::parse(&registry).unwrap().canonical;
        let records = records_for_registry(&store, &registry_id::parse(&registry).unwrap());
        let names = |v: Vec<&MembershipRecord>| {
            let mut n: Vec<String> = v.into_iter().map(|r| r.hash.clone()).collect();
            n.sort();
            n
        };
        assert_eq!(names(scope_candidates(&records, &scope)), ["own"]);
        assert_eq!(
            names(scope_candidates(&records, &"c3".repeat(32))),
            ["wide"],
            "no own record: the registry-wide one"
        );
        store
            .update_cache("wide", |m| m.state = MembershipState::Failed)
            .unwrap();
        let records = records_for_registry(&store, &registry_id::parse(&registry).unwrap());
        assert_eq!(
            names(scope_candidates(&records, &"c3".repeat(32))),
            ["other", "own", "wide"],
            "nothing of its own or wide: every record"
        );
        let _ = canonical;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
