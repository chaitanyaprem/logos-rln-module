use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use crate::error::{ApiError, ErrorKind};
use crate::provider;
use crate::registry_id::CanonicalRegistryId;
use crate::tree::TreeSource;
use crate::util::lock;

const REFRESH_INTERVAL: Duration = Duration::from_secs(10);

const STALE_AFTER_SECS: u64 = 60;

const WINDOW_CAP: usize = 5;

struct Window {
    roots: Vec<[u8; 32]>,
    updated_at: u64,
}

static WINDOWS: Mutex<Option<HashMap<String, Window>>> = Mutex::new(None);
static TRACKED: Mutex<Option<HashMap<String, CanonicalRegistryId>>> = Mutex::new(None);

const NUDGE_MIN_INTERVAL_SECS: u64 = 2;

static LAST_NUDGE_UNIX: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub(crate) fn nudge() {
    use std::sync::atomic::Ordering;
    let now = crate::util::now_unix();
    let last = LAST_NUDGE_UNIX.load(Ordering::Relaxed);
    if now.saturating_sub(last) < NUDGE_MIN_INTERVAL_SECS {
        return;
    }
    if LAST_NUDGE_UNIX
        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        crate::worker::nudge();
    }
}

pub(crate) fn adopt(canonical: &str, roots: Vec<[u8; 32]>) {
    if roots.is_empty() {
        return;
    }
    lock(&WINDOWS).get_or_insert_with(HashMap::new).insert(
        canonical.to_string(),
        Window {
            roots,
            updated_at: crate::util::now_unix(),
        },
    );
}

pub(crate) fn admit(canonical: &str, root: [u8; 32]) {
    let mut guard = lock(&WINDOWS);
    let windows = guard.get_or_insert_with(HashMap::new);
    match windows.get_mut(canonical) {
        Some(w) if w.roots.contains(&root) => {}
        Some(w) => {
            w.roots.insert(0, root);
            w.roots.truncate(WINDOW_CAP);
        }
        None => {
            windows.insert(
                canonical.to_string(),
                Window {
                    roots: vec![root],
                    updated_at: crate::util::now_unix(),
                },
            );
        }
    }
}

pub(crate) fn track(registry: &CanonicalRegistryId) {
    {
        let mut guard = lock(&TRACKED);
        let tracked = guard.get_or_insert_with(HashMap::new);
        if !tracked.contains_key(&registry.canonical) {
            tracked.insert(registry.canonical.clone(), registry.clone());
        }
    }
    ensure_refresher();
}

pub(crate) fn window(canonical: &str) -> Option<Vec<[u8; 32]>> {
    let guard = lock(&WINDOWS);
    let w = guard.as_ref()?.get(canonical)?;
    if crate::util::now_unix().saturating_sub(w.updated_at) > STALE_AFTER_SECS {
        return None;
    }
    Some(w.roots.clone())
}

pub(crate) fn ensure_refresher() {
    crate::worker::ensure(crate::worker::Slot::Refresher, "rln-roots", run_loop);
}

pub(crate) fn run_loop(my_gen: u64) {
    loop {
        if !crate::worker::wait_tick_or_nudge(my_gen, REFRESH_INTERVAL) {
            return;
        }
        refresh_all();
    }
}

pub(crate) fn refresh_all() {
    if crate::worker::is_stopped() {
        return;
    }
    for registry in tracked_snapshot() {
        if let Err(e) = refresh_one(&registry) {
            eprintln!(
                "roots: refresh {} failed: {}",
                registry.canonical, e.message
            );
        }
        if crate::worker::is_stopped() {
            return;
        }
    }
}

#[cfg(test)]
pub(crate) fn tracked_contains(canonical: &str) -> bool {
    lock(&TRACKED)
        .as_ref()
        .is_some_and(|m| m.contains_key(canonical))
}

fn tracked_snapshot() -> Vec<CanonicalRegistryId> {
    lock(&TRACKED)
        .as_ref()
        .map(|m| m.values().cloned().collect())
        .unwrap_or_default()
}

fn refresh_one(registry: &CanonicalRegistryId) -> Result<(), ApiError> {
    let backend = provider::backend_for(&registry.namespace).ok_or_else(|| {
        ApiError::new(
            ErrorKind::UnknownRegistry,
            format!("no registry provider for namespace {}", registry.namespace),
        )
    })?;
    refresh_one_with(registry, backend.tree)
}

fn refresh_one_with(registry: &CanonicalRegistryId, tree: &dyn TreeSource) -> Result<(), ApiError> {
    let roots = tree.valid_roots(registry)?.lifted();
    let mut guard = lock(&WINDOWS);
    let windows = guard.get_or_insert_with(HashMap::new);
    let window = Window {
        roots,
        updated_at: crate::util::now_unix(),
    };
    match windows.get_mut(&registry.canonical) {
        Some(w) => *w = window,
        None => {
            windows.insert(registry.canonical.clone(), window);
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn reset_nudge_for_test() {
    LAST_NUDGE_UNIX.store(0, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) fn set_window_for_test(canonical: &str, roots: Vec<[u8; 32]>, updated_at: u64) {
    lock(&WINDOWS)
        .get_or_insert_with(HashMap::new)
        .insert(canonical.to_string(), Window { roots, updated_at });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cold_registry_has_no_window() {
        assert!(window("logos:local:never-refreshed").is_none());
    }

    #[test]
    fn admit_puts_a_proofs_root_first_and_keeps_the_window_size() {
        let canonical = "logos:local:admit-test";
        admit(canonical, [1; 32]);
        assert_eq!(
            window(canonical),
            Some(vec![[1; 32]]),
            "no window yet: the root alone is one"
        );
        adopt(canonical, (2..=6).map(|n| [n; 32]).collect());
        admit(canonical, [4; 32]);
        assert_eq!(
            window(canonical).unwrap()[0],
            [2; 32],
            "a root already in the window moves nothing"
        );
        admit(canonical, [9; 32]);
        let w = window(canonical).unwrap();
        assert_eq!(w.len(), WINDOW_CAP);
        assert_eq!(
            (w[0], w[4]),
            ([9; 32], [5; 32]),
            "newest first; the oldest drops out"
        );
    }

    #[test]
    fn adopt_installs_a_fresh_window_and_skips_empty() {
        let canonical = "logos:local:adopt-test";
        adopt(canonical, vec![]);
        assert!(
            window(canonical).is_none(),
            "empty adoption must be a no-op"
        );
        adopt(canonical, vec![[7u8; 32]]);
        assert_eq!(window(canonical).expect("adopted window"), vec![[7u8; 32]]);
    }

    #[test]
    fn nudge_is_rate_limited() {
        let _serial = crate::util::lock(&crate::test_support::TEST_GLOBAL_LOCK);
        reset_nudge_for_test();
        let before = crate::worker::nudges_for_test();
        nudge();
        nudge();
        assert_eq!(
            crate::worker::nudges_for_test(),
            before + 1,
            "a second nudge inside the floor must be dropped"
        );
    }

    #[test]
    fn fresh_window_is_served_stale_window_is_cold() {
        let canonical = "logos:local:roots-test";
        let now = crate::util::now_unix();
        set_window_for_test(canonical, vec![[1u8; 32], [2u8; 32]], now);
        let served = window(canonical).expect("fresh window served");
        assert_eq!(served, vec![[1u8; 32], [2u8; 32]]);

        set_window_for_test(canonical, vec![[1u8; 32]], now - STALE_AFTER_SECS - 1);
        assert!(window(canonical).is_none());
    }
}
