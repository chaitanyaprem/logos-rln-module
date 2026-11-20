use std::sync::{Arc, Mutex};

use crate::error::ApiError;
use crate::lifecycle::{MembershipState, StoredCredential};
use crate::provider::Backend;
use crate::sealed_store::{self, store::Store};
use crate::util::now_unix;
use crate::RlnModuleImpl;

pub(crate) static TEST_GLOBAL_LOCK: Mutex<()> = Mutex::new(());

pub(crate) static INSTALLED_BACKEND: Mutex<Option<Backend>> = Mutex::new(None);

pub(crate) fn install_backend(backend: Backend) {
    *crate::util::lock(&INSTALLED_BACKEND) = Some(backend);
}

pub(crate) type FakeAllocator =
    Box<dyn Fn(&str, &serde_json::Value) -> Result<String, ApiError> + Send + Sync>;

pub(crate) static INSTALLED_ALLOCATOR: Mutex<Option<FakeAllocator>> = Mutex::new(None);

pub(crate) fn install_allocator(allocator: FakeAllocator) {
    *crate::util::lock(&INSTALLED_ALLOCATOR) = Some(allocator);
}

pub(crate) struct PublishedStore(Arc<Store>);

impl std::ops::Deref for PublishedStore {
    type Target = Arc<Store>;
    fn deref(&self) -> &Arc<Store> {
        &self.0
    }
}

impl Drop for PublishedStore {
    fn drop(&mut self) {
        sealed_store::store::withdraw(&self.0);
    }
}

pub(crate) fn publish_test_store(dir: std::path::PathBuf) -> PublishedStore {
    sealed_store::store::publish(None);
    let store = Store::open(dir).expect("test store open");
    sealed_store::store::publish(Some(&store));
    PublishedStore(store)
}

pub(crate) fn no_store() -> Result<Arc<Store>, ApiError> {
    Err(ApiError::internal(sealed_store::store::UNINIT_MSG))
}

pub(crate) fn opts_arr(pairs: &[(&str, &str)]) -> String {
    serde_json::Value::Array(
        pairs
            .iter()
            .map(|(k, v)| serde_json::json!({"key": k, "value": v}))
            .collect(),
    )
    .to_string()
}

#[expect(
    clippy::too_many_arguments,
    reason = "test seeding helper: each argument is one persisted field of the row"
)]
pub(crate) fn seed_membership(
    store: &Store,
    hash: &str,
    registry: &str,
    commitment_hex: &str,
    rln_identifier: &str,
    secret_hex: &str,
    state: MembershipState,
    leaf_index: u64,
    rate_limit: u64,
) {
    let identity = sealed_store::format::IdentityBlock {
        registry_id: registry.to_string(),
        rln_identifier: rln_identifier.to_string(),
        identity_commitment: commitment_hex.to_string(),
        submitted_at: now_unix(),
    };
    let credential = StoredCredential {
        identity_commitment: commitment_hex.to_string(),
        identity_secret_hash: secret_hex.to_string(),
        registry_id: registry.to_string(),
    };
    store
        .insert(hash, identity, &credential, 100)
        .expect("seed insert");
    store
        .update_cache(hash, |m| {
            m.state = state;
            m.leaf_index = Some(leaf_index);
            m.rate_limit = Some(rate_limit);
        })
        .expect("seed cache");
}

pub(crate) struct TestImp {
    imp: RlnModuleImpl,
    _store: PublishedStore,
}

impl std::ops::Deref for TestImp {
    type Target = RlnModuleImpl;
    fn deref(&self) -> &RlnModuleImpl {
        &self.imp
    }
}

pub(crate) fn imp_with_store(dir: std::path::PathBuf) -> (TestImp, Arc<Store>) {
    let imp = RlnModuleImpl::default();
    sealed_store::store::open_current(dir);
    let store = imp.store().expect("test store open");
    let guard = PublishedStore(Arc::clone(&store));
    (TestImp { imp, _store: guard }, store)
}
