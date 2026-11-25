use std::collections::HashMap;
use std::sync::{mpsc, Arc, LazyLock, Mutex};
use std::time::Duration;

use logos_rust_sdk::LogosError;

use crate::error::{ApiError, ErrorKind};
use crate::generated::membership_allocation_client_module::MembershipAllocationClientModuleClient;
use crate::generated::rln_lez_module::RlnLezModuleClient;
use crate::lifecycle::MembershipState;
use crate::registry_id::CanonicalRegistryId;
use crate::tree::{MerkleProof, RootSet, TreeSource};
use crate::util::lock;

const TARGET_MODULE: &str = "rln_lez_module";
const READ_TIMEOUT: Duration = Duration::from_secs(70);
const REGISTER_TIMEOUT: Duration = Duration::from_secs(190);
const FEE_STATE_TIMEOUT: Duration = Duration::from_secs(30);
const REPLY_MARGIN: Duration = Duration::from_secs(10);

static LEZ_CLIENT: Mutex<Option<Arc<RlnLezModuleClient>>> = Mutex::new(None);
static ALLOCATION_CLIENT: LazyLock<MembershipAllocationClientModuleClient> =
    LazyLock::new(MembershipAllocationClientModuleClient::new);

pub(crate) fn init_client() {
    let _ = lez_client();
    let _ = allocation_client();
}

fn lez_client() -> Arc<RlnLezModuleClient> {
    let mut slot = lock(&LEZ_CLIENT);
    Arc::clone(slot.get_or_insert_with(|| Arc::new(RlnLezModuleClient::new())))
}

fn forget_lez_client() {
    *lock(&LEZ_CLIENT) = None;
}

fn await_reply<T: Send + 'static>(
    method: &str,
    timeout: Duration,
    start: impl FnOnce(Box<dyn FnOnce(Result<T, LogosError>) + Send + 'static>),
) -> Result<T, ApiError> {
    let (tx, rx) = mpsc::channel::<Result<T, LogosError>>();
    start(Box::new(move |result| {
        let _ = tx.send(result);
    }));
    match rx.recv_timeout(timeout + REPLY_MARGIN) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => {
            if is_no_client(&e) {
                forget_lez_client();
            }
            eprintln!("membership provider: {method}: {e}");
            Err(provider_failure(method))
        }
        Err(_) => {
            eprintln!("membership provider: {method}: reply channel timed out");
            Err(provider_failure(method))
        }
    }
}

fn is_no_client(e: &LogosError) -> bool {
    matches!(e, LogosError::Other(msg) if msg.starts_with("Failed to create protocol client"))
}

fn read_reply(method: &str, reply: Result<String, ApiError>) -> Result<String, ApiError> {
    let value = reply?;
    if value.is_empty() {
        return Err(provider_failure(method));
    }
    Ok(value)
}

fn fold_reply(method: &str, result: Result<String, LogosError>) -> Result<String, ApiError> {
    match result {
        Ok(value) if !value.is_empty() => Ok(value),
        Ok(_) => Err(provider_failure(method)),
        Err(e) => {
            eprintln!("membership provider: {method}: {e}");
            Err(provider_failure(method))
        }
    }
}

fn dispatch_recorded(
    method: &'static str,
    on_done: RegisterCallback,
    start: impl FnOnce(Box<dyn FnOnce(Result<String, LogosError>) + Send + 'static>),
) -> Result<(), ApiError> {
    struct Pending {
        dispatched: bool,
        on_done: Option<RegisterCallback>,
        sync_result: Option<Result<String, ApiError>>,
    }
    let state = Arc::new(Mutex::new(Pending {
        dispatched: false,
        on_done: Some(on_done),
        sync_result: None,
    }));
    let cb_state = Arc::clone(&state);
    start(Box::new(move |result| {
        let outcome = fold_reply(method, result);
        let mut st = lock(&cb_state);
        if !st.dispatched {
            st.sync_result = Some(outcome);
            return;
        }
        let cb = st.on_done.take();
        drop(st);
        if let Some(cb) = cb {
            cb(outcome);
        }
    }));
    let mut st = lock(&state);
    st.dispatched = true;
    match st.sync_result.take() {
        Some(Err(e)) => {
            st.on_done = None;
            Err(e)
        }
        Some(Ok(value)) => {
            let cb = st.on_done.take();
            drop(st);
            if let Some(cb) = cb {
                cb(Ok(value));
            }
            Ok(())
        }
        None => Ok(()),
    }
}

static NETWORK_GATE: std::sync::LazyLock<Mutex<HashMap<String, Result<(), String>>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Gate {
    Accepted,
    Refused(String),
    Retry,
}

pub(crate) fn interpret_use_network(raw: &str) -> Gate {
    let Ok(mut v) = serde_json::from_str::<serde_json::Value>(raw) else {
        return Gate::Retry;
    };
    match v.get("accepted").and_then(serde_json::Value::as_bool) {
        Some(true) => Gate::Accepted,
        Some(false) if v.get("retry").and_then(serde_json::Value::as_bool) == Some(false) => {
            Gate::Refused(match v.get_mut("detail").map(serde_json::Value::take) {
                Some(serde_json::Value::String(detail)) => detail,
                _ => "refused".to_owned(),
            })
        }
        _ => Gate::Retry,
    }
}

fn remember(gate: &mut HashMap<String, Result<(), String>>, reference: &str, answer: Gate) {
    match answer {
        Gate::Accepted => {
            gate.insert(reference.to_owned(), Ok(()));
        }
        Gate::Refused(detail) => {
            gate.insert(reference.to_owned(), Err(detail));
        }
        Gate::Retry => {}
    }
}

fn network_refused(registry: &CanonicalRegistryId, detail: &str) -> ApiError {
    ApiError::new(
        ErrorKind::UnknownRegistry,
        format!(
            "{}: {TARGET_MODULE} will not serve network '{}': {detail}",
            registry.canonical, registry.reference
        ),
    )
}

pub(crate) fn ensure_network(registry: &CanonicalRegistryId) -> Result<(), ApiError> {
    match lock(&NETWORK_GATE).get(&registry.reference) {
        Some(Ok(())) => return Ok(()),
        Some(Err(detail)) => return Err(network_refused(registry, detail)),
        None => {}
    }
    let client = lez_client();
    let raw = await_reply("use_network", READ_TIMEOUT, |done| {
        client.use_network_async_with_timeout(&registry.reference, READ_TIMEOUT, done)
    })?;
    match interpret_use_network(&raw) {
        Gate::Accepted => {
            remember(
                &mut lock(&NETWORK_GATE),
                &registry.reference,
                Gate::Accepted,
            );
            Ok(())
        }
        Gate::Refused(detail) => {
            let err = network_refused(registry, &detail);
            remember(
                &mut lock(&NETWORK_GATE),
                &registry.reference,
                Gate::Refused(detail),
            );
            eprintln!("membership provider: {}", err.message);
            Err(err)
        }
        Gate::Retry => Err(ApiError::new(
            ErrorKind::NotReady,
            format!(
                "{TARGET_MODULE} cannot select network '{}' yet (its wallet home is not resolved)",
                registry.reference
            ),
        )),
    }
}

fn networks_in_order(tracked: &[String]) -> Vec<CanonicalRegistryId> {
    let mut out: Vec<CanonicalRegistryId> = Vec::new();
    for raw in tracked {
        let Ok(registry) = crate::registry_id::parse(raw) else {
            continue;
        };
        if registry.namespace != "logos" || out.iter().any(|r| r.reference == registry.reference) {
            continue;
        }
        out.push(registry);
    }
    out
}

pub(crate) fn select_networks(tracked: &[String]) {
    for registry in networks_in_order(tracked) {
        match ensure_network(&registry) {
            Ok(()) => eprintln!(
                "membership start: network '{}' selected",
                registry.reference
            ),
            Err(e) => eprintln!(
                "membership start: network '{}': {}",
                registry.reference, e.message
            ),
        }
    }
}

pub(crate) const ALLOCATION_REQUEST_TIMEOUT: Duration = Duration::from_secs(340);

fn allocation_client() -> &'static MembershipAllocationClientModuleClient {
    LazyLock::force(&ALLOCATION_CLIENT)
}

pub(crate) fn allocation_request_async(
    args_json: &str,
    on_done: RegisterCallback,
) -> Result<(), ApiError> {
    #[cfg(test)]
    if let Some(allocator) = lock(&crate::test_support::INSTALLED_ALLOCATOR).as_ref() {
        let args = serde_json::from_str(args_json).unwrap_or(serde_json::Value::Null);
        let reply = allocator("request", &args)?;
        on_done(Ok(reply));
        return Ok(());
    }
    let client = allocation_client();
    dispatch_recorded("request", on_done, |done| {
        client.request_async_with_timeout(args_json, ALLOCATION_REQUEST_TIMEOUT, done)
    })
}

pub(crate) const ALLOCATOR_DISCOVER_TIMEOUT: Duration = Duration::from_secs(130);

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DiscoverReply {
    Listed {
        candidates: usize,
        joined: bool,
        peers: u64,
    },
    Unavailable(String),
    Failed(String),
}

impl DiscoverReply {
    fn from_reply(raw: &str) -> Result<Self, ApiError> {
        let mut v: serde_json::Value = serde_json::from_str(raw).map_err(|e| {
            ApiError::new(ErrorKind::ProviderFailure, format!("discover parse: {e}"))
        })?;
        if v.get("kind").is_some() {
            let error = match v.get_mut("error").map(serde_json::Value::take) {
                Some(serde_json::Value::String(s)) => s,
                Some(other) => other.to_string(),
                None => String::new(),
            };
            return Ok(match v.get("kind").and_then(serde_json::Value::as_str) {
                Some("no_provider") => DiscoverReply::Unavailable(error),
                _ => DiscoverReply::Failed(error),
            });
        }
        let listed = || {
            Some(DiscoverReply::Listed {
                candidates: v.get("candidates")?.as_array()?.len(),
                joined: v.get("joined")?.as_bool()?,
                peers: v.get("peers")?.as_u64()?,
            })
        };
        listed().ok_or_else(|| {
            ApiError::new(
                ErrorKind::ProviderFailure,
                format!("discover reply without candidates, joined and peers: {raw}"),
            )
        })
    }
}

pub(crate) fn allocator_discover_async(
    registry: &CanonicalRegistryId,
    auth_type: Option<&str>,
    bootstrap_peers: &[String],
) -> Result<DiscoverReply, ApiError> {
    let mut args = serde_json::json!({ "config": registry.account });
    if let Some(t) = auth_type {
        args["authType"] = t.into();
    }
    if !bootstrap_peers.is_empty() {
        args["bootstrapPeers"] = bootstrap_peers.into();
    }
    #[cfg(test)]
    if let Some(allocator) = lock(&crate::test_support::INSTALLED_ALLOCATOR).as_ref() {
        return DiscoverReply::from_reply(&allocator("discover", &args)?);
    }
    let client = allocation_client();
    let args_json = args.to_string();
    let raw = read_reply(
        "discover",
        await_reply("discover", ALLOCATOR_DISCOVER_TIMEOUT, |done| {
            client.discover_async_with_timeout(&args_json, ALLOCATOR_DISCOVER_TIMEOUT, done)
        }),
    )?;
    DiscoverReply::from_reply(&raw)
}

pub(crate) struct ProviderMembership {
    pub(crate) state: MembershipState,
    pub(crate) leaf_index: Option<u64>,
    pub(crate) rate_limit: u64,
}

pub(crate) type RegisterCallback = Box<dyn FnOnce(Result<String, ApiError>) + Send>;

pub(crate) trait Registry: Send + Sync {
    fn get_membership(
        &self,
        registry: &CanonicalRegistryId,
        id_commitment_hex: &str,
    ) -> Result<Option<ProviderMembership>, ApiError>;

    fn register_async(
        &self,
        registry: &CanonicalRegistryId,
        id_commitment_hex: &str,
        rate_limit: u64,
        on_done: RegisterCallback,
    ) -> Result<(), ApiError>;

    fn get_registry_bounds(
        &self,
        registry: &CanonicalRegistryId,
    ) -> Result<serde_json::Value, ApiError>;

    fn check_registry(&self, registry: &CanonicalRegistryId)
        -> Result<serde_json::Value, ApiError>;
}

pub(crate) trait Payer: Send + Sync {
    fn wallet_status(&self) -> Result<serde_json::Value, ApiError>;

    fn payer_balance(&self) -> Result<u128, ApiError>;

    fn fee_state(&self) -> Result<serde_json::Value, ApiError>;
}

#[derive(Clone, Copy)]
pub(crate) struct Backend {
    pub tree: &'static dyn TreeSource,
    pub registry: &'static dyn Registry,
    pub payer: &'static dyn Payer,
}

const REGISTRY_PROBLEMS: [&str; 5] = [
    "not_deployed",
    "wrong_generation",
    "unexpected_layout",
    "network_conflict",
    "invalid",
];

pub(crate) fn registry_problem(
    prov: &dyn Registry,
    registry: &CanonicalRegistryId,
) -> Option<String> {
    let check = prov.check_registry(registry).ok()?;
    registry_problem_from(&registry.canonical, &check)
}

fn registry_problem_from(canonical: &str, check: &serde_json::Value) -> Option<String> {
    let status = check.get("status")?.as_str()?;
    if !REGISTRY_PROBLEMS.contains(&status) {
        return None;
    }
    let detail = check
        .get("detail")
        .and_then(|d| d.as_str())
        .unwrap_or(status);
    let at = match (
        check.get("sequencer").and_then(|s| s.as_str()),
        check.get("chain_head").and_then(serde_json::Value::as_u64),
    ) {
        (Some(seq), Some(head)) => format!(" on {seq} (chain at block {head})"),
        (Some(seq), None) => format!(" on {seq}"),
        _ => String::new(),
    };
    Some(format!(
        "registry {canonical} is not usable{at}: {status} — {detail}"
    ))
}

fn explained(prov: &dyn Registry, registry: &CanonicalRegistryId, e: ApiError) -> ApiError {
    if e.kind != ErrorKind::ProviderFailure {
        return e;
    }
    match registry_problem(prov, registry) {
        Some(msg) => ApiError::new(ErrorKind::UnknownRegistry, msg),
        None => e,
    }
}

static LEZ_RLN: LezRlnProvider = LezRlnProvider;

pub(crate) fn backend_for(namespace: &str) -> Option<Backend> {
    match namespace {
        "logos" => Some(Backend {
            tree: &LEZ_RLN,
            registry: &LEZ_RLN,
            payer: &LEZ_RLN,
        }),
        #[cfg(test)]
        "fakens" => *lock(&crate::test_support::INSTALLED_BACKEND),
        _ => None,
    }
}

struct LezRlnProvider;

fn provider_failure(method: &str) -> ApiError {
    ApiError::new(
        ErrorKind::ProviderFailure,
        format!("{TARGET_MODULE}.{method} failed (empty reply)"),
    )
}

fn parse_membership_reply(raw: &str) -> Result<Option<ProviderMembership>, ApiError> {
    let fault = |msg: String| ApiError::new(ErrorKind::ProviderFailure, msg);
    let v: serde_json::Value =
        serde_json::from_str(raw).map_err(|e| fault(format!("get_membership reply parse: {e}")))?;
    let registered = v
        .get("registered")
        .and_then(|x| x.as_bool())
        .ok_or_else(|| fault("get_membership: reply carries no registered flag".to_string()))?;
    if !registered {
        return Ok(None);
    }
    let required = |key: &str| {
        v.get(key)
            .and_then(|x| x.as_u64())
            .ok_or_else(|| fault(format!("get_membership: registered member missing {key}")))
    };
    Ok(Some(ProviderMembership {
        state: <MembershipState as serde::Deserialize>::deserialize(
            v.get("state").unwrap_or(&serde_json::Value::Null),
        )
        .map_err(|_| fault("get_membership: unrecognized state".to_string()))?,
        leaf_index: Some(required("leaf_index")?),
        rate_limit: required("rate_limit")?,
    }))
}

impl Registry for LezRlnProvider {
    fn get_membership(
        &self,
        registry: &CanonicalRegistryId,
        id_commitment_hex: &str,
    ) -> Result<Option<ProviderMembership>, ApiError> {
        ensure_network(registry)?;
        let client = lez_client();
        let raw = read_reply(
            "get_membership",
            await_reply("get_membership", READ_TIMEOUT, |done| {
                client.get_membership_async_with_timeout(
                    &registry.account,
                    id_commitment_hex,
                    READ_TIMEOUT,
                    done,
                )
            }),
        )
        .map_err(|e| explained(self, registry, e))?;
        parse_membership_reply(&raw)
    }

    fn register_async(
        &self,
        registry: &CanonicalRegistryId,
        id_commitment_hex: &str,
        rate_limit: u64,
        on_done: RegisterCallback,
    ) -> Result<(), ApiError> {
        let rate_limit = i64::try_from(rate_limit).map_err(|_| {
            ApiError::new(
                ErrorKind::InvalidArgument,
                "rate_limit exceeds the wire's i64",
            )
        })?;

        ensure_network(registry)?;
        let client = lez_client();
        dispatch_recorded("register_member", on_done, |done| {
            client.register_member_async_with_timeout(
                &registry.account,
                "",
                id_commitment_hex,
                rate_limit,
                REGISTER_TIMEOUT,
                done,
            )
        })
    }

    fn get_registry_bounds(
        &self,
        registry: &CanonicalRegistryId,
    ) -> Result<serde_json::Value, ApiError> {
        ensure_network(registry)?;
        let client = lez_client();
        let raw = read_reply(
            "get_registry_bounds",
            await_reply("get_registry_bounds", READ_TIMEOUT, |done| {
                client.get_registry_bounds_async_with_timeout(&registry.account, READ_TIMEOUT, done)
            }),
        )
        .map_err(|e| explained(self, registry, e))?;
        serde_json::from_str::<serde_json::Value>(&raw).map_err(|e| {
            ApiError::new(
                ErrorKind::ProviderFailure,
                format!("bounds reply parse: {e}"),
            )
        })
    }

    fn check_registry(
        &self,
        registry: &CanonicalRegistryId,
    ) -> Result<serde_json::Value, ApiError> {
        ensure_network(registry)?;
        let client = lez_client();
        let raw = read_reply(
            "check_registry",
            await_reply("check_registry", READ_TIMEOUT, |done| {
                client.check_registry_async_with_timeout(&registry.account, READ_TIMEOUT, done)
            }),
        )?;
        serde_json::from_str::<serde_json::Value>(&raw).map_err(|e| {
            ApiError::new(
                ErrorKind::ProviderFailure,
                format!("check_registry parse: {e}"),
            )
        })
    }
}

impl Payer for LezRlnProvider {
    fn wallet_status(&self) -> Result<serde_json::Value, ApiError> {
        let client = lez_client();
        let raw = read_reply(
            "wallet_status",
            await_reply("wallet_status", READ_TIMEOUT, |done| {
                client.wallet_status_async_with_timeout(READ_TIMEOUT, done)
            }),
        )?;
        serde_json::from_str::<serde_json::Value>(&raw).map_err(|e| {
            ApiError::new(
                ErrorKind::ProviderFailure,
                format!("wallet_status parse: {e}"),
            )
        })
    }

    fn payer_balance(&self) -> Result<u128, ApiError> {
        let client = lez_client();
        let raw = read_reply(
            "get_native_balance",
            await_reply("get_native_balance", READ_TIMEOUT, |done| {
                client.get_native_balance_async_with_timeout("", READ_TIMEOUT, done)
            }),
        )?;
        let parsed: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
            ApiError::new(
                ErrorKind::ProviderFailure,
                format!("balance reply parse: {e}"),
            )
        })?;
        parsed
            .get("balance")
            .and_then(|b| b.as_str())
            .and_then(|b| b.parse::<u128>().ok())
            .ok_or_else(|| {
                ApiError::new(
                    ErrorKind::ProviderFailure,
                    "balance reply has no decimal balance",
                )
            })
    }

    fn fee_state(&self) -> Result<serde_json::Value, ApiError> {
        let client = lez_client();
        let raw = read_reply(
            "get_fee_state",
            await_reply("get_fee_state", FEE_STATE_TIMEOUT, |done| {
                client.get_fee_state_async_with_timeout(FEE_STATE_TIMEOUT, done)
            }),
        )?;
        serde_json::from_str::<serde_json::Value>(&raw)
            .map_err(|e| ApiError::new(ErrorKind::ProviderFailure, format!("fee state parse: {e}")))
    }
}

impl TreeSource for LezRlnProvider {
    fn merkle_proof(
        &self,
        registry: &CanonicalRegistryId,
        leaf_index: u64,
    ) -> Result<MerkleProof, ApiError> {
        ensure_network(registry)?;
        let client = lez_client();
        let indices = format!("[{leaf_index}]");
        let raw = read_reply(
            "get_merkle_proofs",
            await_reply("get_merkle_proofs", READ_TIMEOUT, |done| {
                client.get_merkle_proofs_async_with_timeout(
                    &registry.account,
                    &indices,
                    READ_TIMEOUT,
                    done,
                )
            }),
        )
        .map_err(|e| explained(self, registry, e))?;
        let v: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
            ApiError::new(
                ErrorKind::ProviderFailure,
                format!("proofs reply parse: {e}"),
            )
        })?;
        let first = v.as_array().and_then(|arr| arr.first()).ok_or_else(|| {
            ApiError::new(
                ErrorKind::ProviderFailure,
                "empty proof array (leaf out of range?)",
            )
        })?;
        MerkleProof::from_provider_json(first)
    }

    fn valid_roots(&self, registry: &CanonicalRegistryId) -> Result<RootSet, ApiError> {
        ensure_network(registry)?;
        let client = lez_client();
        let raw = read_reply(
            "get_valid_roots",
            await_reply("get_valid_roots", READ_TIMEOUT, |done| {
                client.get_valid_roots_async_with_timeout(&registry.account, READ_TIMEOUT, done)
            }),
        )
        .map_err(|e| explained(self, registry, e))?;
        let parsed: serde_json::Value = serde_json::from_str(&raw).map_err(|e| {
            ApiError::new(
                ErrorKind::ProviderFailure,
                format!("roots reply parse: {e}"),
            )
        })?;
        RootSet::from_provider_json(&parsed)
    }
}

#[cfg(test)]
pub(crate) mod fake {
    use super::*;

    fn unanswered(what: &str) -> ApiError {
        ApiError::new(
            ErrorKind::ProviderFailure,
            format!("fake {what}: no answer configured"),
        )
    }

    #[derive(Default)]
    pub(crate) struct FakeTree {
        pub proof: Option<serde_json::Value>,
        pub roots: Option<serde_json::Value>,
    }

    impl TreeSource for FakeTree {
        fn merkle_proof(&self, _: &CanonicalRegistryId, _: u64) -> Result<MerkleProof, ApiError> {
            MerkleProof::from_provider_json(
                self.proof
                    .as_ref()
                    .ok_or_else(|| unanswered("merkle_proof"))?,
            )
        }

        fn valid_roots(&self, _: &CanonicalRegistryId) -> Result<RootSet, ApiError> {
            RootSet::from_provider_json(
                self.roots
                    .as_ref()
                    .ok_or_else(|| unanswered("valid_roots"))?,
            )
        }
    }

    pub(crate) static REGISTER_CALLS: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);

    #[derive(Default)]
    pub(crate) struct FakeRegistry {
        pub membership: Option<Option<(MembershipState, Option<u64>, u64)>>,
        pub bounds: Option<serde_json::Value>,
    }

    impl Registry for FakeRegistry {
        fn get_membership(
            &self,
            _: &CanonicalRegistryId,
            _: &str,
        ) -> Result<Option<ProviderMembership>, ApiError> {
            let answer = self
                .membership
                .ok_or_else(|| unanswered("get_membership"))?;
            Ok(
                answer.map(|(state, leaf_index, rate_limit)| ProviderMembership {
                    state,
                    leaf_index,
                    rate_limit,
                }),
            )
        }

        fn register_async(
            &self,
            _: &CanonicalRegistryId,
            _: &str,
            _: u64,
            _: RegisterCallback,
        ) -> Result<(), ApiError> {
            REGISTER_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(unanswered("register_async"))
        }

        fn get_registry_bounds(
            &self,
            _: &CanonicalRegistryId,
        ) -> Result<serde_json::Value, ApiError> {
            self.bounds
                .clone()
                .ok_or_else(|| unanswered("get_registry_bounds"))
        }

        fn check_registry(&self, _: &CanonicalRegistryId) -> Result<serde_json::Value, ApiError> {
            Ok(serde_json::json!({ "status": "ok" }))
        }
    }

    #[derive(Default)]
    pub(crate) struct FakePayer {
        pub balance: Option<u128>,
    }

    impl Payer for FakePayer {
        fn wallet_status(&self) -> Result<serde_json::Value, ApiError> {
            Ok(
                serde_json::json!({ "detail": "", "payer": "fake", "ready": true, "state": "ready" }),
            )
        }

        fn payer_balance(&self) -> Result<u128, ApiError> {
            self.balance.ok_or_else(|| unanswered("payer_balance"))
        }

        fn fee_state(&self) -> Result<serde_json::Value, ApiError> {
            Err(unanswered("fee_state"))
        }
    }

    pub(crate) fn backend(tree: FakeTree, registry: FakeRegistry, payer: FakePayer) -> Backend {
        Backend {
            tree: Box::leak(Box::new(tree)),
            registry: Box::leak(Box::new(registry)),
            payer: Box::leak(Box::new(payer)),
        }
    }
}

#[cfg(test)]
mod lp_test_transport {
    #![expect(
        unsafe_code,
        reason = "test-only link targets for the protocol archive symbols; each only returns \"no client\""
    )]
    use std::ffi::{c_char, c_int, c_void};

    #[no_mangle]
    pub extern "C" fn lp_client_create(
        _target_module: *const c_char,
        _origin_module: *const c_char,
        _target_transport_json: *const c_char,
        _capability_transport_json: *const c_char,
    ) -> *mut c_void {
        std::ptr::null_mut()
    }

    #[no_mangle]
    pub extern "C" fn lp_client_destroy(_client: *mut c_void) {}

    #[no_mangle]
    pub extern "C" fn lp_invoke(
        _client: *mut c_void,
        _method: *const c_char,
        _args_json: *const c_char,
        _timeout_ms: c_int,
        _out_result_json: *mut *mut c_char,
        _out_error_json: *mut *mut c_char,
    ) -> c_int {
        -3
    }

    #[no_mangle]
    pub extern "C" fn lp_invoke_async(
        _client: *mut c_void,
        _method: *const c_char,
        _args_json: *const c_char,
        _timeout_ms: c_int,
        _cb: Option<extern "C" fn(c_int, *const c_char, *mut c_void)>,
        _user_data: *mut c_void,
    ) -> c_int {
        -3
    }

    #[no_mangle]
    pub extern "C" fn lp_string_free(_s: *mut c_char) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry_id;

    #[test]
    fn a_discover_reply_is_read_from_its_fields() {
        assert_eq!(
            DiscoverReply::from_reply(
                r#"{"serviceId":"s","candidates":[{"peerId":"a","addrs":["/m"]}],"joined":true,"peers":3}"#
            )
            .unwrap(),
            DiscoverReply::Listed {
                candidates: 1,
                joined: true,
                peers: 3
            }
        );
        assert_eq!(
            DiscoverReply::from_reply(r#"{"kind":"no_provider","error":"libp2p down"}"#).unwrap(),
            DiscoverReply::Unavailable("libp2p down".into())
        );
        assert_eq!(
            DiscoverReply::from_reply(
                r#"{"kind":"invalid_request","error":"discovery unavailable"}"#
            )
            .unwrap(),
            DiscoverReply::Failed("discovery unavailable".into()),
            "the kind decides, not the text"
        );
        for raw in [r#"{"candidates":[]}"#, "not json"] {
            assert_eq!(
                DiscoverReply::from_reply(raw).unwrap_err().kind,
                ErrorKind::ProviderFailure,
                "{raw}"
            );
        }
    }

    #[test]
    fn a_missing_registry_is_explained_and_an_outage_is_not() {
        let gone = serde_json::json!({
            "status": "not_deployed",
            "detail": "config account ab holds no program state",
            "sequencer": "https://testnet.lez.logos.co/",
            "chain_head": 12,
        });
        let msg = registry_problem_from("logos:testnet:ab", &gone).expect("explained");
        assert!(msg.contains("not_deployed"), "{msg}");
        assert!(
            msg.contains("https://testnet.lez.logos.co/ (chain at block 12)"),
            "{msg}"
        );
        for status in ["ok", "unreachable", "something_new"] {
            let check = serde_json::json!({"status": status});
            assert!(
                registry_problem_from("logos:testnet:ab", &check).is_none(),
                "{status}"
            );
        }
        assert!(registry_problem_from("logos:testnet:ab", &serde_json::json!({})).is_none());
    }

    #[test]
    fn a_reply_without_registered_is_a_provider_failure() {
        for raw in [
            r#"{"state":"active","leaf_index":7,"rate_limit":100}"#,
            r#"{"registered":"false"}"#,
            r#"{"registered":null}"#,
            r#"{}"#,
        ] {
            let Err(err) = parse_membership_reply(raw) else {
                panic!("{raw} must not read as an authoritative absence");
            };
            assert_eq!(err.kind, ErrorKind::ProviderFailure, "{raw}");
            assert!(err.message.contains("registered"), "{raw}: {}", err.message);
        }
    }

    #[test]
    fn an_authoritative_absence_still_reads_as_not_registered() {
        let Ok(pm) = parse_membership_reply(r#"{"registered":false}"#) else {
            panic!("an explicit registered:false is a valid answer");
        };
        assert!(pm.is_none());
    }

    #[test]
    fn a_registered_member_needs_every_contract_field() {
        let Ok(Some(pm)) = parse_membership_reply(
            r#"{"registered":true,"state":"active","leaf_index":0,"rate_limit":100}"#,
        ) else {
            panic!("a complete reply must decode");
        };
        assert_eq!(pm.leaf_index, Some(0), "leaf 0 is a valid leaf");
        assert_eq!(pm.rate_limit, 100);

        for raw in [
            r#"{"registered":true,"state":"active","rate_limit":100}"#,
            r#"{"registered":true,"state":"active","leaf_index":7}"#,
            r#"{"registered":true,"leaf_index":7,"rate_limit":100}"#,
        ] {
            let Err(err) = parse_membership_reply(raw) else {
                panic!("{raw} is missing a contract field");
            };
            assert_eq!(err.kind, ErrorKind::ProviderFailure, "{raw}");
        }
    }

    #[test]
    fn stubbed_transport_degrades_to_provider_failure() {
        let registry = registry_id::parse(&format!("logos:local:{}", "ab".repeat(32))).unwrap();
        let backend = backend_for("logos").unwrap();
        assert!(backend
            .registry
            .get_membership(&registry, &"11".repeat(32))
            .is_err());
        assert!(backend.tree.merkle_proof(&registry, 0).is_err());
        assert!(backend.tree.valid_roots(&registry).is_err());
        assert!(backend.payer.fee_state().is_err());
        let fired = Arc::new(Mutex::new(false));
        let seen = Arc::clone(&fired);
        let err = backend
            .registry
            .register_async(
                &registry,
                &"11".repeat(32),
                300,
                Box::new(move |_| {
                    *lock(&seen) = true;
                }),
            )
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::ProviderFailure);
        assert!(
            !*lock(&fired),
            "a synchronous failure must not also reach on_done"
        );
    }

    #[test]
    fn a_register_gets_past_validation() {
        let registry = registry_id::parse(&format!("logos:local:{}", "ab".repeat(32))).unwrap();
        let backend = backend_for("logos").unwrap();
        let err = backend
            .registry
            .register_async(&registry, &"11".repeat(32), 300, Box::new(|_| {}))
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::ProviderFailure);
    }

    #[test]
    fn use_network_replies_decode_to_a_gate() {
        assert_eq!(
            interpret_use_network(
                r#"{"accepted":true,"detail":"","network":"devnet","retry":false,"source":"table"}"#
            ),
            Gate::Accepted
        );
        assert_eq!(
            interpret_use_network(
                r#"{"accepted":false,"detail":"bound to devnet","network":"devnet","retry":false,"source":""}"#
            ),
            Gate::Refused("bound to devnet".to_owned())
        );
        assert_eq!(
            interpret_use_network(r#"{"accepted":false,"detail":"","retry":true}"#),
            Gate::Retry
        );
        for raw in [
            "",
            "garbage",
            "[]",
            r#"{"accepted":false}"#,
            r#"{"retry":false}"#,
        ] {
            assert_eq!(interpret_use_network(raw), Gate::Retry, "{raw:?}");
        }
    }

    #[test]
    fn only_definitive_answers_are_remembered() {
        let mut gate = HashMap::new();
        remember(&mut gate, "devnet", Gate::Retry);
        assert!(gate.is_empty(), "a retry must be asked again");
        remember(&mut gate, "devnet", Gate::Accepted);
        remember(
            &mut gate,
            "testnet",
            Gate::Refused("bound to devnet".to_owned()),
        );
        assert_eq!(gate.get("devnet"), Some(&Ok(())));
        assert_eq!(
            gate.get("testnet"),
            Some(&Err("bound to devnet".to_owned()))
        );
    }

    #[test]
    fn networks_are_selected_in_configuration_order() {
        let a = "aa".repeat(32);
        let b = "bb".repeat(32);
        let c = "cc".repeat(32);
        let tracked = vec![
            format!("logos:testnet:{a}"),
            format!("eip155:1:0x{a}"),
            format!("logos:devnet:{b}"),
            format!("logos:testnet:{c}"),
            "not a registry id".to_owned(),
        ];
        let picked: Vec<String> = networks_in_order(&tracked)
            .into_iter()
            .map(|r| r.canonical)
            .collect();
        assert_eq!(
            picked,
            vec![format!("logos:testnet:{a}"), format!("logos:devnet:{b}")]
        );
    }

    #[test]
    fn a_remembered_refusal_is_unknown_registry() {
        let registry =
            registry_id::parse(&format!("logos:gate-refused-test:{}", "ab".repeat(32))).unwrap();
        lock(&NETWORK_GATE).insert(
            registry.reference.clone(),
            Err("bound to devnet".to_owned()),
        );
        let err = ensure_network(&registry).unwrap_err();
        assert_eq!(err.kind, ErrorKind::UnknownRegistry);
        assert!(err.message.contains("bound to devnet"), "{}", err.message);
        let backend = backend_for("logos").unwrap();
        assert_eq!(
            backend
                .tree
                .valid_roots(&registry)
                .err()
                .expect("refused")
                .kind,
            ErrorKind::UnknownRegistry
        );
        lock(&NETWORK_GATE).remove(&registry.reference);
    }

    #[test]
    fn unknown_namespace_has_no_provider() {
        assert!(backend_for("eip155").is_none());
    }
}
