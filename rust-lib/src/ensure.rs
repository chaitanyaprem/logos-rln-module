use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::delegated::DelegatedOptions;
use crate::error::{ApiError, ErrorKind};
use crate::lifecycle::{granted_by, take_discovery_miss, MembershipState};
use crate::provider::DiscoverReply;
use crate::register::register_scoped;
use crate::registry_id;
use crate::scope::{backend_of, records_for_registry};
use crate::worker;

const REGISTRY_WIDE: &str = "";

const WALLET_WAIT_NOTICE: Duration = Duration::from_secs(300);

const TICK: Duration = Duration::from_secs(5);

const CONFIRM_HOLD: Duration = Duration::from_secs(900);

const DELEGATE_HOLD_MARGIN: Duration = Duration::from_secs(80);

const DELEGATE_HOLD: Duration = crate::provider::ALLOCATION_REQUEST_TIMEOUT
    .saturating_add(Duration::from_secs(
        crate::lifecycle::CONFIRMATION_WINDOW_SECS,
    ))
    .saturating_add(DELEGATE_HOLD_MARGIN);
const ATTRIBUTION_HOLD: Duration = Duration::from_secs(20);

pub(crate) static DISCOVERY_RETRY_SECS: AtomicU64 = AtomicU64::new(60);

fn discovery_retry_every() -> Duration {
    Duration::from_secs(DISCOVERY_RETRY_SECS.load(Ordering::SeqCst))
}

#[derive(Clone, Debug)]
pub(crate) struct Provision {
    pub(crate) registry: String,
    pub(crate) rate_limit: u64,
    pub(crate) allocator: Option<DelegatedOptions>,
}

static EPOCH: AtomicU64 = AtomicU64::new(0);

pub(crate) fn superseded(mine: u64) -> bool {
    worker::is_stopped() || EPOCH.load(Ordering::SeqCst) != mine
}

pub(crate) fn supersede() -> u64 {
    EPOCH.fetch_add(1, Ordering::SeqCst) + 1
}

fn read_interval(waited: Duration) -> Duration {
    if waited < Duration::from_secs(60) {
        TICK
    } else if waited < Duration::from_secs(600) {
        Duration::from_secs(30)
    } else {
        Duration::from_secs(300)
    }
}

const DECLARED_GAS_LIMIT: u128 = 10_000_000;

const ASSUMED_DATA_BYTES: u128 = 100_000;

const BASE_FEE_HEADROOM: u128 = 2;

const FALLBACK_FEE_RESERVE: u128 = (DECLARED_GAS_LIMIT + ASSUMED_DATA_BYTES) * 64;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ReserveBasis {
    Quote(u64),
    Fallback,
}

fn fee_reserve_from(quote: &serde_json::Value) -> Option<u128> {
    let ceiling = |key: &str| {
        quote
            .get(key)
            .and_then(serde_json::Value::as_u64)
            .map(u128::from)
    };
    let exec = ceiling("next_base_fee_exec_ceiling")?;
    let stor = ceiling("next_base_fee_stor_ceiling")?;
    Some(
        DECLARED_GAS_LIMIT
            .saturating_mul(exec)
            .saturating_add(ASSUMED_DATA_BYTES.saturating_mul(stor))
            .saturating_mul(BASE_FEE_HEADROOM),
    )
}

fn fee_reserve(quote: Result<serde_json::Value, ApiError>) -> (u128, ReserveBasis) {
    let computed = quote.ok().and_then(|q| {
        let height = q
            .get("height")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        fee_reserve_from(&q).map(|r| (r, ReserveBasis::Quote(height)))
    });
    computed.unwrap_or((FALLBACK_FEE_RESERVE, ReserveBasis::Fallback))
}

enum Delegated {
    Granted,
    Locked(String),
    Refused(String),
    Missed(String),
    Superseded,
}

fn delegate_one(
    registry: &registry_id::CanonicalRegistryId,
    rate_limit: u64,
    allocator: &DelegatedOptions,
    mine: u64,
) -> Delegated {
    let who = allocator_who(registry, allocator);
    record(
        &registry.canonical,
        Step::Delegating,
        format!("asking {who}"),
    );
    let store = crate::sealed_store::store::current_or_uninit();
    let mut reply =
        match register_scoped(store, registry, REGISTRY_WIDE, rate_limit, Some(allocator)) {
            Ok(v) => v,
            Err(e) if e.kind == ErrorKind::Locked => {
                return Delegated::Locked(format!("{who}: {}", e.message))
            }
            Err(e) => return Delegated::Refused(format!("{who}: {}", e.message)),
        };
    let hash = match reply
        .get_mut("membership_hash")
        .map(serde_json::Value::take)
    {
        Some(serde_json::Value::String(h)) => h,
        _ => String::new(),
    };
    let started = Instant::now();
    let mut attribution_wait = Duration::ZERO;
    loop {
        if superseded(mine) {
            return Delegated::Superseded;
        }
        let record_now = crate::sealed_store::store::current().and_then(|s| {
            s.membership(&hash)
                .filter(|r| r.identity.registry_id == registry.canonical)
        });
        match record_now {
            Some(r) if r.cache.state.is_usable() => {
                let by = match granted_by(&hash) {
                    Some(peer) => format!("allocator {peer}"),
                    None if attribution_wait < ATTRIBUTION_HOLD => {
                        attribution_wait += TICK;
                        std::thread::sleep(TICK);
                        continue;
                    }
                    None => "an allocator".to_string(),
                };
                record(
                    &registry.canonical,
                    Step::Done,
                    format!(
                        "granted by {by} at leaf {}",
                        r.cache.leaf_index.unwrap_or(0)
                    ),
                );
                eprintln!(
                    "provision {}: {by} registered a registry-wide membership at rate {rate_limit}",
                    registry.canonical
                );
                return Delegated::Granted;
            }
            Some(r) if r.cache.state == MembershipState::Pending => {}
            Some(r) => {
                let why = match r.cache.failed_reason.as_deref() {
                    Some(why) => format!("{who}: {why}"),
                    None => format!("{who}: record went {:?}", r.cache.state),
                };
                return if take_discovery_miss(&hash) {
                    Delegated::Missed(why)
                } else {
                    Delegated::Refused(why)
                };
            }
            None => {}
        }
        if started.elapsed() >= DELEGATE_HOLD {
            return Delegated::Refused(format!(
                "{who}: no answer within {}s",
                DELEGATE_HOLD.as_secs()
            ));
        }
        std::thread::sleep(TICK);
    }
}

fn allocator_who(
    registry: &registry_id::CanonicalRegistryId,
    allocator: &DelegatedOptions,
) -> String {
    match (allocator.discover, allocator.allocator_peer_id.is_empty()) {
        (true, true) => format!("a discovered allocator for {}", registry.canonical),
        (true, false) => format!(
            "a discovered allocator for {}, else allocator {} at {}",
            registry.canonical, allocator.allocator_peer_id, allocator.allocator_multiaddr
        ),
        (false, _) => format!(
            "allocator {} at {}",
            allocator.allocator_peer_id, allocator.allocator_multiaddr
        ),
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Step {
    Delegating,
    WaitingForWallet,
    AwaitingFunding,
    Registering,
    Done,
    Refused,
}

impl Step {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Step::Delegating => "delegating",
            Step::WaitingForWallet => "waiting_for_wallet",
            Step::AwaitingFunding => "awaiting_funding",
            Step::Registering => "registering",
            Step::Done => "done",
            Step::Refused => "refused",
        }
    }
}

pub(crate) fn run_with(registries: Vec<Provision>, ticket: u64) {
    run_as(registries, ticket)
}

pub(crate) fn run_alongside(registry: String, rate_limit: u64, ticket: u64) {
    let allocator = crate::config::allocator_for(&registry);
    run_as(
        vec![Provision {
            registry,
            rate_limit,
            allocator,
        }],
        ticket,
    )
}

fn run_as(registries: Vec<Provision>, mine: u64) {
    for Provision {
        registry: raw,
        rate_limit,
        allocator,
    } in registries
    {
        if superseded(mine) {
            return;
        }
        let registry = match registry_id::parse(&raw) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("provision {raw}: not a registry id ({e})");
                continue;
            }
        };
        let _active = ActivePass::enter_with(&registry.canonical, mine);
        match provision_one(&registry, rate_limit, allocator.as_ref(), mine) {
            Ok(()) => wait_confirmed(&registry, mine),
            Err(e) => {
                eprintln!("provision {}: {}", registry.canonical, e.message);
                record(&registry.canonical, Step::Refused, e.message);
            }
        }
    }
}

pub(crate) fn pending_membership_present(registry: &registry_id::CanonicalRegistryId) -> bool {
    crate::sealed_store::store::current()
        .map(|s| {
            s.any_for_registry(&registry.canonical, |r| {
                !r.quarantined && r.cache.state == crate::lifecycle::MembershipState::Pending
            })
        })
        .unwrap_or(false)
}

pub(crate) fn wait_confirmed(registry: &registry_id::CanonicalRegistryId, mine: u64) {
    let started = Instant::now();
    while pending_membership_present(registry) {
        if superseded(mine) || started.elapsed() >= CONFIRM_HOLD {
            return;
        }
        std::thread::sleep(TICK);
    }
}

pub(crate) fn any_pending_membership() -> bool {
    crate::sealed_store::store::current()
        .map(|s| s.has_pending())
        .unwrap_or(false)
}

fn wait_confirmed_any(mine: u64) {
    let started = Instant::now();
    while any_pending_membership() {
        if superseded(mine) || started.elapsed() >= CONFIRM_HOLD {
            return;
        }
        std::thread::sleep(TICK);
    }
}

#[cfg(test)]
pub(crate) fn ticket_in_force() -> u64 {
    EPOCH.load(Ordering::SeqCst)
}

pub(crate) fn live_membership_present(registry: &registry_id::CanonicalRegistryId) -> bool {
    crate::sealed_store::store::current()
        .map(|s| {
            s.any_for_registry(&registry.canonical, |r| {
                !r.quarantined && r.cache.state.is_live()
            })
        })
        .unwrap_or(false)
}

enum Live {
    Usable,
    StillPending,
    Failed(String),
    Superseded,
    None,
}

fn settle_live(registry: &registry_id::CanonicalRegistryId, mine: u64) -> Live {
    let started = Instant::now();
    let mut saw_pending = false;
    loop {
        let Some(store) = crate::sealed_store::store::current() else {
            return Live::None;
        };
        let records: Vec<_> = records_for_registry(&store, registry)
            .into_iter()
            .filter(|r| !r.quarantined)
            .collect();
        if records.iter().any(|r| r.cache.state.is_usable()) {
            return Live::Usable;
        }
        if !records
            .iter()
            .any(|r| r.cache.state == MembershipState::Pending)
        {
            if !saw_pending {
                return Live::None;
            }
            let why = records
                .iter()
                .filter(|r| r.cache.state == MembershipState::Failed)
                .max_by_key(|r| r.identity.submitted_at)
                .and_then(|r| r.cache.failed_reason.clone())
                .unwrap_or_else(|| "the pending registration failed".to_string());
            return Live::Failed(why);
        }
        if !saw_pending {
            saw_pending = true;
            record(
                &registry.canonical,
                Step::Delegating,
                "waiting for an earlier registration to settle",
            );
        }
        if superseded(mine) {
            return Live::Superseded;
        }
        if started.elapsed() >= DELEGATE_HOLD {
            return Live::StillPending;
        }
        std::thread::sleep(TICK);
    }
}

fn ask_again(
    registry: &registry_id::CanonicalRegistryId,
    rate_limit: u64,
    retry: &mut Option<&DelegatedOptions>,
    mine: u64,
) -> bool {
    let Some(a) = *retry else { return false };
    let who = allocator_who(registry, a);
    let next = discovery_retry_every().as_secs();
    let probe = crate::provider::allocator_discover_async(registry, a.auth_type.as_deref());
    let miss = match probe {
        Err(ApiError { message: e, .. }) | Ok(DiscoverReply::Failed(e)) => {
            eprintln!(
                "provision {}: discover failed: {e}; probing again in {next}s",
                registry.canonical
            );
            return false;
        }
        Ok(DiscoverReply::Unavailable(e)) => {
            eprintln!(
                "provision {}: {who}: {e}; no further allocator asks this pass",
                registry.canonical
            );
            *retry = None;
            return false;
        }
        Ok(DiscoverReply::Listed {
            joined: false,
            peers,
            ..
        }) => Some(format!("discovery not joined ({peers} peers)")),
        Ok(DiscoverReply::Listed {
            candidates: 0,
            peers,
            ..
        }) => Some(format!("discovery: no allocator yet ({peers} peers)")),
        Ok(DiscoverReply::Listed { .. }) => None,
    };
    if let Some(why) = miss {
        let why = format!("{who}: {why}");
        eprintln!(
            "provision {}: {why}; probing again in {next}s",
            registry.canonical
        );
        set_fallback(&registry.canonical, why);
        return false;
    }
    match delegate_one(registry, rate_limit, a, mine) {
        Delegated::Granted => {
            clear_fallback(&registry.canonical);
            true
        }
        Delegated::Superseded => true,
        Delegated::Locked(why) => {
            record(
                &registry.canonical,
                Step::AwaitingFunding,
                "keystore locked",
            );
            eprintln!(
                "provision {}: {why} — unlock the keystore and restart (or ensure_membership) to provision",
                registry.canonical
            );
            true
        }
        Delegated::Missed(why) => {
            eprintln!(
                "provision {}: {why}; probing again in {next}s",
                registry.canonical
            );
            set_fallback(&registry.canonical, why);
            false
        }
        Delegated::Refused(why) => {
            eprintln!(
                "provision {}: {why}; no further allocator asks this pass",
                registry.canonical
            );
            *retry = None;
            false
        }
    }
}

fn provision_one(
    registry: &registry_id::CanonicalRegistryId,
    rate_limit: u64,
    allocator: Option<&DelegatedOptions>,
    mine: u64,
) -> Result<(), ApiError> {
    let entered = Instant::now();
    clear_fallback(&registry.canonical);
    match settle_live(registry, mine) {
        Live::Usable => {
            record(
                &registry.canonical,
                Step::Done,
                "membership already present",
            );
            return Ok(());
        }
        Live::StillPending => {
            record(&registry.canonical, Step::Done, "membership pending");
            return Ok(());
        }
        Live::Superseded => return Ok(()),
        Live::Failed(why) => {
            eprintln!(
                "provision {}: earlier registration failed ({why}); provisioning anew",
                registry.canonical
            );
            set_fallback(&registry.canonical, why);
        }
        Live::None => {}
    }
    if let Some(s) = crate::sealed_store::store::current() {
        let records = records_for_registry(&s, registry);
        if let Some(r) = records.iter().find(|r| r.quarantined) {
            record(
                &registry.canonical,
                Step::Refused,
                format!(
                    "membership {} is quarantined (local metadata failed its MAC); \
                     not registering a replacement — inspect with get_memberships, \
                     then register_membership deliberately",
                    r.hash
                ),
            );
            return Ok(());
        }
    }

    let mut retry: Option<&DelegatedOptions> = None;
    if let Some(a) = allocator {
        match delegate_one(registry, rate_limit, a, mine) {
            Delegated::Granted | Delegated::Superseded => return Ok(()),
            Delegated::Locked(why) => {
                record(
                    &registry.canonical,
                    Step::AwaitingFunding,
                    "keystore locked",
                );
                eprintln!(
                    "provision {}: {why} — unlock the keystore and restart (or ensure_membership) to provision",
                    registry.canonical
                );
                return Ok(());
            }
            Delegated::Refused(why) => {
                eprintln!(
                    "provision {}: {why}; falling back to a funded registration",
                    registry.canonical
                );
                set_fallback(&registry.canonical, why);
            }
            Delegated::Missed(why) => {
                eprintln!(
                    "provision {}: {why}; falling back to a funded registration",
                    registry.canonical
                );
                if a.discover {
                    retry = Some(a);
                }
                set_fallback(&registry.canonical, why);
            }
        }
    }

    let backend = backend_of(registry)?;

    record(&registry.canonical, Step::WaitingForWallet, "");
    let began = Instant::now();
    let mut noticed = false;
    loop {
        if superseded(mine) {
            return Ok(());
        }
        if let Ok(status) = backend.payer.wallet_status() {
            match status.get("state").and_then(|x| x.as_str()) {
                Some("ready") => break,
                Some("failed") => {
                    let detail = status.get("detail").and_then(|x| x.as_str()).unwrap_or("");
                    return Err(ApiError::new(
                        ErrorKind::ProviderFailure,
                        format!("the registry module's wallet failed to come up: {detail}"),
                    ));
                }
                Some("pending") => {
                    let waited = began.elapsed();
                    if waited >= WALLET_WAIT_NOTICE && !noticed {
                        noticed = true;
                        let detail = status.get("detail").and_then(|x| x.as_str()).unwrap_or("");
                        eprintln!(
                            "provision {}: still waiting for the wallet after {}s: {detail}",
                            registry.canonical,
                            waited.as_secs()
                        );
                        record(
                            &registry.canonical,
                            Step::WaitingForWallet,
                            format!("{}s: {detail}", waited.as_secs()),
                        );
                    }
                }
                _ => {}
            }
        }
        let waited = began.elapsed();
        let due = waited + read_interval(waited);
        while began.elapsed() < due {
            if superseded(mine) {
                return Ok(());
            }
            std::thread::sleep(TICK);
        }
    }

    if let Some(msg) = crate::provider::registry_problem(backend.registry, registry) {
        return Err(ApiError::new(ErrorKind::UnknownRegistry, msg));
    }

    let price = registration_price(backend.registry, registry, rate_limit)?;
    let (mut reserve, mut basis) = fee_reserve(backend.payer.fee_state());
    log_basis(&registry.canonical, reserve, basis);
    let mut required = price.saturating_add(reserve);

    let payer = backend
        .payer
        .wallet_status()
        .ok()
        .and_then(|s| s.get("payer").and_then(|p| p.as_str()).map(str::to_owned))
        .unwrap_or_default();
    record_funding(
        &registry.canonical,
        format!("{payer} needs {required} native ({price} price + {reserve} fee reserve)"),
        Funding {
            payer: payer.clone(),
            required,
            price,
            fee_reserve: reserve,
            balance: None,
        },
    );
    let mut waited = Duration::ZERO;
    let mut due = Duration::ZERO;
    let mut announced: Option<(u128, u128)> = None;
    let mut asked_while_waiting: Option<Instant> = None;
    loop {
        if superseded(mine) {
            return Ok(());
        }
        if retry.is_some()
            && asked_while_waiting.unwrap_or(entered).elapsed() >= discovery_retry_every()
        {
            if ask_again(registry, rate_limit, &mut retry, mine) {
                return Ok(());
            }
            asked_while_waiting = Some(Instant::now());
            due = waited;
        }
        if waited >= due {
            if live_membership_present(registry) {
                record(
                    &registry.canonical,
                    Step::Done,
                    "membership already present",
                );
                return Ok(());
            }
            if waited > Duration::ZERO {
                let (r, b) = fee_reserve(backend.payer.fee_state());
                if std::mem::discriminant(&b) != std::mem::discriminant(&basis) {
                    log_basis(&registry.canonical, r, b);
                }
                (reserve, basis) = (r, b);
                required = price.saturating_add(reserve);
            }
            match backend.payer.payer_balance() {
                Ok(balance) if balance >= required => break,
                Ok(balance) => {
                    if announced != Some((balance, required)) {
                        eprintln!(
                            "provision {}: waiting for {payer} to hold {required} native \
                             ({price} price + {reserve} fee reserve); it holds {balance}",
                            registry.canonical
                        );
                        announced = Some((balance, required));
                    }
                    record_funding(
                        &registry.canonical,
                        format!(
                            "{payer} needs {required} native \
                             ({price} price + {reserve} fee reserve); it holds {balance}"
                        ),
                        Funding {
                            payer: payer.clone(),
                            required,
                            price,
                            fee_reserve: reserve,
                            balance: Some(balance),
                        },
                    );
                }
                Err(e) => eprintln!(
                    "provision {}: balance read failed: {}",
                    registry.canonical, e.message
                ),
            }
            due = waited + read_interval(waited);
        }
        std::thread::sleep(TICK);
        waited = waited.saturating_add(TICK);
    }

    if superseded(mine) {
        return Ok(());
    }
    if retry.is_some() && asked_while_waiting.is_none() {
        while entered.elapsed() < discovery_retry_every() {
            if superseded(mine) {
                return Ok(());
            }
            std::thread::sleep(TICK.min(discovery_retry_every().saturating_sub(entered.elapsed())));
        }
        if ask_again(registry, rate_limit, &mut retry, mine) {
            return Ok(());
        }
    }
    wait_confirmed_any(mine);
    if superseded(mine) {
        return Ok(());
    }
    if live_membership_present(registry) {
        record(
            &registry.canonical,
            Step::Done,
            "membership already present",
        );
        return Ok(());
    }
    record(&registry.canonical, Step::Registering, "");
    let store = crate::sealed_store::store::current_or_uninit();
    match register_scoped(store, registry, REGISTRY_WIDE, rate_limit, None) {
        Ok(_) => {
            record(&registry.canonical, Step::Done, "submitted");
            eprintln!(
                "provision {}: registered a registry-wide membership at rate {rate_limit}",
                registry.canonical
            );
            Ok(())
        }
        Err(e) if e.kind == ErrorKind::Locked => {
            record(
                &registry.canonical,
                Step::AwaitingFunding,
                "keystore locked",
            );
            eprintln!(
                "provision {}: keystore is locked — unlock it and restart to provision",
                registry.canonical
            );
            Ok(())
        }
        Err(e) => Err(e),
    }
}

fn log_basis(registry: &str, reserve: u128, basis: ReserveBasis) {
    match basis {
        ReserveBasis::Quote(height) => eprintln!(
            "provision {registry}: fee reserve {reserve} from the chain's fee quote at height \
             {height}"
        ),
        ReserveBasis::Fallback => eprintln!(
            "provision {registry}: no fee quote from the registry module; reserving the fixed \
             {reserve}"
        ),
    }
}

fn registration_price(
    registry_source: &dyn crate::provider::Registry,
    registry: &registry_id::CanonicalRegistryId,
    rate_limit: u64,
) -> Result<u128, ApiError> {
    let bounds = registry_source.get_registry_bounds(registry)?;
    let price_per_unit = bounds
        .get("price_per_unit")
        .and_then(|p| p.as_str())
        .and_then(|p| p.parse::<u128>().ok())
        .ok_or_else(|| {
            ApiError::new(
                ErrorKind::ProviderFailure,
                "registry bounds carry no price_per_unit",
            )
        })?;
    Ok(price_per_unit.saturating_mul(u128::from(rate_limit)))
}

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Funding {
    pub(crate) payer: String,
    pub(crate) required: u128,
    pub(crate) price: u128,
    pub(crate) fee_reserve: u128,
    pub(crate) balance: Option<u128>,
}

#[derive(Clone, Debug)]
pub(crate) struct Progress {
    pub(crate) step: Step,
    pub(crate) detail: String,
    pub(crate) funding: Option<Funding>,
    pub(crate) fallback_reason: Option<String>,
}

static PROGRESS: Mutex<Option<HashMap<String, Progress>>> = Mutex::new(None);

fn set_fallback(registry: &str, why: impl Into<String>) {
    if let Some(p) = crate::util::lock(&PROGRESS)
        .as_mut()
        .and_then(|m| m.get_mut(registry))
    {
        p.fallback_reason = Some(why.into());
    }
}

fn clear_fallback(registry: &str) {
    if let Some(p) = crate::util::lock(&PROGRESS)
        .as_mut()
        .and_then(|m| m.get_mut(registry))
    {
        p.fallback_reason = None;
    }
}

fn record_progress(registry: &str, step: Step, detail: String, funding: Option<Funding>) {
    let mut guard = crate::util::lock(&PROGRESS);
    let map = guard.get_or_insert_with(HashMap::new);
    match map.get_mut(registry) {
        Some(p) => {
            p.step = step;
            p.detail = detail;
            p.funding = funding;
        }
        None => {
            map.insert(
                registry.to_owned(),
                Progress {
                    step,
                    detail,
                    funding,
                    fallback_reason: None,
                },
            );
        }
    }
}

fn record(registry: &str, step: Step, detail: impl Into<String>) {
    record_progress(registry, step, detail.into(), None);
}

fn record_funding(registry: &str, detail: impl Into<String>, funding: Funding) {
    record_progress(
        registry,
        Step::AwaitingFunding,
        detail.into(),
        Some(funding),
    );
}

pub(crate) fn mark_requested(registry: &str) {
    record(registry, Step::WaitingForWallet, "");
}

pub(crate) fn progress(registry: &str) -> Option<Progress> {
    crate::util::lock(&PROGRESS)
        .as_ref()
        .and_then(|m| m.get(registry).cloned())
}

static ACTIVE: Mutex<Option<HashMap<String, u64>>> = Mutex::new(None);

pub(crate) struct ActivePass(String, u64);

impl ActivePass {
    #[cfg(test)]
    pub(crate) fn enter(registry: &str) -> Self {
        Self::enter_with(registry, EPOCH.load(Ordering::SeqCst))
    }

    pub(crate) fn enter_with(registry: &str, ticket: u64) -> Self {
        crate::util::lock(&ACTIVE)
            .get_or_insert_with(HashMap::new)
            .insert(registry.to_owned(), ticket);
        ActivePass(registry.to_owned(), ticket)
    }

    pub(crate) fn claim(registry: &str) -> Option<Self> {
        let ticket = EPOCH.load(Ordering::SeqCst);
        let mut guard = crate::util::lock(&ACTIVE);
        let map = guard.get_or_insert_with(HashMap::new);
        if map.contains_key(registry) {
            return None;
        }
        map.insert(registry.to_owned(), ticket);
        Some(ActivePass(registry.to_owned(), ticket))
    }

    pub(crate) fn ticket(&self) -> u64 {
        self.1
    }
}

impl Drop for ActivePass {
    fn drop(&mut self) {
        if let Some(map) = crate::util::lock(&ACTIVE).as_mut() {
            if map.get(&self.0) == Some(&self.1) {
                map.remove(&self.0);
            }
        }
    }
}

pub(crate) fn is_running(registry: &str) -> bool {
    crate::util::lock(&ACTIVE)
        .as_ref()
        .map(|map| map.contains_key(registry))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ensure;
    use crate::generated::RlnModule;
    use crate::test_support::{imp_with_store, seed_membership, TEST_GLOBAL_LOCK};

    #[test]
    fn delegate_hold_outlasts_a_delegated_records_confirmation_deadline() {
        let submitted_at = 1_700_000_000;
        let deadline = crate::register::delegated_confirm_deadline(submitted_at) - submitted_at;
        assert!(DELEGATE_HOLD.as_secs() > deadline);
    }

    #[test]
    fn a_later_pass_supersedes_the_one_still_waiting() {
        let first = EPOCH.fetch_add(1, Ordering::SeqCst) + 1;
        assert!(
            !superseded(first),
            "the only pass must not think itself stale"
        );

        let second = EPOCH.fetch_add(1, Ordering::SeqCst) + 1;
        assert!(
            superseded(first),
            "the earlier pass must stand down once a later start() takes a ticket"
        );
        assert!(!superseded(second), "the newest pass is the one that runs");

        assert!(second > first);
    }

    #[test]
    fn the_funding_read_schedule_relaxes_and_never_spins() {
        let probes = [0, 5, 59, 60, 120, 599, 600, 3600, 86_400];
        let mut previous = Duration::ZERO;
        for secs in probes {
            let interval = read_interval(Duration::from_secs(secs));
            assert!(
                interval >= TICK,
                "at {secs}s the schedule returned {interval:?}, below one tick"
            );
            assert!(
                interval >= previous,
                "at {secs}s the schedule tightened from {previous:?} to {interval:?}"
            );
            previous = interval;
        }
        assert_eq!(read_interval(Duration::ZERO), TICK);
        assert_eq!(
            read_interval(Duration::from_secs(86_400)),
            Duration::from_secs(300)
        );
    }

    fn devnet_quote() -> serde_json::Value {
        serde_json::json!({
            "height": 15082, "base_fee_exec": 8, "base_fee_stor": 8,
            "next_base_fee_exec_floor": 8, "next_base_fee_exec_ceiling": 9,
            "next_base_fee_stor_floor": 8, "next_base_fee_stor_ceiling": 9,
            "max_gas_exec": 10_000_000, "max_gas_stor": 1_000_000,
        })
    }

    #[test]
    fn the_reserve_is_sized_from_the_quoted_ceiling() {
        let (reserve, basis) = fee_reserve(Ok(devnet_quote()));
        assert_eq!(basis, ReserveBasis::Quote(15082));
        assert_eq!(reserve, 181_800_000);
        assert!(reserve < FALLBACK_FEE_RESERVE);
    }

    #[test]
    fn the_headroom_multiplies_the_quoted_reserve() {
        let unmargined = DECLARED_GAS_LIMIT * 9 + ASSUMED_DATA_BYTES * 9;
        assert_eq!(
            fee_reserve_from(&devnet_quote()),
            Some(unmargined * BASE_FEE_HEADROOM)
        );
        let mut dearer = devnet_quote();
        dearer["next_base_fee_exec_ceiling"] = 18.into();
        dearer["next_base_fee_stor_ceiling"] = 18.into();
        assert_eq!(
            fee_reserve_from(&dearer),
            Some(unmargined * 2 * BASE_FEE_HEADROOM)
        );
    }

    #[test]
    fn no_quote_falls_back_to_the_fixed_reserve() {
        assert_eq!(FALLBACK_FEE_RESERVE, 646_400_000);
        let fallback = (FALLBACK_FEE_RESERVE, ReserveBasis::Fallback);
        let failure = ApiError::new(ErrorKind::ProviderFailure, "get_fee_state failed");
        assert_eq!(fee_reserve(Err(failure)), fallback);
        assert_eq!(fee_reserve(Ok(serde_json::json!("garbage"))), fallback);
        assert_eq!(fee_reserve(Ok(serde_json::json!({}))), fallback);
        let mut partial = devnet_quote();
        partial
            .as_object_mut()
            .unwrap()
            .remove("next_base_fee_stor_ceiling");
        assert_eq!(fee_reserve(Ok(partial)), fallback);
        let mut stringly = devnet_quote();
        stringly["next_base_fee_exec_ceiling"] = "9".into();
        assert_eq!(fee_reserve(Ok(stringly)), fallback);
    }

    #[test]
    fn affordability_counts_the_fee_reserve_not_just_the_price() {
        let price = 100u128 * 10_000;
        let (quoted, _) = fee_reserve(Ok(devnet_quote()));
        for reserve in [quoted, FALLBACK_FEE_RESERVE] {
            assert!(
                reserve > price * 100,
                "if the reserve ever stops dwarfing the price, revisit this gate: \
                 reserve {reserve}, price {price}"
            );
            assert!(price < price.saturating_add(reserve));
        }
    }

    #[test]
    fn a_saturating_price_or_quote_stays_unaffordable() {
        let mut absurd = devnet_quote();
        absurd["next_base_fee_exec_ceiling"] = u64::MAX.into();
        absurd["next_base_fee_stor_ceiling"] = u64::MAX.into();
        let (reserve, _) = fee_reserve(Ok(absurd));
        assert!(reserve > FALLBACK_FEE_RESERVE);
        assert_eq!(u128::MAX.saturating_add(reserve), u128::MAX);
        assert_eq!(u128::MAX.saturating_add(FALLBACK_FEE_RESERVE), u128::MAX);
    }

    #[test]
    fn progress_is_recorded_per_registry() {
        let a = "logos:test:aa";
        let b = "logos:test:bb";
        assert!(progress("logos:test:never-touched").is_none());

        record(a, Step::AwaitingFunding, "needs 646400000");
        record(b, Step::Done, "submitted");

        let p = progress(a).expect("a has progress");
        assert_eq!(p.step, Step::AwaitingFunding);
        assert_eq!(p.step.as_str(), "awaiting_funding");
        assert!(p.detail.contains("646400000"));
        assert!(p.funding.is_none(), "a plain record carries no snapshot");

        assert_eq!(progress(b).expect("b has progress").step, Step::Done);

        record(a, Step::Done, "submitted");
        assert_eq!(progress(a).expect("a still has progress").step, Step::Done);
    }

    #[test]
    fn the_fallback_reason_outlives_the_steps_of_its_pass_only() {
        let r = "logos:test:fallback-reason";
        record(r, Step::Delegating, "asking allocator p at /m");
        assert_eq!(progress(r).unwrap().step.as_str(), "delegating");
        assert!(progress(r).unwrap().fallback_reason.is_none());
        set_fallback(
            r,
            "allocator p at /m: allocation_failed: credential already used",
        );
        record(r, Step::WaitingForWallet, "");
        assert_eq!(
            progress(r).unwrap().fallback_reason.as_deref(),
            Some("allocator p at /m: allocation_failed: credential already used")
        );
        record_funding(
            r,
            "x needs 1",
            Funding {
                payer: "x".into(),
                required: 1,
                price: 1,
                fee_reserve: 0,
                balance: None,
            },
        );
        assert!(
            progress(r).unwrap().fallback_reason.is_some(),
            "the funding step keeps it"
        );
        clear_fallback(r);
        assert!(
            progress(r).unwrap().fallback_reason.is_none(),
            "a new pass starts clean"
        );
    }

    #[test]
    fn the_funding_snapshot_is_structured_and_does_not_outlive_its_step() {
        let r = "logos:test:funding-snapshot";
        let first = Funding {
            payer: "ab".repeat(32),
            required: 1_000_000 + 182_000_000,
            price: 1_000_000,
            fee_reserve: 182_000_000,
            balance: None,
        };
        record_funding(r, "ab… needs 183000000 native", first.clone());
        let p = progress(r).expect("recorded");
        assert_eq!(p.step, Step::AwaitingFunding);
        assert_eq!(p.funding.as_ref(), Some(&first));

        let read = Funding {
            balance: Some(1_000_000),
            ..first
        };
        record_funding(r, "…; it holds 1000000", read.clone());
        assert_eq!(progress(r).expect("refreshed").funding, Some(read));

        record(r, Step::Registering, "");
        let p = progress(r).expect("moved on");
        assert_eq!(p.step, Step::Registering);
        assert!(
            p.funding.is_none(),
            "a non-funding step must not carry the snapshot"
        );
    }

    #[test]
    fn a_superseded_pass_cannot_end_the_newer_one() {
        let r = "logos:test:two-passes";
        let older = ActivePass::enter_with(r, 1);
        let newer = ActivePass::enter_with(r, 2);
        drop(older);
        assert!(
            is_running(r),
            "the older marker's drop must not end the newer pass"
        );
        assert!(
            ActivePass::claim(r).is_none(),
            "a running pass refuses a claim"
        );
        drop(newer);
        assert!(!is_running(r));
        let claimed = ActivePass::claim(r).expect("free registry");
        assert!(is_running(r));
        drop(claimed);
        assert!(!is_running(r));
    }

    #[test]
    fn a_pass_is_running_only_while_its_marker_lives() {
        let r = "logos:test:active-marker";
        assert!(!is_running(r));
        {
            let _pass = ActivePass::enter(r);
            assert!(is_running(r));
            record(r, Step::AwaitingFunding, "parked");
        }
        assert!(!is_running(r), "dropping the marker ends the pass");
        assert_eq!(
            progress(r).expect("the last step stays readable").step,
            Step::AwaitingFunding
        );
    }

    #[test]
    fn the_provisioned_scope_is_registry_wide() {
        assert_eq!(REGISTRY_WIDE, "");
    }

    #[test]
    fn a_scoped_membership_counts_as_provisioned() {
        let scoped = "aa".repeat(32);
        assert!(!crate::scope::scope_matches_for_test(
            &scoped,
            REGISTRY_WIDE
        ));
        assert!(crate::scope::scope_matches_for_test("", REGISTRY_WIDE));
    }

    #[test]
    fn a_locked_keystore_stops_the_pass_before_the_funding_wait() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir =
            std::env::temp_dir().join(format!("rln-locked-allocator-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (_imp, _store) = imp_with_store(dir.clone());
        let registry = format!("logos:local:{}", "7d".repeat(32));
        let canonical = registry_id::parse(&registry).unwrap().canonical;
        let allocator = DelegatedOptions::from_allocator_block(
            Some(&serde_json::json!({ "peer_id": "p", "multiaddr": "/m", "discover": false })),
            "x",
        )
        .unwrap();
        ensure::run_with(
            vec![ensure::Provision {
                registry: canonical.clone(),
                rate_limit: 100,
                allocator,
            }],
            ensure::supersede(),
        );
        let step = ensure::progress(&canonical).expect("a step is recorded");
        assert_eq!(step.step, ensure::Step::AwaitingFunding);
        assert_eq!(step.detail, "keystore locked");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_ensure_pass_claimed_before_a_start_is_ended_by_it() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        crate::worker::reset_for_test();
        let dir =
            std::env::temp_dir().join(format!("rln-ensure-claim-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (_imp, store) = imp_with_store(dir.clone());
        store.unlock("pw").unwrap();
        let registry = format!("logos:local:{}", "7f".repeat(32));
        let canonical = registry_id::parse(&registry).unwrap().canonical;
        seed_membership(
            &store,
            "pc",
            &canonical,
            &"57".repeat(32),
            "",
            &"e7".repeat(32),
            MembershipState::Active,
            3,
            100,
        );

        let pass = ActivePass::claim(&canonical).expect("no pass holds the registry");
        ensure::mark_requested(&canonical);
        ensure::supersede();
        ensure::run_alongside(canonical.clone(), 100, pass.ticket());
        let step = ensure::progress(&canonical).expect("a step is recorded");
        assert_eq!(step.step, ensure::Step::WaitingForWallet);
        assert_eq!(step.detail, "");
        drop(pass);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn missed(discovery: &str) -> Result<String, ApiError> {
        Ok(serde_json::json!({
            "kind": "no_provider", "discovery": discovery,
            "error": format!("nobody asked ({discovery})"),
        })
        .to_string())
    }

    fn listed(n: usize) -> Result<String, ApiError> {
        let candidates: Vec<_> = (0..n)
            .map(|i| serde_json::json!({ "peerId": format!("p{i}"), "addrs": ["/m"] }))
            .collect();
        Ok(serde_json::json!({ "serviceId": "s", "candidates": candidates, "joined": true, "peers": 3 }).to_string())
    }

    fn granted(args: &serde_json::Value) -> Result<String, ApiError> {
        let store = crate::sealed_store::store::current().expect("a published store");
        for r in store.pending_records() {
            store
                .update_cache(&r.hash, |m| {
                    m.state = MembershipState::Active;
                    m.leaf_index = Some(5);
                })
                .unwrap();
        }
        Ok(serde_json::json!({ "leaf_index": 5, "allocatorPeerId": "p", "discovered": true, "config_account": args["config"] }).to_string())
    }

    type Answers =
        dyn Fn(&str, usize, &serde_json::Value, &str) -> Result<String, ApiError> + Send + Sync;

    struct DiscoveryPass {
        dir: std::path::PathBuf,
        _imp: crate::test_support::TestImp,
        registry: String,
        requests: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        probes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl DiscoveryPass {
        fn new(name: &str, balance: u128, answers: Box<Answers>) -> Self {
            use crate::provider::fake::{self, FakePayer, FakeRegistry, FakeTree};
            use std::sync::{atomic::AtomicUsize, Arc};
            let dir =
                std::env::temp_dir().join(format!("rln-discovery-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            let (imp, _store) = imp_with_store(dir.clone());
            assert!(imp
                .unlock_keystore("pw".into())
                .contains(r#""unlocked":true"#));
            let registry = registry_id::parse(&format!("fakens:local:{name}"))
                .unwrap()
                .canonical;
            crate::test_support::install_backend(fake::backend(
                FakeTree::default(),
                FakeRegistry {
                    membership: None,
                    bounds: Some(
                        serde_json::json!({ "price_per_unit": "1", "min_rate_limit": 1, "max_rate_limit": 1000 }),
                    ),
                },
                FakePayer {
                    balance: Some(balance),
                },
            ));
            crate::provider::fake::REGISTER_CALLS.store(0, Ordering::SeqCst);
            let (requests, probes) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
            let (r, p, at) = (Arc::clone(&requests), Arc::clone(&probes), registry.clone());
            crate::test_support::install_allocator(Box::new(move |method, args| {
                let n = if method == "request" { &r } else { &p }.fetch_add(1, Ordering::SeqCst);
                answers(method, n, args, &at)
            }));
            DiscoveryPass {
                dir,
                _imp: imp,
                registry,
                requests,
                probes,
            }
        }

        fn run(&self, allocator: &serde_json::Value) {
            let allocator = DelegatedOptions::from_allocator_block(Some(allocator), "x").unwrap();
            ensure::run_with(
                vec![ensure::Provision {
                    registry: self.registry.clone(),
                    rate_limit: 100,
                    allocator,
                }],
                ensure::supersede(),
            );
        }

        fn counts(&self) -> (usize, usize, usize, usize) {
            let records = crate::sealed_store::store::current()
                .unwrap()
                .records_for(&self.registry)
                .len();
            (
                self.requests.load(Ordering::SeqCst),
                self.probes.load(Ordering::SeqCst),
                crate::provider::fake::REGISTER_CALLS.load(Ordering::SeqCst),
                records,
            )
        }
    }

    impl Drop for DiscoveryPass {
        fn drop(&mut self) {
            *crate::lock(&crate::test_support::INSTALLED_ALLOCATOR) = None;
            DISCOVERY_RETRY_SECS.store(60, Ordering::SeqCst);
            crate::sealed_store::store::publish(None);
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[test]
    fn a_funded_pass_probes_a_missed_allocator_once_more_before_paying() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        DISCOVERY_RETRY_SECS.store(2, Ordering::SeqCst);
        let pass = DiscoveryPass::new(
            "funded-grace",
            1_000_000_000_000,
            Box::new(|method, n, args, _| match (method, n) {
                ("request", 0) => missed("none_advertising"),
                ("request", _) => granted(args),
                _ => listed(1),
            }),
        );
        let began = std::time::Instant::now();
        pass.run(&serde_json::json!({}));
        let step = ensure::progress(&pass.registry).expect("a step is recorded");
        assert_eq!(step.step, ensure::Step::Done, "{}", step.detail);
        assert!(step.detail.contains("(discovered)"), "{}", step.detail);
        assert!(step.fallback_reason.is_none());
        assert_eq!(pass.counts(), (2, 1, 0, 2));
        assert!(
            began.elapsed() >= std::time::Duration::from_secs(2),
            "the probe waits out the interval"
        );
    }

    #[test]
    fn an_unfunded_pass_keeps_probing_a_missed_allocator() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        DISCOVERY_RETRY_SECS.store(1, Ordering::SeqCst);
        let seen = std::sync::Arc::new(Mutex::new(None));
        let saw = std::sync::Arc::clone(&seen);
        let pass = DiscoveryPass::new(
            "unfunded",
            0,
            Box::new(move |method, n, args, registry| match (method, n) {
                ("request", 0) => missed("not_joined"),
                ("request", _) => {
                    *crate::lock(&saw) = ensure::progress(registry).and_then(|p| p.fallback_reason);
                    granted(args)
                }
                (_, 0) => listed(0),
                _ => listed(1),
            }),
        );
        pass.run(&serde_json::json!({}));
        let step = ensure::progress(&pass.registry).expect("a step is recorded");
        assert_eq!(step.step, ensure::Step::Done, "{}", step.detail);
        assert!(step.detail.contains("(discovered)"), "{}", step.detail);
        assert!(step.fallback_reason.is_none());
        assert_eq!(
            pass.counts(),
            (2, 2, 0, 2),
            "no request, and no record, for the empty probe"
        );
        let seen = crate::lock(&seen).clone();
        assert!(
            seen.as_deref()
                .is_some_and(|r| r.contains("discovery: no allocator yet (3 peers)")),
            "{seen:?}"
        );
    }

    #[test]
    fn discovery_unavailable_is_asked_once_and_paid_for() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let unavailable: Box<Answers> = Box::new(|method, _, _, _| match method {
            "request" => missed("unavailable"),
            _ => listed(1),
        });
        let refused: Box<Answers> = Box::new(|method, _, _, _| match method {
            "request" => Ok(serde_json::json!({
                "kind": "refused", "reason": "auth_rejected",
                "error": "no allocator advertises R/peer-any; discovery not joined",
                "allocatorPeerId": "p", "discovered": true,
            })
            .to_string()),
            _ => listed(1),
        });
        for (name, answers, reason) in [
            (
                "unavailable",
                unavailable,
                "allocation_failed: nobody asked (unavailable)",
            ),
            (
                "refused",
                refused,
                "allocation_failed: no allocator advertises R/peer-any; discovery not joined",
            ),
        ] {
            DISCOVERY_RETRY_SECS.store(1, Ordering::SeqCst);
            let pass = DiscoveryPass::new(name, 1_000_000_000_000, answers);
            pass.run(&serde_json::json!({}));
            let step = ensure::progress(&pass.registry).expect("a step is recorded");
            assert_eq!(
                pass.counts(),
                (1, 0, 1, 2),
                "{name}: the funded submit was reached"
            );
            assert_eq!(
                step.step,
                ensure::Step::Refused,
                "the fake registry refuses every submit"
            );
            assert!(
                step.fallback_reason
                    .as_deref()
                    .is_some_and(|r| r.contains(reason)),
                "{:?}",
                step.fallback_reason
            );
        }
    }

    #[test]
    fn an_unavailable_probe_ends_the_asking() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        DISCOVERY_RETRY_SECS.store(1, Ordering::SeqCst);
        let pass = DiscoveryPass::new(
            "probe-unavailable",
            1_000_000_000_000,
            Box::new(|method, _, _, _| match method {
                "request" => missed("none_advertising"),
                _ => Ok(serde_json::json!({
                    "kind": "no_provider", "error": "libp2p down",
                })
                .to_string()),
            }),
        );
        pass.run(&serde_json::json!({}));
        assert_eq!(
            pass.counts(),
            (1, 1, 1, 2),
            "one probe, then the funded submit"
        );
    }

    #[test]
    fn a_pinned_pair_is_not_probed() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        DISCOVERY_RETRY_SECS.store(1, Ordering::SeqCst);
        let pass = DiscoveryPass::new(
            "pinned",
            1_000_000_000_000,
            Box::new(|method, _, _, _| match method {
                "request" => missed("none_advertising"),
                _ => listed(1),
            }),
        );
        pass.run(&serde_json::json!({ "peer_id": "p", "multiaddr": "/m", "discover": false }));
        assert_eq!(pass.counts(), (1, 0, 1, 2));
    }

    #[test]
    fn a_pending_registration_anywhere_holds_the_next_submit() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir = std::env::temp_dir().join(format!("rln-any-pending-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (_imp, store) = imp_with_store(dir.clone());
        store.unlock("pw").unwrap();
        assert!(!ensure::any_pending_membership());
        let registry = format!("logos:local:{}", "7e".repeat(32));
        seed_membership(
            &store,
            "pb",
            &registry,
            &"56".repeat(32),
            "",
            &"e6".repeat(32),
            MembershipState::Pending,
            0,
            100,
        );
        assert!(ensure::any_pending_membership());
        store
            .update_cache("pb", |m| m.state = MembershipState::Active)
            .unwrap();
        assert!(!ensure::any_pending_membership());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn provisioning_holds_while_its_registration_is_pending() {
        let _serial = crate::lock(&TEST_GLOBAL_LOCK);
        let dir = std::env::temp_dir().join(format!("rln-ms-hold-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (imp, store) = imp_with_store(dir.clone());
        assert!(imp
            .unlock_keystore("pw".into())
            .contains(r#""unlocked":true"#));
        let registry = format!("logos:local:{}", "7a".repeat(32));
        let parsed = registry_id::parse(&registry).unwrap();
        let hash = "hold-pending";
        seed_membership(
            &store,
            hash,
            &registry,
            &"11".repeat(32),
            "",
            &"22".repeat(32),
            MembershipState::Pending,
            0,
            100,
        );
        assert!(ensure::pending_membership_present(&parsed));

        let t0 = std::time::Instant::now();
        ensure::wait_confirmed(&parsed, u64::MAX);
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(2),
            "a superseded pass must return at once"
        );

        store
            .update_cache(hash, |m| m.state = MembershipState::Active)
            .unwrap();
        assert!(!ensure::pending_membership_present(&parsed));
        let t0 = std::time::Instant::now();
        ensure::wait_confirmed(&parsed, ensure::ticket_in_force());
        assert!(t0.elapsed() < std::time::Duration::from_secs(2));
        drop(imp);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
