use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

const GRACE: Duration = Duration::from_millis(200);

static STOPPED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, PartialEq, Eq)]
enum WorkerState {
    NeverStarted,
    Running,
    Stopped,
}

type Held = Option<(JoinHandle<()>, Arc<AtomicBool>)>;

#[derive(Clone, Copy)]
pub(crate) enum Slot {
    Poller,
    Refresher,
}

struct Supervisor {
    state: WorkerState,
    generation: u64,
    nudges: u64,
    running: usize,
    poller: Held,
    refresher: Held,
    warm: Held,
}

static SUP: Mutex<Supervisor> = Mutex::new(Supervisor {
    state: WorkerState::NeverStarted,
    generation: 0,
    nudges: 0,
    running: 0,
    poller: None,
    refresher: None,
    warm: None,
});
static CVAR: Condvar = Condvar::new();

pub(crate) fn is_stopped() -> bool {
    STOPPED.load(Ordering::SeqCst)
}

pub(crate) fn wait_tick(my_gen: u64, dur: Duration) -> bool {
    wait(my_gen, dur, false)
}

pub(crate) fn wait_tick_or_nudge(my_gen: u64, dur: Duration) -> bool {
    wait(my_gen, dur, true)
}

fn wait(my_gen: u64, dur: Duration, honor_nudge: bool) -> bool {
    let guard = crate::util::lock(&SUP);
    let entry_nudges = guard.nudges;
    let (sup, timeout) = CVAR
        .wait_timeout_while(guard, dur, |s| {
            s.generation == my_gen
                && s.state != WorkerState::Stopped
                && (!honor_nudge || s.nudges == entry_nudges)
        })
        .unwrap_or_else(|p| p.into_inner());
    (timeout.timed_out() || (honor_nudge && sup.nudges != entry_nudges))
        && sup.generation == my_gen
        && sup.state != WorkerState::Stopped
}

pub(crate) fn nudge() {
    crate::util::lock(&SUP).nudges += 1;
    CVAR.notify_all();
}

#[cfg(test)]
pub(crate) fn nudges_for_test() -> u64 {
    crate::util::lock(&SUP).nudges
}

#[cfg(test)]
pub(crate) fn generation_for_test() -> u64 {
    crate::util::lock(&SUP).generation
}

pub(crate) fn ensure(slot: Slot, name: &'static str, body: fn(u64)) {
    fn held(s: &mut Supervisor, slot: Slot) -> &mut Held {
        match slot {
            Slot::Poller => &mut s.poller,
            Slot::Refresher => &mut s.refresher,
        }
    }
    let mut sup = crate::util::lock(&SUP);
    if sup.state == WorkerState::Stopped {
        return;
    }
    let alive = matches!(held(&mut sup, slot), Some((_, done)) if !done.load(Ordering::SeqCst));
    if alive {
        return;
    }
    if let Some((handle, _)) = held(&mut sup, slot).take() {
        let _ = handle.join();
    }
    let gen = sup.generation;
    *held(&mut sup, slot) = Some(spawn_into(name, gen, move || body(gen)));
    sup.running += 1;
}

fn spawn_into(
    name: &'static str,
    gen: u64,
    body: impl FnOnce() + Send + 'static,
) -> (JoinHandle<()>, Arc<AtomicBool>) {
    let done = Arc::new(AtomicBool::new(false));
    let done_in = Arc::clone(&done);
    let handle = std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            body();
            {
                let mut sup = crate::util::lock(&SUP);
                if sup.generation == gen {
                    sup.running = sup.running.saturating_sub(1);
                }
            }
            done_in.store(true, Ordering::SeqCst);
            CVAR.notify_all();
        })
        .expect("invariant: the OS can spawn a worker thread");
    (handle, done)
}

pub(crate) fn start(warm: impl FnOnce() + Send + 'static, respawn: &[fn()]) {
    {
        let mut sup = crate::util::lock(&SUP);
        if sup.state == WorkerState::Stopped {
            sup.generation += 1;
        }
        sup.state = WorkerState::Running;
        STOPPED.store(false, Ordering::SeqCst);
    }
    CVAR.notify_all();
    for ensure_worker in respawn {
        ensure_worker();
    }
    let mut sup = crate::util::lock(&SUP);
    if let Some((handle, done)) = sup.warm.take() {
        if done.load(Ordering::SeqCst) {
            let _ = handle.join();
        }
    }
    if sup.state != WorkerState::Stopped {
        let gen = sup.generation;
        sup.warm = Some(spawn_into("rln-warm", gen, warm));
        sup.running += 1;
    }
}

pub(crate) fn stop() {
    {
        let mut sup = crate::util::lock(&SUP);
        sup.state = WorkerState::Stopped;
        STOPPED.store(true, Ordering::SeqCst);
    }
    CVAR.notify_all();
    let guard = crate::util::lock(&SUP);
    let (mut sup, _) = CVAR
        .wait_timeout_while(guard, GRACE, |s| s.running > 0)
        .unwrap_or_else(|p| p.into_inner());
    let sup: &mut Supervisor = &mut sup;
    for slot in [&mut sup.poller, &mut sup.refresher, &mut sup.warm] {
        if let Some((handle, done)) = slot.take() {
            if done.load(Ordering::SeqCst) {
                let _ = handle.join();
            }
        }
    }
    sup.generation += 1;
    sup.running = 0;
}

#[cfg(test)]
pub(crate) fn live_worker_count() -> usize {
    crate::util::lock(&SUP).running
}

#[cfg(test)]
pub(crate) fn reset_for_test() {
    let mut sup = crate::util::lock(&SUP);
    sup.state = WorkerState::NeverStarted;
    sup.generation += 1;
    sup.poller = None;
    sup.refresher = None;
    sup.warm = None;
    sup.running = 0;
    STOPPED.store(false, Ordering::SeqCst);
    drop(sup);
    CVAR.notify_all();
}

#[cfg(test)]
pub(crate) fn spawn_blocking_for_test(gate: Arc<(Mutex<bool>, Condvar)>, exited: Arc<AtomicBool>) {
    let mut sup = crate::util::lock(&SUP);
    let gen = sup.generation;
    sup.poller = Some(spawn_into("rln-test-blocker", gen, move || {
        let (m, cv) = &*gate;
        let mut released = m.lock().unwrap_or_else(|p| p.into_inner());
        while !*released {
            released = cv.wait(released).unwrap_or_else(|p| p.into_inner());
        }
        exited.store(true, Ordering::SeqCst);
    }));
    sup.running += 1;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    const RESPAWN: &[fn()] = &[
        crate::poller::ensure_running,
        crate::roots::ensure_refresher,
    ];

    #[test]
    fn nudge_wakes_only_the_subscribed_loop() {
        let _serial = crate::util::lock(&crate::test_support::TEST_GLOBAL_LOCK);
        reset_for_test();
        let gen = generation_for_test();

        let woke = Arc::new(AtomicBool::new(false));
        let woke2 = Arc::clone(&woke);
        let subscriber = std::thread::spawn(move || {
            woke2.store(
                wait_tick_or_nudge(gen, Duration::from_secs(60)),
                Ordering::SeqCst,
            );
        });
        let returned = Arc::new(AtomicBool::new(false));
        let returned2 = Arc::clone(&returned);
        let bystander = std::thread::spawn(move || {
            let due = wait_tick(gen, Duration::from_secs(60));
            returned2.store(true, Ordering::SeqCst);
            due
        });

        std::thread::sleep(Duration::from_millis(100));
        let began = Instant::now();
        nudge();
        subscriber.join().unwrap();
        assert!(
            woke.load(Ordering::SeqCst),
            "a nudge must count as a DUE tick, not a spurious wake"
        );
        assert!(
            began.elapsed() < Duration::from_secs(5),
            "nudge wake took {:?}",
            began.elapsed()
        );

        std::thread::sleep(Duration::from_millis(200));
        assert!(
            !returned.load(Ordering::SeqCst),
            "a nudge must not accelerate a loop that does not subscribe to it"
        );

        reset_for_test();
        assert!(
            !bystander.join().unwrap(),
            "a superseded waiter must not report a due tick"
        );
    }

    #[test]
    fn stop_joins_workers_quickly() {
        let _serial = crate::util::lock(&crate::test_support::TEST_GLOBAL_LOCK);
        reset_for_test();
        crate::poller::ensure_running();
        crate::roots::ensure_refresher();
        assert_eq!(live_worker_count(), 2);
        let began = Instant::now();
        stop();
        assert_eq!(live_worker_count(), 0);
        assert!(
            began.elapsed() < Duration::from_secs(2),
            "stop took {:?}",
            began.elapsed()
        );
        reset_for_test();
    }

    #[test]
    fn start_after_stop_respawns_with_new_generation() {
        let _serial = crate::util::lock(&crate::test_support::TEST_GLOBAL_LOCK);
        reset_for_test();
        start(|| {}, RESPAWN);
        let g1 = generation_for_test();
        assert!(live_worker_count() >= 2);
        stop();
        assert_eq!(live_worker_count(), 0);
        assert!(is_stopped());
        start(|| {}, RESPAWN);
        assert!(generation_for_test() > g1);
        assert!(!is_stopped());
        assert!(live_worker_count() >= 2);
        stop();
        reset_for_test();
    }

    #[test]
    fn spawn_is_forbidden_while_stopped_but_interest_is_recorded() {
        let _serial = crate::util::lock(&crate::test_support::TEST_GLOBAL_LOCK);
        reset_for_test();
        stop();
        crate::poller::ensure_running();
        let registry =
            crate::registry_id::parse(&format!("logos:local:{}", "fe".repeat(32))).unwrap();
        crate::roots::track(&registry);
        assert_eq!(live_worker_count(), 0, "nothing may spawn while stopped");
        assert!(
            crate::roots::tracked_contains(&registry.canonical),
            "interest is still recorded for the next start()"
        );
        reset_for_test();
    }

    #[test]
    fn on_demand_spawn_before_start_is_idempotent() {
        let _serial = crate::util::lock(&crate::test_support::TEST_GLOBAL_LOCK);
        reset_for_test();
        crate::poller::ensure_running();
        assert_eq!(live_worker_count(), 1);
        crate::poller::ensure_running();
        assert_eq!(live_worker_count(), 1);
        stop();
        reset_for_test();
    }

    #[test]
    fn stop_detaches_blocked_worker_without_duplicating_it() {
        let _serial = crate::util::lock(&crate::test_support::TEST_GLOBAL_LOCK);
        reset_for_test();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let exited = Arc::new(AtomicBool::new(false));
        spawn_blocking_for_test(Arc::clone(&gate), Arc::clone(&exited));

        let began = Instant::now();
        stop();
        assert!(
            began.elapsed() < Duration::from_secs(2),
            "stop took {:?}",
            began.elapsed()
        );
        assert!(
            !exited.load(Ordering::SeqCst),
            "the blocked worker is still parked, i.e. detached"
        );

        {
            let (m, cv) = &*gate;
            *m.lock().unwrap() = true;
            cv.notify_all();
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while !exited.load(Ordering::SeqCst) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            exited.load(Ordering::SeqCst),
            "released straggler must self-exit"
        );

        start(|| {}, RESPAWN);
        assert_eq!(live_worker_count(), 3);
        stop();
        reset_for_test();
    }
}
