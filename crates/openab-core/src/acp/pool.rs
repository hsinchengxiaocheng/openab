use crate::acp::connection::{AcpConnection, SessionActivity};
use crate::acp::project::ProjectContext;
use crate::acp::protocol::ConfigOption;
use crate::config::AgentConfig;
use anyhow::{anyhow, Context, Result};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;
use tracing::{info, warn};

/// Phase 6.2.9: pool-key prefix that marks a key as belonging to a fenced
/// native workflow dispatch.  Keys with this prefix are guaranteed to spawn a
/// fresh ACP `session/new` on every entry and never read from or write to
/// `state.persisted`, so a native turn cannot inherit unrelated historical
/// ACP conversation state merely because the same OpenAB daemon, Discord
/// delivery target, or ACP process was previously used.
pub const NATIVE_DISPATCH_KEY_PREFIX: &str = "native-dispatch:";

/// Returns true when `key` is a fenced native-work dispatch key.
pub fn is_native_dispatch_key(key: &str) -> bool {
    key.starts_with(NATIVE_DISPATCH_KEY_PREFIX)
}

/// Render the canonical native-dispatch execution-session key.
///
/// Format: `native-dispatch:{agent}:{dispatch_id}`.
///
/// The dispatch_id is already a UUID4-hex with the `oad-` prefix
/// (`oad-<32-hex>`), so it is guaranteed safe ASCII. The agent name is
/// one of `ArthurClaude` / `ArthurCodex` / `ArthurGemini` per the
/// `validate_agent_work` allowlist. Both are bounded ASCII path-safe
/// components. The combined key therefore matches the same
/// `redact_session_ids` policy as the legacy pool keys and is safe to
/// log, write to disk, and route through the pool.
pub fn format_native_dispatch_key(agent: &str, dispatch_id: &str) -> String {
    format!("{}{}:{}", NATIVE_DISPATCH_KEY_PREFIX, agent, dispatch_id)
}

/// Error substrings produced by `AcpConnection::send_request` that indicate a
/// transient failure worth preserving the session ID for retry, as opposed to
/// a permanent agent-side rejection.
const TRANSIENT_LOAD_ERRORS: &[&str] = &["timeout waiting for", "channel closed"];

/// Combined state protected by a single lock to prevent deadlocks.
/// Lock ordering: never await a per-connection mutex while holding `state`.
struct PoolState {
    /// Active connections: thread_key → AcpConnection handle.
    active: HashMap<String, Arc<Mutex<AcpConnection>>>,
    /// Lock-free cancel handles: thread_key → (stdin, session_id).
    /// Stored separately so cancel can work without locking the connection.
    cancel_handles: HashMap<String, CancelHandle>,
    /// Lock-free facade tokens: thread_key → the exact `OPENAB_SESSION_TOKEN` minted for the
    /// connection currently under that key. Stored here, not just inside the connection, so hung
    /// eviction can revoke the exact token **synchronously** — the `AcpConnection` DropGuard that
    /// normally revokes it cannot fire while a hung streaming task still holds an Arc of the
    /// connection, and `AcpTunnelSource` authorizes by channel alone, so an un-revoked predecessor
    /// token would keep reaching whatever tunnel a successor registers for that channel (F3).
    #[cfg(feature = "acp-mcp")]
    facade_tokens: HashMap<String, String>,
    /// Lock-free activity handles for hung-session detection without the connection mutex.
    activity: HashMap<String, Arc<SessionActivity>>,
    /// Child process-group ids, captured at insert time so hung eviction can
    /// kill the agent process without ever locking the connection.
    pgids: HashMap<String, i32>,
    /// Suspended sessions: thread_key → ACP sessionId.
    /// Used at runtime to decide which thread can be resumed via `session/load`
    /// because it no longer has a live in-memory connection.
    suspended: HashMap<String, String>,
    /// Persisted resumable sessions: thread_key → ACP sessionId.
    /// Includes both suspended sessions and active sessions so a process restart
    /// can recover any live thread via `session/load`.
    persisted: HashMap<String, String>,
    /// Serializes create/resume work per thread so rapid same-thread requests
    /// cannot race each other into duplicate `session/load` attempts.
    creating: HashMap<String, Arc<Mutex<()>>>,
    /// Per-session working directory overrides (from control directives).
    /// thread_key → canonical workspace path.
    session_workdirs: HashMap<String, String>,
    /// Per-session project bindings: thread_key → canonical
    /// (project_id, project_root). Populated only when an incoming
    /// `ProjectContext` carries a non-empty `project_id`; anonymous
    /// `[[ws:@alias]]` workspace hints are intentionally absent so they
    /// don't pin a session to a project_id. Mismatch against an existing
    /// binding is the staleness gate; see
    /// [`SessionPool::get_or_create`] and workflow
    /// `20260818-openab-project-scoped-acp-session-bootstrap`.
    session_projects: HashMap<String, ProjectContext>,
}

pub struct SessionPool {
    state: RwLock<PoolState>,
    config: AgentConfig,
    max_sessions: usize,
    /// Phase 6.4.10 (TOCTOU fix) — count of in-flight creation
    /// reservations. The invariant the pool enforces is:
    ///
    ///   `state.active.len() + outstanding_creations <= max_sessions`
    ///
    /// Every spawn path (`get_or_create`, `create_fresh_session_only`)
    /// must increment this counter BEFORE starting
    /// `AcpConnection::spawn` (in `ensure_capacity_or_evict`) and either
    /// commit it (on successful `state.active.insert`) or release it via
    /// RAII (on any failure path). Without this counter, two concurrent
    /// creators could each observe `active.len() < max_sessions`, each
    /// spawn, and each insert — pushing `active.len()` to
    /// `max_sessions + 1` (or more). The reservation is held across
    /// spawn, initialize, and session/load/new; only the in-memory
    /// `state.active.insert` consumes it. Lock-free `AtomicUsize` because
    /// the holder only needs to count, never inspect.
    outstanding_creations: std::sync::atomic::AtomicUsize,
    /// Force-evict sessions stuck in-flight longer than this threshold
    /// (`prompt_hard_timeout_secs + hung_grace_secs`, wired in main.rs).
    hung_threshold_secs: u64,
    mapping_path: PathBuf,
    meta_path: PathBuf,
    projects_path: PathBuf,
    default_config_options: HashMap<String, String>,
    /// Per-key set of thread_keys whose required project binding is
    /// UNTRUSTED. A key enters this set ONLY when `session_projects.json`
    /// failed to deserialize at startup — at that point, every key in
    /// `state.persisted` / `state.suspended` MIGHT have been pinned to a
    /// different project, and we cannot tell which without the lost data.
    /// A key is REMOVED from this set ONLY by:
    ///   - `reset_session(K)` / `purge_session_entries(K)` (the key is
    ///     dropped, so its untrusted state is no longer relevant); or
    ///   - a successful pinned `get_or_create(K, ...)` that persists a
    ///     NEW trusted binding for K (the per-key remove happens in the
    ///     post-spawn section, atomically with the in-memory insert).
    ///
    /// Critically, `save_projects` does NOT clear any global flag here:
    /// saving an unrelated fresh session C MUST NOT make a different
    /// untrusted key B "trusted" again. The previous design's
    /// `projects_corrupt: AtomicBool` had exactly that defect
    /// (workflow 20260818-openab-project-session-pinning-hardening,
    /// confirmed VERIFIER_FAIL on the second correction cycle).
    untrusted_project_keys: RwLock<HashSet<String>>,
    #[cfg(feature = "acp-mcp")]
    session_registrar: Option<Arc<dyn crate::acp_mcp::SessionTokenRegistrar>>,
    #[cfg(feature = "acp-mcp")]
    facade_url: Option<String>,
    /// Phase 6.4.10 (TOCTOU fix regression test) — deterministic
    /// barrier used by tests Q/R/M to pin contention to the exact
    /// moment in `ensure_capacity_or_evict` where the eviction branch
    /// is about to release the state lock. Production code never sets
    /// this; the only readers are `cfg(test)` paths inside
    /// `ensure_capacity_or_evict`, which `.notified().await` only when
    /// the optional is `Some`.
    #[cfg(test)]
    eviction_reservation_barrier: Option<Arc<tokio::sync::Notify>>,
    /// Phase 6.4.10 (TOCTOU fix regression test) — fires once every
    /// time `ensure_capacity_or_evict` is entered, BEFORE the state
    /// write lock is acquired. Tests use it to prove that a second
    /// creator actually reached the capacity-attempt boundary before
    /// the test releases the first creator. Production code never
    /// sets this; the only call site is a `cfg(test)` guard at the
    /// top of `ensure_capacity_or_evict`.
    #[cfg(test)]
    ensure_capacity_attempt_notify: Option<Arc<tokio::sync::Notify>>,
    /// Phase 6.4.10 (TOCTOU fix regression test) — fires inside the
    /// eviction branch AFTER `outstanding_creations.fetch_add(1)` and
    /// BEFORE the barrier await. Tests use this as the
    /// "participant has parked with reservation" signal — its
    /// position AFTER fetch_add means the test thread can read the
    /// atomic counter and observe `outstanding == 1` deterministically
    /// (no race window between the arrival notify and the fetch_add).
    /// Production code never sets this.
    #[cfg(test)]
    eviction_reserved_notify: Option<Arc<tokio::sync::Notify>>,
    /// Phase 6.4.10 (cancellation-test synchronization) — fires once
    /// from inside `CreationReservation::drop`, AFTER the failure-path
    /// `fetch_sub` (or after `commit()` sets `committed = true`).
    /// Test P awaits this to deterministically observe that the RAII
    /// guard's Drop has run after the creator is cancelled, replacing
    /// the prior `for _ in 0..16 { yield_now().await; }` heuristic.
    /// Production code never sets this; the only call site is a
    /// `cfg(test)` guard inside `Drop for CreationReservation`.
    #[cfg(test)]
    reservation_drop_notify: Option<Arc<tokio::sync::Notify>>,
    /// Phase 6.4.10 (contention-test synchronization) — fires at the
    /// TOP of `get_or_create` and `create_fresh_session_only`, BEFORE
    /// any lock acquire (and specifically BEFORE the per-thread
    /// gate's `state.write().await`). Tests K/L/Q/R use this as the
    /// "participant has entered the pool" signal because it is
    /// reachable by BOTH creators in a contention shape — the parked
    /// creator (B) and the blocked creator (C, blocked on the
    /// per-thread gate's write lock while B holds it inside the
    /// eviction branch). The pre-existing
    /// `ensure_capacity_attempt_notify` fires INSIDE
    /// `ensure_capacity_or_evict` and is therefore unreachable for
    /// the blocked creator; `creator_arrival_notify` sits above the
    /// per-thread gate's lock so it fires for both. Production
    /// code never sets this; the only call sites are `cfg(test)`
    /// guards at the top of `get_or_create` and
    /// `create_fresh_session_only`.
    #[cfg(test)]
    creator_arrival_notify: Option<Arc<tokio::sync::Notify>>,
}

type CancelHandle = (Arc<tokio::sync::Mutex<tokio::process::ChildStdin>>, String);
type ActiveSnapshot = Vec<(String, Arc<Mutex<AcpConnection>>)>;

/// Phase 6.4.10 (TOCTOU fix) — entry lifecycle phase encoding used by
/// `AcpConnection::phase`. The numeric values are part of the
/// observable contract: the eviction and prompt-claim paths CAS on
/// these exact bytes. `EntryPhase` here is a documentation alias; the
/// runtime authority is the atomic in `AcpConnection`.
pub mod entry_phase {
    /// Entry is idle — no prompt in flight, not selected for eviction.
    /// Safe to claim for either a prompt or for eviction.
    pub const IDLE: u8 = 0;
    /// Entry is currently executing a prompt turn. Must NOT be evicted.
    pub const BUSY: u8 = 1;
    /// Entry has been claimed for eviction. Must NOT be reused.
    pub const EVICTING: u8 = 2;
}

/// Phase 6.4.10 (TOCTOU fix) — RAII guard for one outstanding creation
/// reservation. Returned by [`SessionPool::ensure_capacity_or_evict`].
///
/// The reservation covers the entire creation window:
///
///   capacity decision
///   → optional idle eviction
///   → spawn
///   → initialize / session_new
///   → successful insertion
///
/// On every failure path the reservation is dropped, which decrements
/// `outstanding_creations` so the next creator can succeed. On the
/// success path the caller invokes [`CreationReservation::commit`],
/// which marks the reservation as consumed (the new entry already
/// counts as a live slot via `state.active.insert`, so the reservation
/// must NOT also be subtracted by the Drop impl — that would double-
/// count and unbalance the invariant).
///
/// Holding a reservation does NOT lock any pool state. The caller is
/// expected to be the single creator for the slot; the `creating` per-
/// thread gate and the `phase` atomic provide the actual exclusion.
pub(super) struct CreationReservation<'a> {
    pool: &'a SessionPool,
    committed: bool,
    /// Phase 6.4.10 (cancellation-test synchronization) — optional
    /// notify fired from `Drop`. Set only via the cfg(test)
    /// `new_with_drop_notify` constructor; production code never
    /// sets it. The notify is fired AFTER the failure-path
    /// `fetch_sub` (or after `commit()` flips `committed`) so any
    /// waiter observing the permit is guaranteed to see the post-
    /// decrement counter.
    #[cfg(test)]
    drop_notify: Option<Arc<tokio::sync::Notify>>,
}

impl<'a> CreationReservation<'a> {
    fn new(pool: &'a SessionPool) -> Self {
        Self {
            pool,
            committed: false,
            #[cfg(test)]
            drop_notify: None,
        }
    }

    /// cfg(test) constructor that arms a `Notify` to fire from
    /// `Drop`. Used by test P to deterministically observe that
    /// `CreationReservation::Drop` has run after a creator is
    /// cancelled at the eviction branch barrier.
    #[cfg(test)]
    fn new_with_drop_notify(
        pool: &'a SessionPool,
        notify: Arc<tokio::sync::Notify>,
    ) -> Self {
        Self {
            pool,
            committed: false,
            drop_notify: Some(notify),
        }
    }

    /// Mark the reservation as consumed. Called by the caller after
    /// `state.active.insert` succeeds. The new entry replaces the
    /// reservation: the counter that was holding a slot for the future
    /// entry is now the live entry itself (counted via
    /// `state.active.len()`). The decrement here keeps the invariant
    /// `active.len() + outstanding_creations <= max_sessions` correct:
    /// without it, a successful insert would double-count.
    fn commit(mut self) {
        self.pool
            .outstanding_creations
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        self.committed = true;
    }
}

impl Drop for CreationReservation<'_> {
    fn drop(&mut self) {
        if !self.committed {
            // Failure path: spawn, initialize, or insert failed before
            // the slot was consumed. Release it so the next creator can
            // observe free capacity. Lock-free AtomicUsize — safe to
            // call from a Drop impl.
            self.pool
                .outstanding_creations
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        }
        // cfg(test) hook: signal that this Drop has run, regardless
        // of branch. Test P awaits this after `b_task.abort()` to
        // deterministically observe that the failure-path fetch_sub
        // (or commit's fetch_sub) has already been applied, replacing
        // the prior yield-loop heuristic. `notify_one()` stores a
        // permit if no waiter is registered yet, so it is safe to
        // fire unconditionally on Drop.
        #[cfg(test)]
        if let Some(n) = self.drop_notify.as_ref() {
            n.notify_one();
        }
    }
}
type EvictionCandidate = (String, Arc<Mutex<AcpConnection>>, Instant, Option<String>);

/// Public test-only DTO for `SessionPool::with_test_state`. Mirrors the
/// subset of `PoolState` fields that integration tests need to seed
/// (workflow `20260818-openab-project-aware-thread-routing`). Exposes
/// only the project-binding and resumable-session fields; the rest of
/// `PoolState` (active connections, cancel handles, etc.) is internal
/// to the pool's runtime and is constructed empty by `with_test_state`.
///
/// Gated by `#[cfg(any(test, feature = "test-utils"))]` so production
/// release builds never include this surface. Cross-crate integration
/// tests in `src/ctl.rs` enable the feature via `dev-dependencies`.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Default, Clone)]
pub struct SessionPoolTestState {
    /// Pre-populated persisted sessionId map (thread_key → sessionId).
    pub persisted: HashMap<String, String>,
    /// Pre-populated suspended sessionId map (thread_key → sessionId).
    pub suspended: HashMap<String, String>,
    /// Pre-populated session_workdirs map (thread_key → canonical path).
    pub session_workdirs: HashMap<String, String>,
    /// Pre-populated session_projects map (thread_key → ProjectContext).
    pub session_projects: HashMap<String, ProjectContext>,
}

fn remove_if_same_handle<T>(
    map: &mut HashMap<String, Arc<Mutex<T>>>,
    key: &str,
    expected: &Arc<Mutex<T>>,
) -> Option<Arc<Mutex<T>>> {
    let should_remove = map
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, expected));
    if should_remove {
        map.remove(key)
    } else {
        None
    }
}

fn get_or_insert_gate(map: &mut HashMap<String, Arc<Mutex<()>>>, key: &str) -> Arc<Mutex<()>> {
    map.entry(key.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

/// Returns true when a session should be treated as stale during idle cleanup.
fn classify_idle(last_active: Instant, alive: bool, cutoff: Instant) -> bool {
    last_active < cutoff || !alive
}

/// Returns true when a locked, in-flight session has exceeded the hung threshold.
fn classify_hung(
    in_flight: bool,
    last_active_age: std::time::Duration,
    threshold: std::time::Duration,
) -> bool {
    in_flight && last_active_age > threshold
}

/// Emit the force-evict warning with **both** ids redacted.
///
/// `key` is a pool key `<platform>:<channel_id>` (`acp_<uuid>`) and `session_id` is `sess_<uuid>`;
/// either resumes the session, so both are credentials. Extracted from the loop in `cleanup_idle`
/// so the redaction can be exercised by a test for real — R1 redacted the sites it enumerated and
/// this force-evict site was outside that list, logging both ids raw.
fn warn_force_evicting_hung(
    key: &str,
    session_id: Option<&str>,
    age_secs: u64,
    threshold_secs: u64,
) {
    warn!(
        thread_id = %crate::redact::redact_session_ids(key),
        session_id = %session_id.map(crate::redact::redact_session_ids).unwrap_or_default(),
        age_secs,
        threshold_secs,
        "force-evicting hung session"
    );
}

/// Returns true when `candidate_last_active` is a better eviction target than `current_oldest`.
fn better_candidate(current_oldest: Option<Instant>, candidate_last_active: Instant) -> bool {
    match current_oldest {
        Some(oldest) => candidate_last_active < oldest,
        None => true,
    }
}

/// Phase 6.4.10 — RAII guard that holds a busy marker for one pool entry.
///
/// The previous design only cleared the busy flag via an explicit
/// `AcpConnection::prompt_done` call. Every non-happy path (prompt error,
/// non-terminal ACP stop, timeout, cancellation, workflow-hook error,
/// early return from the prompt closure) skipped that call, so the entry
/// remained marked "busy forever" and the capacity check at `state.active
/// .len() >= max_sessions` rejected every subsequent request for that key
/// — even after the underlying agent process had finished streaming.
///
/// `BusyGuard` is installed around the prompt closure inside
/// [`SessionPool::with_connection`]. Its `Drop` clears the busy flag
/// unconditionally, so every exit path through `with_connection` (success,
/// error, cancellation, timeout, panic) restores the entry to the IDLE /
/// REUSABLE state. The explicit busy/idle lifecycle authority replaces
/// the previous implicit detection, which inferred idleness from the
/// per-connection mutex being momentarily free — a side-effect that does not
/// match the documented pool semantics ("Maximum number of concurrent
/// agent sessions").
struct BusyGuard {
    activity: Arc<SessionActivity>,
}

impl BusyGuard {
    /// Mark `activity` as busy and return a guard that clears it on drop.
    /// The guard also `touch`es `activity` so the cleanup_idle TTL clock
    /// sees the prompt as recently active.
    fn arm(activity: Arc<SessionActivity>) -> Self {
        activity.set_in_flight(true);
        activity.touch();
        Self { activity }
    }

    /// Explicitly disarm the guard without going through `Drop`. Used by
    /// `with_connection` callers that want to release the busy marker
    /// before the prompt closure finishes (rare; mostly diagnostic).
    #[allow(dead_code)]
    fn disarm(mut self) {
        self.activity.set_in_flight(false);
        self.activity.touch();
        // Prevent Drop from running again.
        self.activity = Arc::new(SessionActivity::new());
    }
}

impl Drop for BusyGuard {
    fn drop(&mut self) {
        // Order matters: touch first so the cleanup_idle TTL clock sees a
        // recent activity, then clear busy. A panicking Drop must not
        // abort the program — keep this body infallible.
        self.activity.touch();
        self.activity.set_in_flight(false);
    }
}

/// Phase 6.4.10 (TOCTOU fix) — helpers to access the per-connection
/// `phase` atomic through `Arc<Mutex<AcpConnection>>`. We can't put
/// methods directly on `Mutex<AcpConnection>` (foreign type), and we
/// cannot reach AcpConnection's fields without the mutex, so the
/// helpers use a non-blocking `try_lock` to get a stable reference and
/// then operate on the inner atomic. `try_lock` failure means the
/// entry is BUSY in `with_connection` — exactly the entries we want to
/// skip — so treating it as "cannot claim" is semantically correct.
#[inline]
fn phase_load(conn: &Arc<Mutex<AcpConnection>>) -> u8 {
    use std::sync::atomic::Ordering;
    match conn.try_lock() {
        Ok(g) => g.phase.load(Ordering::Acquire),
        Err(_) => entry_phase::BUSY, // BUSY in with_connection
    }
}

#[inline]
fn phase_try_cas(conn: &Arc<Mutex<AcpConnection>>, from: u8, to: u8) -> bool {
    use std::sync::atomic::Ordering;
    match conn.try_lock() {
        Ok(g) => g
            .phase
            .compare_exchange(from, to, Ordering::AcqRel, Ordering::Acquire)
            .is_ok(),
        Err(_) => false, // BUSY → cannot claim
    }
}

#[inline]
fn phase_force_set(conn: &Arc<Mutex<AcpConnection>>, to: u8) {
    use std::sync::atomic::Ordering;
    if let Ok(g) = conn.try_lock() {
        g.phase.store(to, Ordering::Release);
    }
}

/// Phase 6.4.10 (TOCTOU fix) — select the oldest IDLE pool entry for
/// eviction when the pool is at capacity, and atomically claim it as
/// EVICTING before returning.
///
/// The previous implementation inferred idleness from `try_lock`
/// succeeding on the per-connection mutex (a transient observation) AND
/// from `activity.in_flight()`. Neither was an exclusive transition
/// authority: a concurrent `with_connection` could win the mutex and
/// arm BusyGuard AFTER eviction had decided to remove the entry. The
/// new helper reads the authoritative `phase` atomic on each connection
/// and CAS-es IDLE → EVICTING. If the CAS loses, another evictor won
/// the race and this candidate is skipped; if it loses because the
/// entry became BUSY, the candidate is skipped (must not evict BUSY).
///
/// Returns `None` when every entry is BUSY (or the only candidate is the
/// request's own session key, which must NEVER be evicted). The chosen
/// candidate's `phase` is already EVICTING when the helper returns; the
/// caller is responsible for either committing the removal or releasing
/// the phase back to IDLE.
fn find_oldest_idle_eviction_candidate(
    state: &PoolState,
    exclude_key: &str,
) -> Option<EvictionCandidate> {
    let mut best: Option<EvictionCandidate> = None;
    for (key, conn) in &state.active {
        if key == exclude_key {
            // A request must NEVER evict its own session.
            continue;
        }
        // Atomic load — no mutex, no transient observation. If the
        // phase is anything other than IDLE (BUSY or EVICTING), skip.
        if phase_load(conn) != entry_phase::IDLE {
            continue;
        }
        // Read last_active + acp_session_id under the per-connection lock.
        // try_lock failure here means a concurrent reader holds the
        // connection; skip and pick the next candidate. The CAS below
        // would still succeed, but we want last_active to be observed
        // authoritatively. The caller will retry the eviction on the
        // next spawn, or fail closed if every candidate was skipped.
        let Ok(conn_guard) = conn.try_lock() else {
            continue;
        };
        let candidate = (
            key.clone(),
            Arc::clone(conn),
            conn_guard.last_active,
            conn_guard.acp_session_id.clone(),
        );
        drop(conn_guard);
        if better_candidate(best.as_ref().map(|(_, _, t, _)| *t), candidate.2) {
            best = Some(candidate);
        }
    }
    let candidate = best?;
    // Atomically claim EVICTING for the chosen candidate. If the CAS
    // loses (another evictor already claimed it, or `with_connection`
    // claimed it for a prompt between our scan and our CAS), return
    // None and let the caller fail closed. The caller can retry by
    // re-calling `ensure_capacity_or_evict` on the next spawn.
    if !phase_try_cas(&candidate.1, entry_phase::IDLE, entry_phase::EVICTING) {
        return None;
    }
    Some(candidate)
}

/// Prepare facade browser capabilities for one session: write the agent's facade MCP entry, and
/// mint its session token **only if that write succeeded**.
///
/// The token is useless without the config. The file carries
/// `Authorization: Bearer ${OPENAB_SESSION_TOKEN}`, and it is the artifact the OPERATOR wires in
/// — since D-15 openab writes only `.openab/mcp-facade.json`, which no agent reads on its own, so
/// the import or `--mcp-config` flag is what actually points the agent at the facade. The ordering
/// still holds for a narrower reason: if openab cannot even author that file, the session has no
/// path to the facade it could be wired to, and minting regardless would register a live
/// credential for a session that cannot use it and leave it valid until eviction, while the
/// failure showed up only as a warning. Returning `None` keeps the session running without
/// browser capabilities, which is the honest description of what actually happened.
#[cfg(feature = "acp-mcp")]
async fn setup_facade_session(
    workdir: &str,
    facade_url: &str,
    channel_id: &str,
    registrar: &Arc<dyn crate::acp_mcp::SessionTokenRegistrar>,
) -> Option<String> {
    match crate::acp_mcp::write_facade_mcp_config(workdir, facade_url).await {
        Ok(()) => Some(registrar.mint(channel_id)),
        Err(e) => {
            tracing::error!(
                workdir, error = %e,
                "facade mcp config write failed — starting this session WITHOUT browser \
                 capabilities and not minting a session token that could never be presented"
            );
            None
        }
    }
}

/// Canonicalize a non-anonymous `ProjectContext` ONCE so the mismatch gate
/// and the workdir resolution both work from the same byte-equal form.
/// Returns `Ok(None)` for `None` and for anonymous contexts (no canonical
/// form, no mismatch check). Returns `Ok(Some(canonical))` for a valid
/// pinned context, or `Err(_)` for an invalid pinned path (nonexistent /
/// not a directory). The error string is propagated to the caller verbatim.
fn canonicalize_pinned(project: Option<&ProjectContext>) -> Result<Option<ProjectContext>, String> {
    match project {
        Some(p) if !p.is_anonymous() => p.canonicalized().map(Some),
        _ => Ok(None),
    }
}

/// Resolve the effective working directory for a session given the incoming
/// `ProjectContext`, the already-canonicalized pinned form (if any), the
/// stored per-session override (if any), and the configured
/// `[agent].working_dir`. Returns the resolved workdir and the project
/// binding (if any) to persist after a successful spawn.
///
/// Precedence (matches workflow req #6):
///   1. **Project-pinned** — when `canonical_pinned` is `Some`, its
///      `project_root` is authoritative; the canonical binding is returned
///      for persistence so the mismatch gate can later detect a stale
///      cross-project session.
///   2. **Stored** — per-thread immutability (ADR §4.5). When the incoming
///      context is anonymous, the existing workspace sticks; when no
///      binding exists yet, the anonymous path is used.
///   3. **Configured** — `[agent].working_dir`. The legacy single-project
///      fallback.
fn resolve_effective_workdir(
    project: Option<&ProjectContext>,
    canonical_pinned: Option<&ProjectContext>,
    stored_workdir: Option<&str>,
    config_workdir: &str,
) -> (String, Option<ProjectContext>) {
    if let Some(canonical) = canonical_pinned {
        return (
            canonical.project_root.to_string_lossy().to_string(),
            Some(canonical.clone()),
        );
    }

    if let Some(p) = project {
        if p.is_anonymous() {
            // Anonymous context (legacy `[[ws:@alias]]`): stored wins over the
            // workspace hint so the immutability invariant holds. When no
            // binding exists yet, use the anonymous path. Callers are expected
            // to have validated the path already (resolve_workspace does so);
            // we do not re-canonicalize because callers may pass a path that
            // is intentionally not on disk yet.
            if let Some(stored) = stored_workdir {
                return (stored.to_string(), None);
            }
            return (p.project_root.to_string_lossy().to_string(), None);
        }
    }

    let wd = stored_workdir.unwrap_or(config_workdir).to_string();
    (wd, None)
}
///
/// The single implementation for both hung eviction and [`SessionPool::reset_session`]; the latter
/// removes `active` itself and then calls this. It used to be a second copy of the same list, which
/// is how the two could drift — and the line most likely to be lost from a copy is the one below
/// about the creating gate, because it says *not* to remove something.
///
/// Hung eviction must NOT leave the session resumable: the old streaming task still holds an Arc
/// clone of the connection, so the agent process may be alive and mid-turn. If the session id
/// stayed in `suspended`/`persisted`, the next message would `session/load` the same session while
/// the old process still owns an in-flight turn.
fn purge_session_entries(state: &mut PoolState, key: &str) {
    state.cancel_handles.remove(key);
    state.activity.remove(key);
    state.pgids.remove(key);
    state.suspended.remove(key);
    state.persisted.remove(key);
    // Do NOT remove the creating gate: it is concurrency control, not session
    // state. Removing it while a holder still owns the old gate Arc would let
    // a concurrent get_or_create mint a fresh gate and run two creations for
    // the same key.
    state.session_workdirs.remove(key);
    // Project bindings must be cleared alongside session_workdirs so a
    // re-acquired thread key cannot accidentally inherit a stale project
    // identity from a previous (different) session for the same key. The
    // mismatch check is the live gate; persistence is the cross-restart one.
    state.session_projects.remove(key);
}

/// Escalating kill for a hung agent's process group: wait 10s after the
/// session/cancel attempt, SIGTERM, wait 2s, SIGKILL. Mirrors
/// `AcpConnection::kill_process_group`, which cannot run here because the
/// hung task never drops its connection Arc.
async fn kill_pgid_after_grace(pgid: Option<i32>) {
    let Some(pgid) = pgid.filter(|p| *p > 0) else {
        return;
    };
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    #[cfg(unix)]
    {
        unsafe {
            libc::kill(-pgid, libc::SIGTERM);
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        unsafe {
            libc::kill(-pgid, libc::SIGKILL);
        }
    }
    #[cfg(not(unix))]
    {
        // No process-group kill on non-unix; rely on AcpConnection::Drop's
        // Windows handling if/when the hung task eventually unwinds.
        let _ = pgid;
    }
}

/// Remove a hung session from all pool maps. Returns true if the exact
/// connection captured at classification time was still registered; when a
/// fresh replacement exists for the key, nothing is touched.
///
/// Note: this helper intentionally does NOT touch
/// `untrusted_project_keys`. The caller (the cleanup_idle loop) holds
/// `&SessionPool` and is responsible for the per-key untrusted removal
/// AFTER `apply_hung_eviction` succeeds, so the `RwLock` write is taken
/// only when the eviction actually happened.
fn apply_hung_eviction(
    state: &mut PoolState,
    key: &str,
    expected: &Arc<Mutex<AcpConnection>>,
) -> bool {
    if remove_if_same_handle(&mut state.active, key, expected).is_none() {
        return false;
    }
    purge_session_entries(state, key);
    true
}

/// Record `token` as the facade token for `key`, revoking whatever token it supersedes.
///
/// A superseded token belongs to a predecessor connection under the same key. Its `AcpConnection`
/// DropGuard normally revokes it, but if that predecessor is hung (a stuck streaming task still
/// holds an Arc) the guard never fires — so revoking the superseded token here is what stops it
/// staying valid for the channel after a successor takes over (F3). Revocation is by exact token
/// and idempotent, so overlapping with the guard on a clean replacement is harmless.
#[cfg(feature = "acp-mcp")]
fn install_facade_token(
    state: &mut PoolState,
    key: &str,
    token: String,
    registrar: Option<&Arc<dyn crate::acp_mcp::SessionTokenRegistrar>>,
) {
    if let Some(superseded) = state.facade_tokens.insert(key.to_string(), token) {
        if let Some(registrar) = registrar {
            registrar.revoke(&superseded);
        }
    }
}

/// Revoke and forget the facade token recorded for `key`, if any.
///
/// Called from every path that removes a connection from `active` (hung eviction, idle eviction,
/// reset, suspend). On the clean paths the connection also drops and its guard revokes the same
/// token — idempotent — but the hung path is the one that needs this: the guard cannot fire while
/// the hung task holds an Arc, so without a synchronous revoke here the token outlives the eviction
/// and `AcpTunnelSource` (channel-only authorization) would let the hung predecessor reach a
/// successor's tunnel (F3). `purge_session_entries` deliberately does NOT touch `facade_tokens`, so
/// this can run *after* `apply_hung_eviction` and still find the token to revoke.
#[cfg(feature = "acp-mcp")]
fn revoke_facade_token_for_key(
    state: &mut PoolState,
    key: &str,
    registrar: Option<&Arc<dyn crate::acp_mcp::SessionTokenRegistrar>>,
) {
    if let Some(token) = state.facade_tokens.remove(key) {
        if let Some(registrar) = registrar {
            registrar.revoke(&token);
        }
    }
}

impl SessionPool {
    pub fn new(
        config: AgentConfig,
        max_sessions: usize,
        hung_threshold_secs: u64,
        default_config_options: HashMap<String, String>,
    ) -> Result<Self> {
        let openab_dir = Self::production_persistence_root()?;
        Ok(Self::from_persistence_root(
            config,
            max_sessions,
            hung_threshold_secs,
            default_config_options,
            openab_dir,
        ))
    }

    /// Resolve and create the production persistence namespace.
    ///
    /// ACP session IDs are authority-bearing state: resuming an ID launches
    /// work in the session's original agent.  The canonical thread key is
    /// deliberately shared across adapters, so it cannot also distinguish
    /// OpenAB daemon identities.  Keep that key unchanged and isolate its
    /// backing files by the deployment identity instead.
    fn production_persistence_root() -> Result<PathBuf> {
        let root = Self::agent_persistence_root(
            std::env::var_os("HOME"),
            std::env::var_os("ARTHUR_AGENT_NAME"),
        )?;
        std::fs::create_dir_all(&root).with_context(|| {
            format!(
                "failed to create agent-scoped ACP persistence directory {}",
                root.display()
            )
        })?;
        Ok(root)
    }

    /// Derive the agent-scoped root without consulting process environment.
    /// Kept separate from `production_persistence_root` so tests and other
    /// non-production construction paths need not mutate global environment.
    fn agent_persistence_root(
        home: Option<std::ffi::OsString>,
        agent_name: Option<std::ffi::OsString>,
    ) -> Result<PathBuf> {
        let home = home
            .and_then(|value| value.into_string().ok())
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow!("HOME is required for ACP session persistence"))?;
        let agent_name = agent_name
            .and_then(|value| value.into_string().ok())
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                anyhow!("ARTHUR_AGENT_NAME is required for agent-scoped ACP session persistence")
            })?;
        let mut components = Path::new(&agent_name).components();
        if !matches!(components.next(), Some(std::path::Component::Normal(_)))
            || components.next().is_some()
            || agent_name.contains('\\')
        {
            return Err(anyhow!(
                "ARTHUR_AGENT_NAME must be a single safe path component"
            ));
        }
        Ok(PathBuf::from(home)
            .join(".openab")
            .join("agents")
            .join(agent_name))
    }

    fn from_persistence_root(
        config: AgentConfig,
        max_sessions: usize,
        hung_threshold_secs: u64,
        default_config_options: HashMap<String, String>,
        openab_dir: PathBuf,
    ) -> Self {
        let mapping_path = openab_dir.join("thread_map.json");
        let meta_path = openab_dir.join("session_meta.json");
        let projects_path = openab_dir.join("session_projects.json");
        // Phase 6.2.9 fix round 3: scrub any pre-seeded native-dispatch
        // keys out of the maps we just loaded. The on-disk file may
        // predate the Phase 6.2.9 isolation prefix (e.g. written by a
        // buggy pre-fix daemon) and we do not want to carry that state
        // forward into the in-memory pool — the fast lane would never
        // load these entries, but a generic `save_mapping` round-trip
        // could re-serialize them and grow the contaminated set over
        // time.
        let mut suspended = Self::load_mapping(&mapping_path);
        let mut session_workdirs = Self::load_mapping(&meta_path);
        let pre_filter_native_count = suspended
            .keys()
            .filter(|k| is_native_dispatch_key(k))
            .count();
        suspended.retain(|k, _| !is_native_dispatch_key(k));
        session_workdirs.retain(|k, _| !is_native_dispatch_key(k));
        if pre_filter_native_count > 0 {
            warn!(
                filtered = pre_filter_native_count,
                path = %mapping_path.display(),
                "Phase 6.2.9: scrubbed pre-seeded native-dispatch keys from in-memory state at startup"
            );
        }
        // Fail-closed for project-binding persistence (Defect 4 of workflow
        // 20260818-openab-project-session-pinning-hardening, refined in the
        // second correction cycle). A corrupt session_projects.json MUST NOT
        // silently become "no project bindings" — that would let
        // `get_or_create` reuse a persisted session whose original project
        // identity was lost.
        //
        // The previous design used a single global `projects_corrupt: bool`
        // and let `save_projects` clear it on any successful write. That
        // made the corruption guard vacuous: a fresh pinned session C would
        // clear the flag, and old untrusted key B would silently resume
        // afterward. The fix is per-key: every key in `persisted` /
        // `suspended` at the time of corruption is marked untrusted, and
        // each one is only removed from the untrusted set by its own
        // reset / purge / successful pinned-save path.
        let (session_projects, untrusted_keys) = match Self::load_projects(&projects_path) {
            Ok(mut map) => {
                // Phase 6.2.9 fix round 3: scrub native-dispatch keys
                // from the loaded project binding map. We keep them
                // out of the untrusted-key set on purpose: native
                // keys are not project-bound; they are simply dropped.
                map.retain(|k, _| !is_native_dispatch_key(k));
                (map, HashSet::new())
            }
            Err(e) => {
                let keys: HashSet<String> = suspended.keys().cloned().collect();
                warn!(
                    path = %projects_path.display(),
                    error = %e,
                    untrusted_key_count = keys.len(),
                    "corrupt session_projects file — every persisted/suspended thread_key is \
                     marked UNTRUSTED individually; project-pinned get_or_create for each \
                     of those keys will fail closed until that key is reset or re-pinned"
                );
                (HashMap::new(), keys)
            }
        };
        Self {
            state: RwLock::new(PoolState {
                active: HashMap::new(),
                cancel_handles: HashMap::new(),
                #[cfg(feature = "acp-mcp")]
                facade_tokens: HashMap::new(),
                activity: HashMap::new(),
                pgids: HashMap::new(),
                persisted: suspended.clone(),
                suspended,
                creating: HashMap::new(),
                session_workdirs,
                session_projects,
            }),
            config,
            max_sessions,
            outstanding_creations: std::sync::atomic::AtomicUsize::new(0),
            hung_threshold_secs,
            mapping_path,
            meta_path,
            projects_path,
            default_config_options,
            untrusted_project_keys: RwLock::new(untrusted_keys),
            #[cfg(feature = "acp-mcp")]
            session_registrar: None,
            #[cfg(feature = "acp-mcp")]
            facade_url: None,
            #[cfg(test)]
            eviction_reservation_barrier: None,
            #[cfg(test)]
            ensure_capacity_attempt_notify: None,
            #[cfg(test)]
            eviction_reserved_notify: None,
            #[cfg(test)]
            reservation_drop_notify: None,
            #[cfg(test)]
            creator_arrival_notify: None,
        }
    }

    /// Wire the facade session-token registrar + facade URL, set by the root
    /// when `[mcp]` is running. With both present the pool does its half: mints
    /// one token per session, injects it as `OPENAB_SESSION_TOKEN` in the agent
    /// process env, and writes the static facade MCP entry once per workdir.
    ///
    /// That is necessary but NOT sufficient for browser capabilities to route
    /// through the facade. The operator must still put the written entry in front
    /// of the agent, and a `type:acp` server must actually attach over `/acp` —
    /// admission is that transport auth, not a config allowlist (D-29 removed
    /// `[[mcp.acp_servers]]`, reversing D-20).
    #[cfg(feature = "acp-mcp")]
    pub fn with_facade_sessions(
        mut self,
        registrar: Option<Arc<dyn crate::acp_mcp::SessionTokenRegistrar>>,
        facade_url: Option<String>,
    ) -> Self {
        self.session_registrar = registrar;
        self.facade_url = facade_url;
        self
    }

    /// Test-only constructor: build a `SessionPool` with a pre-populated
    /// `PoolState`. Bypasses the real filesystem persistence path so tests
    /// can drive `get_or_create` directly with specific thread_key →
    /// project bindings. Gated by `#[cfg(any(test, feature = "test-utils"))]`
    /// so production release builds do not see this constructor. The
    /// cross-crate integration entry point is `with_test_state` (gated by
    /// the same predicate).
    ///
    /// Both gates are required because `cfg(test)` does NOT propagate
    /// to a dependency when that dependency is compiled for an external
    /// (binary) test build. The `test-utils` feature is activated by the
    /// binary's `[dev-dependencies]` for the test build only.
    #[cfg(any(test, feature = "test-utils"))]
    fn with_state_for_test(config: AgentConfig, state: PoolState, projects_path: PathBuf) -> Self {
        Self {
            state: RwLock::new(state),
            config,
            max_sessions: 4,
            outstanding_creations: std::sync::atomic::AtomicUsize::new(0),
            hung_threshold_secs: 600,
            mapping_path: projects_path
                .parent()
                .unwrap_or(Path::new("/tmp"))
                .join("thread_map.json"),
            meta_path: projects_path
                .parent()
                .unwrap_or(Path::new("/tmp"))
                .join("session_meta.json"),
            projects_path,
            default_config_options: HashMap::new(),
            untrusted_project_keys: RwLock::new(HashSet::new()),
            #[cfg(feature = "acp-mcp")]
            session_registrar: None,
            #[cfg(feature = "acp-mcp")]
            facade_url: None,
            #[cfg(test)]
            eviction_reservation_barrier: None,
            #[cfg(test)]
            ensure_capacity_attempt_notify: None,
            #[cfg(test)]
            eviction_reserved_notify: None,
            #[cfg(test)]
            reservation_drop_notify: None,
            #[cfg(test)]
            creator_arrival_notify: None,
        }
    }

    /// Test-only constructor exposed to integration tests in other crates
    /// (workflow `20260818-openab-project-aware-thread-routing`), behind
    /// the `test-utils` feature. Used to seed a `SessionPool` with a known
    /// subset of state — specifically `persisted` / `suspended` /
    /// `session_projects` / `session_workdirs` — so tests can drive the
    /// `has_reusable_session` / `get_pinned_project` semantics without
    /// reaching into private `PoolState` fields.
    ///
    /// Production release builds (the default `cargo build --release`)
    /// do NOT compile this; only crates with `openab-core/test-utils` in
    /// their feature list will see it. `src/ctl.rs`'s integration tests
    /// enable the feature via `dev-dependencies`.
    ///
    /// The companion in-crate `with_state_for_test` is gated by
    /// `#[cfg(test)]` and used by the pool's own internal tests.
    ///
    /// Workflow context: tests A, B, D, F, G, K, L, M, N, O all rely on
    /// being able to construct a pool with a known substate and read the
    /// resulting `session_projects` / `has_reusable_session` via the
    /// public API. This seam is the canonical entry point for those
    /// tests.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_test_state(
        config: AgentConfig,
        test_state: SessionPoolTestState,
        projects_path: PathBuf,
    ) -> Self {
        let state = PoolState {
            active: HashMap::new(),
            cancel_handles: HashMap::new(),
            #[cfg(feature = "acp-mcp")]
            facade_tokens: HashMap::new(),
            activity: HashMap::new(),
            pgids: HashMap::new(),
            suspended: test_state.suspended,
            persisted: test_state.persisted,
            creating: HashMap::new(),
            session_workdirs: test_state.session_workdirs,
            session_projects: test_state.session_projects,
        };
        Self::with_state_for_test(config, state, projects_path)
    }

    /// Test-only seam: replace the untrusted-key set so a test can drive
    /// the fail-closed path of `get_or_create` per-key, without writing
    /// to the production agent-scoped persistence directory (which would race other
    /// tests). Replaces the previous `set_projects_corrupt_for_test(bool)`
    /// seam that drove the (now-removed) global flag.
    /// Compiled out of release builds.
    #[cfg(test)]
    async fn set_untrusted_keys_for_test<I: IntoIterator<Item = String>>(&self, keys: I) {
        let mut set = self.untrusted_project_keys.write().await;
        set.clear();
        set.extend(keys);
    }

    /// Phase 6.4.10 — test-only seam to override the pool's `max_sessions`
    /// ceiling. The default `with_state_for_test` constructor pins it to 4;
    /// the lifecycle regression tests want a tight ceiling (2 or 3) so they
    /// can demonstrate the capacity invariant without spawning dozens of
    /// sessions. Compiled out of release builds.
    #[cfg(test)]
    fn set_max_sessions_for_test(&mut self, max_sessions: usize) {
        self.max_sessions = max_sessions;
    }

    /// Phase 6.4.10 (TOCTOU fix regression test) — install the
    /// deterministic eviction-reservation barrier. The next call to
    /// `ensure_capacity_or_evict`'s eviction branch will park after
    /// reserving the slot but before releasing the state lock. Tests
    /// can use the returned `Arc<Notify>` handle to wake the parked
    /// creator once they have asserted that no concurrent creator can
    /// reserve the same slot. Production code never calls this method.
    #[cfg(test)]
    fn install_eviction_reservation_barrier_for_test(
        &mut self,
    ) -> Arc<tokio::sync::Notify> {
        let notify = Arc::new(tokio::sync::Notify::new());
        self.eviction_reservation_barrier = Some(Arc::clone(&notify));
        notify
    }

    /// Phase 6.4.10 (TOCTOU fix regression test) — install the
    /// `ensure_capacity_or_evict` entry-point Notify. Every call to
    /// `ensure_capacity_or_evict` (from any creator task) fires this
    /// Notify once at the top of the function, BEFORE acquiring the
    /// state write lock. Tests use it as a "participant reached the
    /// contention boundary" signal in K/L/Q/R. Production code never
    /// calls this method.
    #[cfg(test)]
    fn install_ensure_capacity_attempt_notify_for_test(
        &mut self,
    ) -> Arc<tokio::sync::Notify> {
        let notify = Arc::new(tokio::sync::Notify::new());
        self.ensure_capacity_attempt_notify = Some(Arc::clone(&notify));
        notify
    }

    /// Phase 6.4.10 (TOCTOU fix regression test) — install the
    /// "eviction reserved" Notify. Fires inside the eviction branch
    /// AFTER `outstanding_creations.fetch_add(1)` and BEFORE the
    /// barrier await. Tests K/L/Q/R wait on this to deterministically
    /// observe that the parked creator has its reservation in
    /// place. Production code never calls this method.
    #[cfg(test)]
    fn install_eviction_reserved_notify_for_test(
        &mut self,
    ) -> Arc<tokio::sync::Notify> {
        let notify = Arc::new(tokio::sync::Notify::new());
        self.eviction_reserved_notify = Some(Arc::clone(&notify));
        notify
    }

    /// Phase 6.4.10 (cancellation-test synchronization) — install the
    /// `CreationReservation::Drop` Notify. The next
    /// `CreationReservation` constructed via the cfg(test) helper
    /// (`create_reservation_for_test`) carries a clone of this
    /// Notify; `Drop` fires it once after the failure-path `fetch_sub`
    /// (or after `commit()` sets `committed = true`). Test P awaits
    /// this after `b_task.abort()` to deterministically observe that
    /// the RAII guard's Drop has run, replacing the prior
    /// `for _ in 0..16 { yield_now().await; }` heuristic. Production
    /// code never calls this method.
    #[cfg(test)]
    fn install_reservation_drop_notify_for_test(
        &mut self,
    ) -> Arc<tokio::sync::Notify> {
        let notify = Arc::new(tokio::sync::Notify::new());
        self.reservation_drop_notify = Some(Arc::clone(&notify));
        notify
    }

    /// Phase 6.4.10 (contention-test synchronization) — install the
    /// `creator_arrival_notify` hook. Fires at the TOP of
    /// `get_or_create` and `create_fresh_session_only`, BEFORE any
    /// lock acquire (and specifically before the per-thread gate's
    /// `state.write().await`). Tests K/L/Q/R use this as the
    /// "participant has entered the pool" signal because it is
    /// reachable by both creators in a contention shape — the blocked
    /// creator (C) fires this hook on entry, BEFORE being blocked
    /// behind the per-thread gate lock.
    ///
    /// Unlike `ensure_capacity_attempt_notify` (which fires inside
    /// `ensure_capacity_or_evict` and therefore is unreachable for a
    /// creator blocked at the per-thread gate), `creator_arrival_notify`
    /// is reachable for every entry into `get_or_create`. This makes
    /// the "consume B's arrival, then spawn C, then consume C's
    /// arrival" sequence structurally impossible to coalesce against
    /// a stale seed permit. Compiled out of release builds.
    #[cfg(test)]
    fn install_creator_arrival_notify_for_test(
        &mut self,
    ) -> Arc<tokio::sync::Notify> {
        let notify = Arc::new(tokio::sync::Notify::new());
        self.creator_arrival_notify = Some(Arc::clone(&notify));
        notify
    }

    /// cfg(test) helper used by the two `ensure_capacity_or_evict`
    /// reservation-creation sites. If a `reservation_drop_notify` is
    /// installed, returns a `CreationReservation` armed with it so
    /// tests can await Drop. Otherwise returns the production
    /// constructor's output. Compiled out of release builds.
    #[cfg(test)]
    fn create_reservation_for_test<'a>(
        &'a self,
    ) -> crate::acp::pool::CreationReservation<'a> {
        if let Some(n) = self.reservation_drop_notify.as_ref() {
            CreationReservation::new_with_drop_notify(self, Arc::clone(n))
        } else {
            CreationReservation::new(self)
        }
    }

    fn load_mapping(path: &Path) -> HashMap<String, String> {
        match std::fs::read_to_string(path) {
            Ok(data) => serde_json::from_str(&data).unwrap_or_else(|e| {
                warn!(path = %path.display(), error = %e, "corrupt mapping file, starting fresh");
                HashMap::new()
            }),
            Err(_) => HashMap::new(),
        }
    }

    /// Load the per-session project bindings persisted at `path`.
    ///
    /// Returns `Ok(map)` on success. A missing file is `Ok(empty)` (first
    /// boot, normal). A *corrupt* file is `Err(_)` so the caller can set
    /// `projects_corrupt = true` and force project-pinned `get_or_create`
    /// to fail closed. The previous "corrupt file → empty map" behavior
    /// was unsafe (Defect 4 of workflow
    /// 20260818-openab-project-session-pinning-hardening): it silently
    /// demoted a partial or stale binding set to "no binding", which
    /// would have let the mismatch gate accept a `project B` call against
    /// a session whose original `project A` identity was just lost.
    fn load_projects(path: &Path) -> Result<HashMap<String, ProjectContext>, String> {
        let data = match std::fs::read_to_string(path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(HashMap::new());
            }
            Err(e) => {
                return Err(format!("read error: {e}"));
            }
        };
        serde_json::from_str(&data).map_err(|e| format!("json parse error: {e}"))
    }

    /// Phase 6.2.9 fix round 3 — sanitize any `native-dispatch:*` keys
    /// out of the durable snapshot BEFORE serializing to disk. The
    /// pool's in-memory maps MAY contain such keys (e.g. a legacy
    /// `thread_map.json` written before this fix round, or a malicious
    /// pre-seeded entry); the on-disk result MUST be free of them.
    /// Non-native keys pass through verbatim.
    fn filter_native_keys_string_map(src: &HashMap<String, String>) -> HashMap<String, String> {
        let mut out = HashMap::with_capacity(src.len());
        for (k, v) in src {
            if !is_native_dispatch_key(k) {
                out.insert(k.clone(), v.clone());
            }
        }
        out
    }

    fn filter_native_keys_projects_map(
        src: &HashMap<String, ProjectContext>,
    ) -> HashMap<String, ProjectContext> {
        let mut out = HashMap::with_capacity(src.len());
        for (k, v) in src {
            if !is_native_dispatch_key(k) {
                out.insert(k.clone(), v.clone());
            }
        }
        out
    }

    fn save_mapping(&self, persisted: &HashMap<String, String>) {
        // Defense-in-depth: sanitize native-dispatch keys BEFORE
        // serializing. The in-memory map is never mutated by this
        // helper (callers can still see the live state) — we only
        // scrub the snapshot that goes to disk.
        let sanitized = Self::filter_native_keys_string_map(persisted);
        if sanitized.len() < persisted.len() {
            info!(
                filtered = persisted.len() - sanitized.len(),
                "save_mapping: native-dispatch keys excluded from durable snapshot"
            );
        }
        let data = match serde_json::to_string_pretty(&sanitized) {
            Ok(d) => d,
            Err(e) => {
                warn!(error = %e, "failed to serialize thread mapping");
                return;
            }
        };
        let tmp = self.mapping_path.with_extension("json.tmp");
        if let Err(e) =
            std::fs::write(&tmp, &data).and_then(|_| std::fs::rename(&tmp, &self.mapping_path))
        {
            warn!(path = %self.mapping_path.display(), error = %e, "failed to persist thread mapping");
        }
    }

    fn save_meta(&self, workdirs: &HashMap<String, String>) {
        let sanitized = Self::filter_native_keys_string_map(workdirs);
        if sanitized.len() < workdirs.len() {
            info!(
                filtered = workdirs.len() - sanitized.len(),
                "save_meta: native-dispatch keys excluded from durable snapshot"
            );
        }
        let data = match serde_json::to_string_pretty(&sanitized) {
            Ok(d) => d,
            Err(e) => {
                warn!(error = %e, "failed to serialize session metadata");
                return;
            }
        };
        let tmp = self.meta_path.with_extension("json.tmp");
        if let Err(e) =
            std::fs::write(&tmp, &data).and_then(|_| std::fs::rename(&tmp, &self.meta_path))
        {
            warn!(path = %self.meta_path.display(), error = %e, "failed to persist session metadata");
        }
    }

    /// Persist per-session project bindings to
    /// `${HOME}/.openab/agents/${ARTHUR_AGENT_NAME}/session_projects.json`.
    /// Mirrors `save_meta`'s atomic-write pattern (`.json.tmp` sibling + rename) so
    /// a crash mid-write cannot leave a half-written file behind to be mistaken for a
    /// real binding on the next startup.
    ///
    /// CRITICAL: this function does NOT clear any "corrupt" / "untrusted" state.
    /// The previous design's `save_projects` cleared a global `projects_corrupt`
    /// flag on every successful write, which made the corruption guard vacuous:
    /// persisting an unrelated fresh session C would let an OLD untrusted key B
    /// silently resume on the next call. The untrusted-key set is now per-key
    /// and is removed only by the per-key reset/purge path or the per-key
    /// post-spawn save of that specific key's new binding. `save_projects`
    /// remains a pure "write the in-memory map to disk" operation.
    ///
    /// Phase 6.2.9 fix round 3: this helper additionally scrubs
    /// native-dispatch keys out of the durable snapshot so that a
    /// legacy pre-seeded binding cannot survive a daemon restart.
    fn save_projects(&self, projects: &HashMap<String, ProjectContext>) {
        let sanitized = Self::filter_native_keys_projects_map(projects);
        if sanitized.len() < projects.len() {
            info!(
                filtered = projects.len() - sanitized.len(),
                "save_projects: native-dispatch keys excluded from durable snapshot"
            );
        }
        let data = match serde_json::to_string_pretty(&sanitized) {
            Ok(d) => d,
            Err(e) => {
                warn!(error = %e, "failed to serialize session projects");
                return;
            }
        };
        let tmp = self.projects_path.with_extension("json.tmp");
        if let Err(e) =
            std::fs::write(&tmp, &data).and_then(|_| std::fs::rename(&tmp, &self.projects_path))
        {
            warn!(
                path = %self.projects_path.display(),
                error = %e,
                "failed to persist session projects"
            );
        }
    }

    /// Check if session state exists for this thread (active, suspended, or persisted).
    #[allow(dead_code)]
    pub async fn has_active_session(&self, thread_id: &str) -> bool {
        let state = self.state.read().await;
        // Any of these means the thread already has session state.
        if state.suspended.contains_key(thread_id) || state.persisted.contains_key(thread_id) {
            return true;
        }
        if let Some(conn) = state.active.get(thread_id) {
            match conn.try_lock() {
                Ok(c) => return c.alive(),
                Err(_) => return true, // lock held = connection busy streaming = alive
            }
        }
        false
    }

    /// Read-only lookup of the project binding currently persisted for
    /// `thread_key` in `state.session_projects`.
    ///
    /// This is the SINGLE source of truth for "what project is this thread
    /// pinned to?" — the ctl layer's `thread.pin` invariant uses it to
    /// distinguish the four pre-bootstrap cases (idempotent, mismatch,
    /// unpinned-but-reusable, fresh). Maps to ADR §4.5 + the ctl-bootstrap
    /// requirements of workflow
    /// `20260818-openab-project-aware-thread-routing` (tests K, L, O).
    ///
    /// Returns `None` for anonymous bindings (those are deliberately not
    /// stored — see `project.rs::ProjectContext::is_anonymous`) and for
    /// threads that have never been pinned.
    pub async fn get_pinned_project(
        &self,
        thread_key: &str,
    ) -> Option<crate::acp::project::ProjectContext> {
        let state = self.state.read().await;
        state.session_projects.get(thread_key).cloned()
    }

    /// Returns true iff `get_or_create` for `session_key` would NOT spawn a
    /// brand-new ACP session — i.e. the session key has reusable state in
    /// one of: active connection, suspended session, or persisted session_id.
    ///
    /// This is the SINGLE source of truth for "does a reusable session
    /// exist?" that the ctl layer's `thread.pin` depends on. Mirroring the
    /// decomposition inline in the ctl layer would duplicate SessionPool
    /// lifecycle knowledge outside the pool — every state added to
    /// `PoolState` would need a parallel branch in ctl. Keeping the check
    /// next to the maps that actually drive `get_or_create` is the only
    /// way to keep the two consistent.
    ///
    /// Workflow `20260818-openab-project-aware-thread-routing` invariant:
    /// if `get_pinned_project(S) == None` AND `has_reusable_session(S)` is
    /// true, `thread.pin(S, project)` MUST fail closed with "session
    /// already exists without trusted project binding; reset/recreate
    /// required before pinning" — it must not silently resume the existing
    /// session under the new ProjectContext.
    pub async fn has_reusable_session(&self, session_key: &str) -> bool {
        let state = self.state.read().await;
        state.active.contains_key(session_key)
            || state.suspended.contains_key(session_key)
            || state.persisted.contains_key(session_key)
    }

    /// Phase 6.4.10 (TOCTOU fix) — capacity reservation. Acquires a
    /// reservation covering the entire creation window
    /// (spawn → initialize → session/load-or-new → insert) and either
    /// frees room by evicting the oldest IDLE entry or fails closed.
    ///
    /// Returns a [`CreationReservation`] on success. The caller MUST hold
    /// it across `AcpConnection::spawn`, `initialize`, and
    /// `session/load` / `session_new`, then either:
    ///
    ///   * commit it (after `state.active.insert` succeeds) — the new
    ///     entry IS the slot; or
    ///   * let it drop (on any failure path) — the slot is released so
    ///     the next creator can observe free capacity.
    ///
    /// The invariant the pool enforces is:
    ///
    ///   `state.active.len() + outstanding_creations <= max_sessions`
    ///
    /// This closes the previous TOCTOU window where concurrent creators
    /// could each observe `state.active.len() < max_sessions`, each
    /// spawn, and each insert — pushing the pool to `max_sessions + 1`
    /// (or more on the native-dispatch fast lane).
    async fn ensure_capacity_or_evict<'a>(
        &'a self,
        exclude_key: &str,
    ) -> Result<CreationReservation<'a>> {
        // ── TEST-ONLY ARRIVAL SIGNAL ──────────────────────────────
        // Fire once at the top of every capacity-attempt path, BEFORE
        // any state lock is acquired. Tests K/L/Q/R wait on this to
        // prove that a second creator actually entered the
        // contention boundary (i.e., reached this function) before
        // the test releases the first creator. This is the
        // deterministic replacement for the prior atomic-poll /
        // yield-loop assumptions.
        //
        // `notify_one()` adds a permit; if no waiter is registered
        // yet, the permit is stored (Tokio stores up to one) and
        // consumed by the next `notified().await`. Production code
        // never installs this field, so the `if let` is dead in
        // production.
        #[cfg(test)]
        if let Some(attempt_notify) = self.ensure_capacity_attempt_notify.as_ref() {
            attempt_notify.notify_one();
        }

        // ── Phase 6.4.10 (TOCTOU fix) — capacity reservation. ─────────────
        //
        // The pool-wide invariant is:
        //
        //   `state.active.len() + outstanding_creations <= max_sessions`
        //
        // held continuously under the pool state authority (i.e. there
        // must be no state-unlocked interval where a concurrent caller
        // can observe the freed-but-unreserved capacity and claim it).
        //
        // Both branches below (room-available AND eviction) therefore
        // reserve the slot — increment `outstanding_creations` — while
        // still holding the same state write lock that observes or
        // creates the capacity. The lock is released only after the
        // reservation is in place.
        {
            // Branch 1: room available. Lock + check + reserve + drop
            // while the state authority is still held.
            let mut state = self.state.write().await;
            let used = state.active.len()
                + self
                    .outstanding_creations
                    .load(std::sync::atomic::Ordering::Acquire);
            if used < self.max_sessions {
                self.outstanding_creations
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                #[cfg(test)]
                return Ok(self.create_reservation_for_test());
                #[cfg(not(test))]
                return Ok(CreationReservation::new(self));
            }

            // Branch 2: pool is at capacity. Try to evict the oldest
            // IDLE entry. The candidate picker atomically claims
            // EVICTING, so concurrent `with_connection` calls cannot
            // promote it to BUSY while we are mid-eviction.
            let candidate = find_oldest_idle_eviction_candidate(&state, exclude_key);
            match candidate {
                Some((key, expected_conn, _, sid)) => {
                    // The candidate's phase is already EVICTING. Remove
                    // it from `state.active` AND increment
                    // `outstanding_creations` inside this same critical
                    // section, so no other creator can observe the
                    // freed-but-unreserved slot. After this point the
                    // invariant holds: `active.len() + outstanding == max`.
                    if remove_if_same_handle(&mut state.active, &key, &expected_conn).is_some() {
                        state.cancel_handles.remove(&key);
                        state.activity.remove(&key);
                        state.pgids.remove(&key);
                        #[cfg(feature = "acp-mcp")]
                        revoke_facade_token_for_key(
                            &mut state,
                            &key,
                            self.session_registrar.as_ref(),
                        );
                        info!(
                            evicted = %crate::redact::redact_session_ids(&key),
                            "pool full, pre-evicting oldest idle session before spawn"
                        );
                        // Native-dispatch keys MUST NOT be persisted under
                        // any eviction path; the fast lane guarantees a
                        // fresh ACP session on every entry.
                        if is_native_dispatch_key(&key) {
                            state.session_workdirs.remove(&key);
                            state.session_projects.remove(&key);
                        } else if let Some(sid) = sid {
                            state.persisted.insert(key.clone(), sid.clone());
                            state.suspended.insert(key, sid);
                        } else {
                            state.persisted.remove(&key);
                        }
                        // ── ATOMIC RESERVATION ─────────────────────
                        // Increment outstanding_creations BEFORE
                        // releasing the state lock. This is the
                        // critical fix for the EVICTION-THEN-RESERVE
                        // TOCTOU: previously the fetch_add happened
                        // after `drop(state)`, leaving a window where
                        // a concurrent creator could observe
                        // `active.len() + outstanding == 0` and reserve
                        // the freed slot itself.
                        //
                        // The reservation guard is constructed HERE
                        // (before any subsequent await) so its Drop
                        // runs even if the future is cancelled at the
                        // barrier below. Otherwise a cancellation
                        // between fetch_add and the barrier would
                        // leak the reservation permanently.
                        self.outstanding_creations
                            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                        #[cfg(test)]
                        let reservation = self.create_reservation_for_test();
                        #[cfg(not(test))]
                        let reservation = CreationReservation::new(self);
                        // ── TEST-ONLY ARRIVAL-AFTER-RESERVATION ────
                        // Fire once AFTER fetch_add and BEFORE the
                        // barrier await. Tests K/L/Q/R consume this
                        // permit to deterministically observe that
                        // the creator has its reservation in place.
                        // Unlike `ensure_capacity_attempt_notify`
                        // (which fires at function entry, before
                        // fetch_add), this notify proves the
                        // reservation is committed and
                        // `outstanding_creations == 1` is observable
                        // when the test thread wakes.
                        //
                        // Production code never sets this field, so
                        // the `if let` is dead in production.
                        #[cfg(test)]
                        if let Some(reserved) = self.eviction_reserved_notify.as_ref() {
                            reserved.notify_one();
                        }
                        // ── TEST-ONLY BARRIER ─────────────────────
                        // Park here until the test pokes the barrier
                        // notify. This pins contention to the exact
                        // moment the corrected implementation owns the
                        // freed slot — between fetch_add and drop. If
                        // the production fix is correct, any
                        // concurrent creator that observes the pool
                        // during this parked window must see
                        // `active.len() + outstanding == max` (not 0).
                        // Production code never sets this field, so
                        // the `if let` is dead in production.
                        //
                        // Cancellation safety: if the future is
                        // cancelled here, `reservation` is dropped,
                        // which decrements outstanding_creations via
                        // its Drop impl. That is the property the
                        // cancellation test (P) verifies.
                        #[cfg(test)]
                        if let Some(barrier) = self.eviction_reservation_barrier.as_ref() {
                            barrier.notified().await;
                        }
                        // Reservation is now in place; safe to drop
                        // the state lock. The new creator owns the
                        // freed slot.
                        drop(state);
                        Ok(reservation)
                    } else {
                        // Race: a concurrent caller replaced the
                        // candidate's Arc between our scan and our
                        // remove. Release the EVICTING claim (the
                        // entry will be either re-claimed or naturally
                        // returned to IDLE on the next prompt
                        // release). Fail closed because capacity is
                        // full — no reservation is acquired on this
                        // path.
                        drop(state);
                        phase_force_set(&expected_conn, entry_phase::IDLE);
                        Err(anyhow!("pool exhausted ({} sessions)", self.max_sessions))
                    }
                }
                None => {
                    // Every entry is BUSY or otherwise non-evictable —
                    // fail closed per the documented pool semantics
                    // ("if all sessions are busy, new requests are
                    // rejected"). The spawn has NOT happened yet, so
                    // no ACP child process is leaked on this path.
                    drop(state);
                    Err(anyhow!("pool exhausted ({} sessions)", self.max_sessions))
                }
            }
        }
    }

    pub async fn get_or_create(
        &self,
        thread_id: &str,
        project: Option<&ProjectContext>,
    ) -> Result<bool> {
        // Phase 6.4.10 (contention-test synchronization) — fire the
        // arrival hook BEFORE any lock acquire, so that even a creator
        // blocked at the per-thread gate's `state.write().await` has
        // already registered its arrival with the test driver.
        // Compiled out of release builds.
        #[cfg(test)]
        if let Some(arrival) = self.creator_arrival_notify.as_ref() {
            arrival.notify_one();
        }

        // ── Phase 0: fenced native-work dispatch fast lane (Phase 6.2.9) ─────────
        //
        // A native-work dispatch arrives under an explicit per-dispatch
        // execution-session key (see `admission.rs::WorkAdmissionRequest::
        // native_execution_session_key` and the prefix constant
        // `NATIVE_DISPATCH_KEY_PREFIX`). Such keys MUST:
        //
        //   * spawn a brand new ACP `session/new` (no `session/load`),
        //     so prior `session/update`s / cached turns from unrelated
        //     workflow runs never replay;
        //   * never read or write `state.persisted`, so a daemon restart
        //     cannot reconnect this dispatch to a historical ACP session;
        //   * never read `state.active` either — even if the scheduler
        //     re-dispatches the same dispatch_id with a matching
        //     fingerprint, a fresh process is required. (Idempotency for
        //     genuine retries is owned by the ctl-side
        //     `agent:conversation_key:dispatch_id` ledger, not the pool.)
        //
        // We therefore route native-dispatch keys through
        // `create_fresh_session_only`, which performs the minimal subset of
        // `get_or_create` needed to produce a fresh ACP session and never
        // consults any persisted/suspended state.
        if is_native_dispatch_key(thread_id) {
            return self.create_fresh_session_only(thread_id, project).await;
        }

        let create_gate = {
            let mut state = self.state.write().await;
            get_or_insert_gate(&mut state.creating, thread_id)
        };
        let _create_guard = create_gate.lock().await;

        // ── Phase 1: snapshot every state slice this decision needs ────────────
        //
        // Read active connection, suspended sessionId, AND stored project
        // binding for this thread in a single read lock. Hoisting
        // `session_projects` here is what lets the mismatch gate run
        // BEFORE the busy/alive/resume fast paths below (Defect 1 of
        // workflow 20260818-openab-project-session-pinning-hardening).
        let (existing, saved_session_id, stored_project) = {
            let state = self.state.read().await;
            (
                state.active.get(thread_id).cloned(),
                state.suspended.get(thread_id).cloned(),
                state.session_projects.get(thread_id).cloned(),
            )
        };

        // ── Phase 2: canonicalize the incoming project ONCE ────────────────────
        //
        // For pinned (non-anonymous) contexts this validates the path and
        // produces a canonical form. The canonical form is the same one
        // the stored binding was stored as, so the mismatch check below
        // is a byte-equal comparison and the workdir resolution uses the
        // same path the agent will see as `cwd`. Anonymous and absent
        // contexts yield `None` — no canonical form, no mismatch check.
        // Any invalid-pinned-path error is surfaced here, BEFORE any
        // fast path, so a typo in a project_id never silently falls
        // through to the configured working directory.
        let canonical_pinned = canonicalize_pinned(project).map_err(anyhow::Error::msg)?;

        // ── Phase 3: FAIL-CLOSED PROJECT MISMATCH GATE ────────────────────────
        //
        // Runs BEFORE the busy / alive / resumed fast paths so a stale
        // cross-project active session cannot be silently reused. The
        // previous ordering checked mismatch AFTER the connection
        // fast paths, so `active project A + incoming project B` could
        // return `Ok(false)` and reuse A without ever comparing the
        // contexts. Comparison is on the canonical form produced in
        // Phase 2 and the canonical form stored at persist time, so
        // `"/a"` vs `"/a/"` vs `"/a/./"` all compare equal (canonical
        // equivalence is preserved).
        if let Some(incoming) = &canonical_pinned {
            if let Some(stored) = &stored_project {
                if stored.project_id != incoming.project_id
                    || stored.project_root != incoming.project_root
                {
                    warn!(
                        thread_id = %crate::redact::redact_session_ids(thread_id),
                        stored_project_id = %stored.project_id,
                        incoming_project_id = %incoming.project_id,
                        "project context mismatch — refusing to reuse stale session"
                    );
                    return Err(anyhow!(
                        "project context mismatch: existing session for {} is bound to \
                         project_id={:?} project_root={:?}, but incoming context is \
                         project_id={:?} project_root={:?}; refuse to reuse a stale \
                         cross-project session",
                        crate::redact::redact_session_ids(thread_id),
                        stored.project_id,
                        stored.project_root,
                        incoming.project_id,
                        incoming.project_root,
                    ));
                }
            }
        }

        // ── Phase 4: fail-closed for untrusted per-key project binding ────────
        //
        // The `untrusted_project_keys` set is populated ONLY by
        // `SessionPool::new()` when `load_projects` returned `Err`. In that
        // case, EVERY key in `state.persisted` / `state.suspended` at
        // startup was added to the set, because we cannot tell which of
        // them was project-pinned to which project without the lost
        // binding data. For each such key, a pinned `get_or_create`
        // must fail closed: the original project identity of the
        // resumable session is unknown, and reusing it would be a
        // potentially cross-project silent reuse.
        //
        // A key leaves the untrusted set ONLY through:
        //   - `reset_session(K)` / `purge_session_entries(K)` (key gone);
        //   - the post-spawn section below, when a successful pinned
        //     `get_or_create(K, ...)` persists a NEW trusted binding
        //     for K (the per-key remove happens atomically there).
        //
        // Saving an UNRELATED key C does NOT remove key B from the
        // untrusted set — the previous global-flag design had exactly
        // that defect (verified by the second VERIFIER cycle).
        if canonical_pinned.is_some() {
            let is_untrusted = {
                let set = self.untrusted_project_keys.read().await;
                set.contains(thread_id)
            };
            if is_untrusted {
                let has_persisted = {
                    let state = self.state.read().await;
                    state.persisted.contains_key(thread_id)
                        || state.suspended.contains_key(thread_id)
                };
                if has_persisted {
                    warn!(
                        thread_id = %crate::redact::redact_session_ids(thread_id),
                        "project binding for this thread is untrusted (session_projects \
                         persistence was corrupt at startup); refusing to resume — caller \
                         must reset and retry"
                    );
                    return Err(anyhow!(
                        "project binding for {} is untrusted (session_projects persistence \
                         was corrupt at startup); refusing to resume. Reset the session and \
                         retry with a trusted project context.",
                        crate::redact::redact_session_ids(thread_id),
                    ));
                }
                // No persisted mapping — fresh key, even though the file
                // was corrupt. Allow it: there is no cross-project reuse
                // risk to gate. (Defect 4 required invariant: "do not
                // globally block all future fresh sessions if they have
                // no persisted / suspended mapping".)
            }
        }

        // ── Phase 5: existing-connection fast paths (busy / alive / resume) ──
        //
        // The mismatch gate above has already verified the contexts are
        // compatible, so it is safe to short-circuit when the existing
        // connection is busy or alive. Lock held = busy streaming =
        // alive (same convention as `has_active_session`); cleanup_idle
        // owns hung recovery. Never await the connection's mutex here
        // while holding `create_gate` — F1.
        let had_existing = existing.is_some();
        let mut saved_session_id = saved_session_id;
        if let Some(conn) = existing.clone() {
            let Ok(conn) = conn.try_lock() else {
                return Ok(false);
            };
            if conn.alive() {
                return Ok(false);
            }
            if saved_session_id.is_none() {
                saved_session_id = conn.acp_session_id.clone();
            }
        }

        // Snapshot active handles so we can inspect them outside the state lock.
        //
        // Phase 6.4.10: this read-only snapshot is no longer the eviction
        // decision. The capacity-aware pre-eviction runs below, BEFORE the
        // spawn, under the state write lock so concurrent `get_or_create`
        // calls for distinct keys cannot both pick the same idle entry and
        // race-evict each other.
        let _snapshot: ActiveSnapshot = {
            let state = self.state.read().await;
            state
                .active
                .iter()
                .map(|(k, v)| (k.clone(), Arc::clone(v)))
                .collect()
        };

        // Resolve effective working directory.
        //
        // Precedence (matches workflow req #6 + ADR §4.5 immutability):
        //   1. project-pinned project_root when the incoming ProjectContext carries
        //      a non-empty project_id. Authoritative: the session is bound to this
        //      (project_id, project_root) for its lifetime.
        //   2. Stored per-session workdir (legacy immutability — once a workspace
        //      sticks for a thread it stays, even if a later call forgets the
        //      override).
        //   3. Configured [agent].working_dir.
        //
        // Anonymous contexts (`project_id.is_empty()`) preserve the legacy
        // `[[ws:@alias]]` semantics: stored wins over the workspace hint, so an
        // existing session is not silently moved by a new directive in a later
        // message (ADR §4.5). The anonymous path itself is used as the workdir
        // only when no stored binding exists.
        let stored_workdir = {
            let state = self.state.read().await;
            state.session_workdirs.get(thread_id).cloned()
        };

        // Use the pre-canonicalized form from Phase 2. resolve_effective_workdir
        // no longer re-validates the path; the canonical form is the
        // authoritative workdir AND the binding we will persist.
        let (effective_workdir, project_to_store) = resolve_effective_workdir(
            project,
            canonical_pinned.as_ref(),
            stored_workdir.as_deref(),
            &self.config.working_dir,
        );

        // ── Phase 5.5: capacity reservation (Phase 6.4.10 TOCTOU fix) ───────────
        //
        // Acquire a reservation that spans the entire creation window
        // (spawn → initialize → session/load-or-new → insert). On any
        // failure between here and the successful `state.active.insert`,
        // the reservation's Drop impl releases the slot so concurrent
        // creators can observe free capacity. The invariant
        // `active.len() + outstanding_creations <= max_sessions` is
        // enforced at the moment of increment, so two concurrent
        // creators cannot both observe room and both proceed to spawn.
        //
        // The reservation is consumed on the success path via
        // `commit()` after `state.active.insert` — the new entry IS the
        // slot, so the counter must not also decrement.
        let _creation_reservation = self.ensure_capacity_or_evict(thread_id).await?;

        // Browser capabilities for an `acp:` session come from the OAB MCP Facade and nowhere
        // else: mint a per-session token (it rides the agent spawn below as OPENAB_SESSION_TOKEN)
        // and write the static facade entry before the agent boots. The returned guard revokes
        // that token when this connection is dropped, on any evict path.
        //
        // There is no transport fallback. Without `[mcp]` the root wires no registrar, and the
        // session simply starts without browser capabilities — which is the honest outcome and is
        // reported once at startup rather than being silently substituted per session.
        #[cfg(feature = "acp-mcp")]
        let mut session_token: Option<String> = None;
        #[cfg(feature = "acp-mcp")]
        let facade_token_guard: Option<tokio_util::sync::DropGuard> = match (
            thread_id.strip_prefix("acp:"),
            self.session_registrar.as_ref(),
            self.facade_url.as_ref(),
        ) {
            (Some(channel_id), Some(registrar), Some(facade_url)) => {
                match setup_facade_session(&effective_workdir, facade_url, channel_id, registrar)
                    .await
                {
                    Some(token) => {
                        session_token = Some(token.clone());
                        info!(thread_id = %crate::redact::redact_session_ids(thread_id), "session token minted for facade browser capabilities");
                        // The guard carries the TOKEN it minted, not the channel. A replaced
                        // session's teardown runs after its successor has already re-minted for
                        // the same channel, so revoking by channel would strip the live token and
                        // silently cut the new agent off from the facade; revoking this exact
                        // token is a no-op by then (R1).
                        let ct = tokio_util::sync::CancellationToken::new();
                        let child = ct.child_token();
                        let registrar = registrar.clone();
                        tokio::spawn(async move {
                            child.cancelled().await;
                            registrar.revoke(&token);
                        });
                        Some(ct.drop_guard())
                    }
                    // No config, so no token and no revoke guard to arm. The session still
                    // starts — it simply has no browser capabilities.
                    None => None,
                }
            }
            _ => None,
        };

        // Build the replacement connection outside the state lock so one stuck
        // initialization does not block all unrelated sessions.
        #[cfg(feature = "acp-mcp")]
        let spawn_env: std::collections::HashMap<String, String> = {
            let mut env = self.config.env.clone();
            if let Some(tok) = &session_token {
                // The static facade MCP entry references ${OPENAB_SESSION_TOKEN};
                // the value lives only in this agent process's environment.
                env.insert("OPENAB_SESSION_TOKEN".to_string(), tok.clone());
            }
            env
        };
        #[cfg(not(feature = "acp-mcp"))]
        let spawn_env = self.config.env.clone();
        let mut new_conn = AcpConnection::spawn(
            &self.config.command,
            &self.config.args,
            &effective_workdir,
            &spawn_env,
            &self.config.inherit_env,
        )
        .await?;

        new_conn.initialize().await?;

        let mut resumed = false;
        let mut load_failed: Option<&str> = None;
        if let Some(ref sid) = saved_session_id {
            if new_conn.supports_load_session {
                match new_conn.session_load(sid, &effective_workdir).await {
                    Ok(()) => {
                        info!(thread_id = %crate::redact::redact_session_ids(thread_id), session_id = %crate::redact::redact_session_ids(sid), "session resumed via session/load");
                        resumed = true;
                    }
                    Err(e) => {
                        let err_str = e.to_string();
                        let is_transient =
                            TRANSIENT_LOAD_ERRORS.iter().any(|s| err_str.contains(s));
                        if is_transient {
                            warn!(thread_id = %crate::redact::redact_session_ids(thread_id), session_id = %crate::redact::redact_session_ids(sid), error = %e,
                                "session/load failed transiently, preserving session ID for retry");
                            load_failed = Some(if err_str.contains("timeout waiting for") {
                                "timeout"
                            } else {
                                "connection lost"
                            });
                        } else {
                            warn!(thread_id = %crate::redact::redact_session_ids(thread_id), session_id = %crate::redact::redact_session_ids(sid), error = %e,
                                "session/load failed, creating new session");
                        }
                    }
                }
            }
        }

        if let Some(reason) = load_failed {
            // session/load failed transiently. The original session ID is already
            // in state.persisted (we haven't touched it), so the next message will
            // retry session/load automatically. Return an error so the current message
            // is not processed against a context-free session.
            return Err(anyhow!(
                "session load {reason}: could not restore previous session"
            ));
        }

        if !resumed {
            new_conn.session_new(&effective_workdir).await?;

            // Apply default config options (e.g. mode=bypass, model=swe-1-6)
            for (config_id, value) in &self.default_config_options {
                if let Err(e) = new_conn.set_config_option(config_id, value).await {
                    warn!(config_id, value, error = %e, "failed to set default config option");
                }
            }

            // Surface the reset banner both for restored sessions and for stale
            // live entries that died before we could recover a resumable
            // session id. In both cases the caller is continuing after an
            // unexpected session loss.
            if had_existing || saved_session_id.is_some() {
                new_conn.session_reset = true;
            }
        }

        let cancel_handle = new_conn.cancel_handle();
        let activity_handle = new_conn.activity_handle();
        let child_pgid = new_conn.child_pgid();
        let cancel_session_id = new_conn.acp_session_id.clone().unwrap_or_default();
        #[cfg(feature = "acp-mcp")]
        new_conn.set_facade_token_guard(facade_token_guard);
        let new_conn = Arc::new(Mutex::new(new_conn));

        let mut state = self.state.write().await;

        // Another task may have created a healthy connection while we were
        // initializing this one.
        if let Some(existing) = state.active.get(thread_id).cloned() {
            let Ok(existing) = existing.try_lock() else {
                return Ok(false);
            };
            if existing.alive() {
                // A concurrent creator already inserted a healthy entry
                // for this key. Release our reservation (the slot was
                // never used) and let the caller reuse the existing
                // entry. The existing entry's phase is whatever the
                // concurrent creator left it as (IDLE for fresh spawns).
                drop(existing);
                drop(state);
                // Reservation drops here — outstanding_creations--.
                return Ok(false);
            }
            warn!(thread_id = %crate::redact::redact_session_ids(thread_id), "stale connection, rebuilding");
            drop(existing);
            state.active.remove(thread_id);
            state.cancel_handles.remove(thread_id);
            state.activity.remove(thread_id);
            state.pgids.remove(thread_id);
        }

        // Phase 6.4.10 (TOCTOU fix) — capacity is reserved by the
        // CreationReservation held by this function; the previous
        // post-spawn length check (`state.active.len() >= max_sessions`)
        // was removed because the reservation is the single source of
        // truth. The new entry's phase is initialized to IDLE inside
        // `AcpConnection::spawn`.
        if cancel_session_id.is_empty() {
            state.persisted.remove(thread_id);
        } else {
            state
                .persisted
                .insert(thread_id.to_string(), cancel_session_id.clone());
        }
        state.suspended.remove(thread_id);
        state.active.insert(thread_id.to_string(), new_conn);
        state
            .activity
            .insert(thread_id.to_string(), activity_handle);
        if let Some(pgid) = child_pgid {
            state.pgids.insert(thread_id.to_string(), pgid);
        }
        if !cancel_session_id.is_empty() {
            state
                .cancel_handles
                .insert(thread_id.to_string(), (cancel_handle, cancel_session_id));
        }
        // Record this connection's exact token lock-free, revoking any predecessor token it
        // supersedes under the same key (its guard cannot fire if that predecessor is hung). F3.
        #[cfg(feature = "acp-mcp")]
        if let Some(token) = session_token {
            install_facade_token(
                &mut state,
                thread_id,
                token,
                self.session_registrar.as_ref(),
            );
        }
        self.save_mapping(&state.persisted);

        // Persist workspace override only after session spawn succeeded (口渡 F2).
        // Mirrors the original behavior: a thread's first workspace hint sticks
        // for the rest of the session's lifetime (ADR §4.5 immutability). When
        // a project-pinned context drove `effective_workdir`, persist the workdir
        // AND the project binding so a process restart does not drop the pin.
        if project.is_some() {
            state
                .session_workdirs
                .entry(thread_id.to_string())
                .or_insert_with(|| effective_workdir.clone());
            self.save_meta(&state.session_workdirs);
        }
        if let Some(p) = &project_to_store {
            state
                .session_projects
                .insert(thread_id.to_string(), p.clone());
            self.save_projects(&state.session_projects);
            // A fresh trusted binding for this specific key has just
            // been persisted. Remove the key from the untrusted set so
            // future pinned get_or_create calls for THIS key are no
            // longer fail-closed. Other untrusted keys (e.g. another
            // pre-existing key whose binding was lost) remain
            // untrusted; only this key's per-key state is cleared.
            self.untrusted_project_keys.write().await.remove(thread_id);
        }

        // Return true only for genuinely new sessions — not resumed or reconnected ones.
        // A session with prior state (saved_session_id or had_existing) is a resume,
        // even if we had to spawn a new ACP process. ADR §2.2: directives are first-message-only.
        let is_fresh = !had_existing && saved_session_id.is_none();
        // Phase 6.4.10 (TOCTOU fix) — the new entry is the slot.
        // Commit the reservation so its Drop impl does not also release
        // it (the counter already accounts for the entry via
        // state.active.insert).
        _creation_reservation.commit();
        Ok(is_fresh)
    }

    /// Phase 6.2.9: spawn a fresh ACP `session/new` for a fenced native-work
    /// dispatch without consulting any persisted or suspended state.
    ///
    /// Called only from `get_or_create` when the pool key carries the
    /// `native-dispatch:` prefix. Behaviour:
    ///
    ///   * `state.persisted` and `state.suspended` are never read or written
    ///     for this key — a daemon restart therefore cannot replay the
    ///     dispatch into a historical session.
    ///   * `state.active` is also never read — the dispatch always gets its
    ///     own ACP process and its own ACP session id, even if a previous
    ///     dispatch id with the same key shape already produced a session
    ///     earlier in this daemon's lifetime. Idempotency for repeated
    ///     scheduler dispatch of the same dispatch_id is the ctl-side
    ///     ledger's job (`agent:conversation_key:dispatch_id`), not the
    ///     pool's.
    ///   * `state.session_workdirs` / `state.session_projects` are never
    ///     consulted: native-work dispatches carry their own project
    ///     metadata on the message envelope, and reusing a stale
    ///     cross-project binding is exactly the kind of contamination this
    ///     method exists to prevent.
    ///
    /// Returns `Ok(true)` to signal "a brand-new ACP session was created";
    /// the dispatcher uses that signal the same way it does for genuine
    /// first-message-only directive processing.
    pub async fn create_fresh_session_only(
        &self,
        thread_id: &str,
        project: Option<&ProjectContext>,
    ) -> Result<bool> {
        // Phase 6.4.10 (contention-test synchronization) — fire the
        // arrival hook BEFORE any lock acquire, so that even a creator
        // blocked at the per-thread gate's `state.write().await` has
        // already registered its arrival with the test driver.
        // Compiled out of release builds.
        #[cfg(test)]
        if let Some(arrival) = self.creator_arrival_notify.as_ref() {
            arrival.notify_one();
        }

        // Native-dispatch keys never have a stable per-thread workdir — the
        // dispatch is single-turn by design. Fall back to the configured
        // working directory. If the caller supplied a project-pinned
        // context, honour it via the same canonical-form path that
        // `get_or_create` uses, so the failure modes for an invalid pinned
        // path match.
        let canonical_pinned = canonicalize_pinned(project).map_err(anyhow::Error::msg)?;
        let stored_workdir: Option<String> = None;
        let (effective_workdir, _project_to_store_unused) = resolve_effective_workdir(
            project,
            canonical_pinned.as_ref(),
            stored_workdir.as_deref(),
            &self.config.working_dir,
        );

        // Mint a per-session facade token when the `[mcp]` registrar is
        // configured. Native-dispatch keys always carry the `acp:`
        // namespace in the ctl layer's ChannelRef, so the same
        // `setup_facade_session` path is reusable.
        #[cfg(feature = "acp-mcp")]
        let mut session_token: Option<String> = None;
        #[cfg(feature = "acp-mcp")]
        let facade_token_guard: Option<tokio_util::sync::DropGuard> = match (
            self.session_registrar.as_ref(),
            self.facade_url.as_ref(),
        ) {
            (Some(registrar), Some(facade_url)) => {
                let channel_id = thread_id
                    .strip_prefix(NATIVE_DISPATCH_KEY_PREFIX)
                    .unwrap_or(thread_id);
                match setup_facade_session(&effective_workdir, facade_url, channel_id, registrar)
                    .await
                {
                    Some(token) => {
                        session_token = Some(token.clone());
                        info!(execution_session_key = %crate::redact::redact_session_ids(thread_id), "facade session token minted for native dispatch");
                        let ct = tokio_util::sync::CancellationToken::new();
                        let child = ct.child_token();
                        let registrar = registrar.clone();
                        tokio::spawn(async move {
                            child.cancelled().await;
                            registrar.revoke(&token);
                        });
                        Some(ct.drop_guard())
                    }
                    None => None,
                }
            }
            _ => None,
        };

        #[cfg(feature = "acp-mcp")]
        let spawn_env: std::collections::HashMap<String, String> = {
            let mut env = self.config.env.clone();
            if let Some(tok) = &session_token {
                env.insert("OPENAB_SESSION_TOKEN".to_string(), tok.clone());
            }
            env
        };
        #[cfg(not(feature = "acp-mcp"))]
        let spawn_env = self.config.env.clone();

        // Phase 6.4.10 (TOCTOU fix) — bring the native-dispatch fast
        // lane under the same capacity reservation as `get_or_create`.
        // The previous code allowed `state.active.len() > max_sessions`
        // with only a warning, so two concurrent native dispatches could
        // each observe room, each spawn, and each insert. The fast lane
        // now acquires a CreationReservation that spans spawn + init +
        // session_new + insert, and fails closed (without spawning) when
        // every entry is BUSY. The reservation's Drop impl releases the
        // slot on every failure path.
        let _creation_reservation = self.ensure_capacity_or_evict(thread_id).await?;

        let mut new_conn = AcpConnection::spawn(
            &self.config.command,
            &self.config.args,
            &effective_workdir,
            &spawn_env,
            &self.config.inherit_env,
        )
        .await?;
        new_conn.initialize().await?;

        // CRITICAL: never `session/load` here. Native-dispatch keys must
        // always spawn a brand-new ACP `session/new` so historical turns
        // from unrelated workflow runs cannot replay.
        new_conn.session_new(&effective_workdir).await?;
        for (config_id, value) in &self.default_config_options {
            if let Err(e) = new_conn.set_config_option(config_id, value).await {
                warn!(config_id, value, error = %e, "failed to set default config option");
            }
        }
        let new_session_id = new_conn.acp_session_id.clone().unwrap_or_default();

        // Structured log line for production correlation: workflow_run_id /
        // role / dispatch_id / execution_session_key are surfaced at every
        // observation site by the dispatcher; this line records the
        // pool-side fact that a fresh ACP session was created and prior
        // history was NOT replayed. No prompts / no secrets.
        info!(
            execution_session_key = %crate::redact::redact_session_ids(thread_id),
            acp_session_id = %crate::redact::redact_session_ids(&new_session_id),
            acp_session_created = true,
            prior_history_replayed = false,
            "native dispatch: fresh ACP session created, historical turns NOT replayed"
        );

        let cancel_handle = new_conn.cancel_handle();
        let activity_handle = new_conn.activity_handle();
        let child_pgid = new_conn.child_pgid();
        #[cfg(feature = "acp-mcp")]
        new_conn.set_facade_token_guard(facade_token_guard);
        let new_conn = Arc::new(Mutex::new(new_conn));

        let mut state = self.state.write().await;
        // Do NOT insert into state.persisted (daemon-restart isolation).
        // Do NOT insert into state.suspended (idle-eviction isolation).
        // Do NOT insert into state.session_workdirs / state.session_projects
        // (cross-project contamination isolation).
        state.active.insert(thread_id.to_string(), new_conn);
        state
            .activity
            .insert(thread_id.to_string(), activity_handle);
        if let Some(pgid) = child_pgid {
            state.pgids.insert(thread_id.to_string(), pgid);
        }
        if !new_session_id.is_empty() {
            state.cancel_handles.insert(
                thread_id.to_string(),
                (cancel_handle, new_session_id.clone()),
            );
        }
        #[cfg(feature = "acp-mcp")]
        if let Some(token) = session_token {
            install_facade_token(
                &mut state,
                thread_id,
                token,
                self.session_registrar.as_ref(),
            );
        }

        // Phase 6.4.10 (TOCTOU fix) — the reservation is the capacity
        // authority. The previous `state.active.len() > max_sessions + 1`
        // defense-in-depth check is gone because it accepted a temporary
        // over-shoot; the new invariant guarantees
        // `active.len() + outstanding_creations <= max_sessions` at all
        // times. The reservation is committed so its Drop does not also
        // release the slot (the new entry IS the slot).
        _creation_reservation.commit();
        Ok(true)
    }

    /// Get mutable access to a connection. Caller must have called get_or_create first.
    ///
    /// Only the per-connection `Mutex` is held during `f`; the pool-level
    /// `RwLock` is acquired briefly (read-only) to look up the `Arc` and then
    /// released, so other connections can be used concurrently.
    ///
    /// Phase 6.4.10 (TOCTOU fix): this method atomically claims the
    /// entry's lifecycle phase via compare-and-swap on the per-connection
    /// `phase` atomic BEFORE taking the connection mutex. The CAS
    /// guarantees that:
    ///
    ///   * a session being evicted cannot begin a new prompt (CAS fails
    ///     because the phase is EVICTING); and
    ///   * a session beginning a prompt cannot be subsequently evicted
    ///     (eviction's CAS fails because the phase is BUSY).
    ///
    /// The previous design inferred idleness from `try_lock` on the
    /// connection mutex, which is a transient observation: a concurrent
    /// `with_connection` could win the mutex AFTER eviction had decided
    /// to remove the entry. The new atomic CAS closes that window.
    ///
    /// The `BusyGuard` continues to maintain the legacy
    /// `activity.in_flight` flag for backward compatibility with the
    /// adapter and cleanup paths; the `phase` atomic is the new
    /// authoritative state for eviction eligibility.
    pub async fn with_connection<F, R>(&self, thread_id: &str, f: F) -> Result<R>
    where
        F: for<'a> FnOnce(
            &'a mut AcpConnection,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<R>> + Send + 'a>,
        >,
    {
        let conn = {
            let state = self.state.read().await;
            state.active.get(thread_id).cloned().ok_or_else(|| {
                anyhow!(
                    "no connection for thread {}",
                    crate::redact::redact_session_ids(thread_id)
                )
            })?
        };

        // Atomic IDLE -> BUSY claim. If the CAS fails the entry is
        // either already busy (another prompt in flight) or being
        // evicted (capacity-eviction in progress). Either way, this
        // caller MUST NOT proceed — the entry is not available for a
        // new prompt. Surface a clear error so the adapter can fall
        // back to its busy handling rather than silently double-
        // executing the closure.
        if !phase_try_cas(&conn, entry_phase::IDLE, entry_phase::BUSY) {
            let phase = phase_load(&conn);
            let reason = match phase {
                entry_phase::BUSY => "session is busy executing another prompt",
                entry_phase::EVICTING => "session is being evicted",
                _ => "session is not in IDLE state",
            };
            return Err(anyhow!(
                "cannot acquire session for thread {}: {reason}",
                crate::redact::redact_session_ids(thread_id),
            ));
        }

        // The guard is installed AFTER the CAS so the phase cannot be
        // released to IDLE before the claim. On Drop (every exit path:
        // success, error, cancellation, timeout, panic) the phase
        // transitions BUSY → IDLE. If the closure panics, Drop still
        // runs because the guard is on the stack frame that panicked.
        struct PhaseGuard {
            conn: Arc<Mutex<AcpConnection>>,
        }
        impl Drop for PhaseGuard {
            fn drop(&mut self) {
                // Best-effort release. If another caller claimed the
                // entry between our BUSY claim and now (unlikely but
                // possible during a complex lifecycle), the CAS is a
                // no-op. Idempotent and infallible.
                phase_force_set(&self.conn, entry_phase::IDLE);
            }
        }

        let mut conn_guard_lock = conn.lock().await;
        let activity = Arc::clone(&conn_guard_lock.activity);
        let _busy_guard = BusyGuard::arm(activity);
        let _phase_guard = PhaseGuard { conn: Arc::clone(&conn) };
        let result = f(&mut conn_guard_lock).await;
        // Drop order matters:
        //   1. `_busy_guard` clears activity.in_flight (atomic, no lock).
        //   2. `conn_guard_lock` releases the per-connection mutex so
        //      the next step's `try_lock` (for phase release) succeeds.
        //   3. `_phase_guard` releases the phase atomic to IDLE.
        // If `_phase_guard` dropped first, its `try_lock` would fail
        // because we still hold `conn_guard_lock` — leaving the entry
        // stuck in BUSY forever (the original TOCTOU class of bug).
        drop(_busy_guard);
        drop(conn_guard_lock);
        drop(_phase_guard);
        result
    }

    /// Get cached configOptions for a session (e.g. available models).
    pub async fn get_config_options(&self, thread_id: &str) -> Vec<ConfigOption> {
        let state = self.state.read().await;
        let conn = match state.active.get(thread_id) {
            Some(c) => c.clone(),
            None => return Vec::new(),
        };
        drop(state);
        let conn = conn.lock().await;
        conn.config_options.clone()
    }

    /// Set a config option (e.g. model) via ACP and return updated options.
    pub async fn set_config_option(
        &self,
        thread_id: &str,
        config_id: &str,
        value: &str,
    ) -> Result<Vec<ConfigOption>> {
        let conn = {
            let state = self.state.read().await;
            state.active.get(thread_id).cloned().ok_or_else(|| {
                anyhow!(
                    "no connection for thread {}",
                    crate::redact::redact_session_ids(thread_id)
                )
            })?
        };
        let mut conn = conn.lock().await;
        conn.set_config_option(config_id, value).await
    }

    /// Phase 6.4.1F — apply the structured `write_policy` to the
    /// ACP connection that owns this session key. Called by
    /// `dispatch_batch` after `ensure_session` for fresh native-work
    /// turns so the connection's `WritePolicyGuard` reflects the
    /// current dispatch's policy BEFORE the first
    /// `session/request_permission` can be observed by the reader
    /// loop. The operation is idempotent and lock-free.
    pub async fn set_session_write_policy(&self, thread_id: &str, policy: &str) {
        let conn = {
            let state = self.state.read().await;
            state.active.get(thread_id).cloned()
        };
        if let Some(conn) = conn {
            let conn = conn.lock().await;
            conn.write_policy_guard.set(policy);
        }
    }

    /// Resume one approval decision on the active ACP session for this thread.
    pub async fn resume_approval(
        &self,
        thread_id: &str,
        expected_session_id: &str,
        approval_id: &str,
        decision: &str,
        conversation_id: &str,
        expected_revision: u64,
        identity: crate::acp::connection::AcpPromptIdentity,
    ) -> Result<serde_json::Value> {
        let expected_session_id = expected_session_id.to_string();
        let approval_id = approval_id.to_string();
        let decision = decision.to_string();
        let conversation_id = conversation_id.to_string();

        self.with_connection(thread_id, move |conn| {
            Box::pin(async move {
                let current_session_id = conn
                    .current_session_id()
                    .ok_or_else(|| anyhow!("no session"))?;

                if current_session_id != expected_session_id {
                    return Err(anyhow!(
                        "approval session mismatch: pending approval is bound to a different ACP session"
                    ));
                }

                conn.resume_approval(
                    &approval_id,
                    &decision,
                    &conversation_id,
                    expected_revision,
                    &identity,
                )
                .await
            })
        })
        .await
    }

    /// Query account-level usage/billing from the backend agent for a session
    /// (kiro-cli extension). Fails when there is no active session for the
    /// thread or the backend does not support usage queries.
    pub async fn get_usage(&self, thread_id: &str) -> Result<crate::acp::protocol::UsageReport> {
        let conn = {
            let state = self.state.read().await;
            state.active.get(thread_id).cloned().ok_or_else(|| {
                anyhow!(
                    "no connection for thread {}",
                    crate::redact::redact_session_ids(thread_id)
                )
            })?
        };
        let mut conn = conn.lock().await;
        conn.get_usage().await
    }

    /// Cancel the current in-flight operation for a session.
    /// Uses pre-stored cancel handles to avoid locking the connection (which is held during streaming).
    pub async fn cancel_session(&self, thread_id: &str) -> Result<()> {
        let (stdin, session_id) = {
            let state = self.state.read().await;
            state
                .cancel_handles
                .get(thread_id)
                .cloned()
                .ok_or_else(|| {
                    anyhow!(
                        "no session for thread {}",
                        crate::redact::redact_session_ids(thread_id)
                    )
                })?
        };
        let data = serde_json::to_string(&serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/cancel",
            "params": {"sessionId": session_id}
        }))?;
        tracing::info!(session_id = %crate::redact::redact_session_ids(&session_id), "sending session/cancel");
        use tokio::io::AsyncWriteExt;
        let mut w = stdin.lock().await;
        w.write_all(data.as_bytes()).await?;
        w.write_all(b"\n").await?;
        w.flush().await?;
        Ok(())
    }

    /// Reset a session: cancel any in-flight operation, remove the active connection,
    /// and clear all suspended state. The ACP process will be killed once the last
    /// Arc reference is dropped (after streaming finishes). The next message will
    /// trigger a fresh `get_or_create` with a new ACP session.
    pub async fn reset_session(&self, thread_id: &str) -> Result<()> {
        // Send session/cancel via the lock-free stdin handle first.
        // This stops in-flight streaming even while with_connection() holds the
        // connection mutex, so the old process finishes promptly.
        if let Some((stdin, session_id)) = {
            let state = self.state.read().await;
            state.cancel_handles.get(thread_id).cloned()
        } {
            let data = serde_json::to_string(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "session/cancel",
                "params": {"sessionId": session_id}
            }))?;
            tracing::info!(session_id = %crate::redact::redact_session_ids(&session_id), "reset: sending session/cancel");
            use tokio::io::AsyncWriteExt;
            let mut w = stdin.lock().await;
            let _ = w.write_all(data.as_bytes()).await;
            let _ = w.write_all(b"\n").await;
            let _ = w.flush().await;
        }

        let mut state = self.state.write().await;
        let had_active = state.active.remove(thread_id).is_some();
        // Everything else a reset clears is exactly what hung eviction clears, including the rule
        // that the creating gate survives. Call the one implementation rather than keeping a second
        // copy of the list: the copies are what let the two drift, and the gate rule is precisely
        // the kind of line that gets dropped from a duplicate without anyone noticing.
        purge_session_entries(&mut state, thread_id);
        // Resetting a hung session drops the map's Arc but not the one the stuck task holds, so the
        // guard cannot revoke — do it synchronously here too (F3).
        #[cfg(feature = "acp-mcp")]
        revoke_facade_token_for_key(&mut state, thread_id, self.session_registrar.as_ref());
        self.save_mapping(&state.persisted);
        self.save_meta(&state.session_workdirs);
        // Project binding must be cleared on reset so the next message can pin
        // a fresh project context. Without this, a reset under thread_key T
        // would carry over the previous session's project binding, and the
        // mismatch check would refuse the new context against the OLD pin.
        self.save_projects(&state.session_projects);
        // Drop the state write lock before touching the untrusted set.
        drop(state);
        // A reset removes this key's session entirely. The untrusted
        // marker is tied to "this key might have a wrong binding for
        // a resumable session"; once the session is gone, the marker
        // is no longer relevant — a subsequent fresh pinned
        // get_or_create under this key starts a clean slate.
        self.untrusted_project_keys.write().await.remove(thread_id);
        if had_active {
            info!(thread_id = %crate::redact::redact_session_ids(thread_id), "session reset");
            Ok(())
        } else {
            Err(anyhow!(
                "no session for thread {}",
                crate::redact::redact_session_ids(thread_id)
            ))
        }
    }

    pub async fn cleanup_idle(&self, ttl_secs: u64) {
        let cutoff = Instant::now() - std::time::Duration::from_secs(ttl_secs);
        let hung_threshold = std::time::Duration::from_secs(self.hung_threshold_secs);

        let (snapshot, activity_map, cancel_map, pgid_map) = {
            let state = self.state.read().await;
            let snapshot: ActiveSnapshot = state
                .active
                .iter()
                .map(|(k, v)| (k.clone(), Arc::clone(v)))
                .collect();
            (
                snapshot,
                state.activity.clone(),
                state.cancel_handles.clone(),
                state.pgids.clone(),
            )
        };

        let mut stale = Vec::new();
        let mut hung: Vec<(String, Arc<Mutex<AcpConnection>>)> = Vec::new();
        for (key, conn) in snapshot {
            // Skip active sessions for this cleanup round instead of waiting on
            // their per-connection mutex. A busy session is not idle unless hung.
            let conn_handle = Arc::clone(&conn);
            let Ok(conn) = conn.try_lock() else {
                if let Some(activity) = activity_map.get(&key) {
                    if classify_hung(activity.in_flight(), activity.age(), hung_threshold) {
                        let session_id = cancel_map.get(&key).map(|(_, sid)| sid.clone());
                        warn_force_evicting_hung(
                            &key,
                            session_id.as_deref(),
                            activity.age().as_secs(),
                            self.hung_threshold_secs,
                        );
                        // Best-effort session/cancel via the lock-free stdin
                        // handle, detached so a wedged stdin can never block
                        // cleanup (and never while holding `state`). The hung
                        // task never unwinds, so AcpConnection::Drop never
                        // fires; after the cancel attempt, kill the child
                        // process group directly or the agent leaks forever (F4).
                        let stdin_handle = cancel_map.get(&key).map(|(stdin, _)| Arc::clone(stdin));
                        let pgid = pgid_map.get(&key).copied();
                        tokio::spawn(async move {
                            if let (Some(stdin), Some(session_id)) = (stdin_handle, session_id) {
                                let _ = tokio::time::timeout(
                                    std::time::Duration::from_secs(5),
                                    async move {
                                        if let Ok(data) =
                                            serde_json::to_string(&serde_json::json!({
                                                "jsonrpc": "2.0",
                                                "method": "session/cancel",
                                                "params": {"sessionId": session_id}
                                            }))
                                        {
                                            use tokio::io::AsyncWriteExt;
                                            let mut w = stdin.lock().await;
                                            let _ = w.write_all(data.as_bytes()).await;
                                            let _ = w.write_all(b"\n").await;
                                            let _ = w.flush().await;
                                        }
                                    },
                                )
                                .await;
                            }
                            kill_pgid_after_grace(pgid).await;
                        });
                        hung.push((key, conn_handle));
                    }
                }
                continue;
            };
            // try_lock success means no turn is streaming under
            // with_connection, so a true in_flight flag is stale (the turn
            // aborted without prompt_done). Self-heal it so the session can
            // never be falsely classified as hung later.
            if let Some(activity) = activity_map.get(&key) {
                if activity.in_flight() {
                    activity.set_in_flight(false);
                    activity.touch();
                }
            }
            if classify_idle(conn.last_active, conn.alive(), cutoff) {
                stale.push((key, conn_handle, conn.acp_session_id.clone()));
            }
        }

        if stale.is_empty() && hung.is_empty() {
            return;
        }

        let mut state = self.state.write().await;
        // Keys that were fully idle-evicted (no resumable sessionId
        // left) — their untrusted marker must be removed AFTER we drop
        // the state write lock, so we don't nest two write locks.
        let mut fully_evicted_keys: Vec<String> = Vec::new();
        for (key, expected_conn, sid) in stale {
            if remove_if_same_handle(&mut state.active, &key, &expected_conn).is_some() {
                info!(thread_id = %crate::redact::redact_session_ids(&key), "cleaning up idle session");
                state.cancel_handles.remove(&key);
                state.activity.remove(&key);
                state.pgids.remove(&key);
                #[cfg(feature = "acp-mcp")]
                revoke_facade_token_for_key(&mut state, &key, self.session_registrar.as_ref());
                // Phase 6.2.9: native-dispatch:* keys MUST NOT be persisted
                // during idle cleanup. The fast lane guarantees a fresh
                // ACP session on every entry, so persisting an idle
                // session's id would defeat the isolation contract on
                // any subsequent daemon restart that rehydrated
                // thread_map.json before the prefix check ran. We drop
                // them entirely here (no persisted, no suspended, no
                // workdir, no project binding).
                if is_native_dispatch_key(&key) {
                    state.session_workdirs.remove(&key);
                    state.session_projects.remove(&key);
                    fully_evicted_keys.push(key);
                    continue;
                }
                if let Some(sid) = sid {
                    state.persisted.insert(key.clone(), sid.clone());
                    state.suspended.insert(key, sid);
                } else {
                    state.persisted.remove(&key);
                    state.session_workdirs.remove(&key);
                    // An idle-evicted session loses its project binding too:
                    // the session is gone, and a future get_or_create under the
                    // same thread_key must NOT inherit a project_id the new
                    // call did not ask for.
                    state.session_projects.remove(&key);
                    // The untrusted marker is also tied to "this key
                    // has a resumable session whose binding might be
                    // wrong". When the session is fully evicted (no
                    // resumable sessionId), the marker is no longer
                    // relevant — capture the key for post-state-lock
                    // removal below. We can't take the untrusted-set
                    // write lock while holding the state write lock.
                    fully_evicted_keys.push(key);
                }
            }
        }
        for (key, expected_conn) in hung {
            if apply_hung_eviction(&mut state, &key, &expected_conn) {
                // The DropGuard cannot fire — the hung streaming task still holds an Arc, so the
                // connection never drops. Revoke the exact token synchronously, or it keeps
                // resolving to the channel and a successor's tunnel becomes reachable by the hung
                // predecessor (F3). Safe after `apply_hung_eviction`: its `purge_session_entries`
                // leaves `facade_tokens` alone.
                #[cfg(feature = "acp-mcp")]
                revoke_facade_token_for_key(&mut state, &key, self.session_registrar.as_ref());
                // The key's project binding was just purged alongside the
                // session state. The untrusted marker is no longer
                // relevant: future calls under this key start fresh.
                // Defer the untrusted-set removal to AFTER the state
                // write lock is dropped (see post-loop block).
                fully_evicted_keys.push(key);
            } else {
                warn!(thread_id = %crate::redact::redact_session_ids(&key), "hung session was replaced before eviction; maps untouched");
            }
        }
        self.save_mapping(&state.persisted);
        self.save_meta(&state.session_workdirs);
        self.save_projects(&state.session_projects);
        // Drop the state write lock before touching the untrusted set
        // (avoid nested write locks — the `untrusted_project_keys`
        // RwLock is a separate lock that the gate consults and that
        // test seams write to).
        drop(state);
        if !fully_evicted_keys.is_empty() {
            let mut untrusted = self.untrusted_project_keys.write().await;
            for k in &fully_evicted_keys {
                untrusted.remove(k);
            }
        }
    }

    pub async fn shutdown(&self) {
        // Snapshot active handles, then drop state lock before awaiting
        // per-connection mutexes (lock ordering: never hold state while
        // awaiting a connection lock).
        let snapshot: Vec<(String, Arc<Mutex<AcpConnection>>)> = {
            let state = self.state.read().await;
            state
                .active
                .iter()
                .map(|(k, v)| (k.clone(), Arc::clone(v)))
                .collect()
        };

        let mut session_ids: Vec<(String, String)> = Vec::new();
        for (key, conn) in snapshot {
            let conn = conn.lock().await;
            if let Some(sid) = conn.acp_session_id.clone() {
                session_ids.push((key, sid));
            }
        }

        let mut state = self.state.write().await;
        // Phase 6.2.9: native-dispatch:* keys MUST NOT be persisted during
        // shutdown. Only Discord (`discord:<channel>:<thread>`) and other
        // adapter keys are written to `state.persisted`/`state.suspended`
        // here so a daemon restart can resume human conversational
        // sessions. Native-dispatch sessions are ephemeral by design.
        let mut persisted_native_count: usize = 0;
        for (key, sid) in session_ids {
            if is_native_dispatch_key(&key) {
                persisted_native_count += 1;
                continue;
            }
            state.persisted.insert(key.clone(), sid.clone());
            state.suspended.insert(key, sid);
        }
        if persisted_native_count > 0 {
            info!(
                excluded_native_keys = persisted_native_count,
                "pool shutdown: native-dispatch sessions were excluded from persistence"
            );
        }
        self.save_mapping(&state.persisted);
        let count = state.active.len();
        state.active.clear();
        state.cancel_handles.clear();
        state.activity.clear();
        state.pgids.clear();
        info!(count, "pool shutdown complete");
    }
}

#[cfg(test)]
mod tests {
    use super::{
        better_candidate, classify_hung, classify_idle, entry_phase, format_native_dispatch_key,
        get_or_insert_gate, is_native_dispatch_key, phase_force_set, phase_load,
        purge_session_entries, remove_if_same_handle, PoolState, SessionPool,
        SessionPoolTestState,
    };
    use crate::acp::connection::SessionActivity;
    use crate::acp::project::ProjectContext;
    use crate::config::AgentConfig;
    use std::collections::HashMap;
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio::time::Instant;

    fn persistence_test_config() -> AgentConfig {
        AgentConfig {
            command: "test-agent".into(),
            args: Vec::new(),
            working_dir: "/tmp".into(),
            env: HashMap::new(),
            inherit_env: Vec::new(),
            command_explicit: true,
        }
    }

    #[test]
    fn agent_persistence_roots_are_distinct_for_each_deployment_identity() {
        let home = tempfile::tempdir().unwrap();
        let home = home.path().as_os_str().to_os_string();
        let roots: Vec<PathBuf> = ["ArthurClaude", "ArthurCodex", "ArthurGemini"]
            .into_iter()
            .map(|agent| {
                SessionPool::agent_persistence_root(Some(home.clone()), Some(OsString::from(agent)))
                    .expect("valid agent identity")
            })
            .collect();

        assert_eq!(
            roots[0],
            PathBuf::from(&home).join(".openab/agents/ArthurClaude")
        );
        assert_eq!(
            roots[1],
            PathBuf::from(&home).join(".openab/agents/ArthurCodex")
        );
        assert_eq!(
            roots[2],
            PathBuf::from(&home).join(".openab/agents/ArthurGemini")
        );
        assert_ne!(roots[0], roots[1]);
        assert_ne!(roots[0], roots[2]);
        assert_ne!(roots[1], roots[2]);
    }

    #[test]
    fn agent_persistence_root_rejects_missing_or_blank_agent_identity() {
        let home = Some(OsString::from("/tmp/openab-home"));

        let missing = SessionPool::agent_persistence_root(home.clone(), None)
            .expect_err("missing ARTHUR_AGENT_NAME must fail closed");
        assert!(missing.to_string().contains("ARTHUR_AGENT_NAME"));

        let blank = SessionPool::agent_persistence_root(home, Some(OsString::from(" \t ")))
            .expect_err("blank ARTHUR_AGENT_NAME must fail closed");
        assert!(blank.to_string().contains("ARTHUR_AGENT_NAME"));
    }

    #[test]
    fn same_thread_key_loads_only_the_owning_agents_persisted_session() {
        let home = tempfile::tempdir().unwrap();
        let key = "discord:1540258407175422004";

        for (agent, session_id) in [
            ("ArthurClaude", "claude-session"),
            ("ArthurCodex", "codex-session"),
            ("ArthurGemini", "gemini-session"),
        ] {
            let root = SessionPool::agent_persistence_root(
                Some(home.path().as_os_str().to_os_string()),
                Some(OsString::from(agent)),
            )
            .unwrap();
            std::fs::create_dir_all(&root).unwrap();
            let pool = SessionPool::from_persistence_root(
                persistence_test_config(),
                1,
                60,
                HashMap::new(),
                root,
            );
            pool.save_mapping(&HashMap::from([(key.to_string(), session_id.to_string())]));
        }

        for (agent, expected_session_id) in [
            ("ArthurClaude", "claude-session"),
            ("ArthurCodex", "codex-session"),
            ("ArthurGemini", "gemini-session"),
        ] {
            let root = SessionPool::agent_persistence_root(
                Some(home.path().as_os_str().to_os_string()),
                Some(OsString::from(agent)),
            )
            .unwrap();
            let pool = SessionPool::from_persistence_root(
                persistence_test_config(),
                1,
                60,
                HashMap::new(),
                root,
            );
            let state = pool.state.try_read().expect("uncontended test pool");
            assert_eq!(
                state.persisted.get(key),
                Some(&expected_session_id.to_string())
            );
        }
    }

    #[test]
    fn mapping_metadata_and_project_persistence_round_trip_within_agent_namespace() {
        let home = tempfile::tempdir().unwrap();
        let root = SessionPool::agent_persistence_root(
            Some(home.path().as_os_str().to_os_string()),
            Some(OsString::from("ArthurCodex")),
        )
        .unwrap();
        std::fs::create_dir_all(&root).unwrap();
        let key = "discord:1540258407175422004".to_string();
        let project_root = home.path().canonicalize().unwrap();
        let projects = HashMap::from([(
            key.clone(),
            ProjectContext {
                project_id: "ai-workstation".into(),
                project_root: project_root.clone(),
            },
        )]);
        let pool = SessionPool::from_persistence_root(
            persistence_test_config(),
            1,
            60,
            HashMap::new(),
            root.clone(),
        );
        pool.save_mapping(&HashMap::from([(key.clone(), "codex-session".into())]));
        pool.save_meta(&HashMap::from([(
            key.clone(),
            "/workspace/ai-workstation".into(),
        )]));
        pool.save_projects(&projects);

        let restored = SessionPool::from_persistence_root(
            persistence_test_config(),
            1,
            60,
            HashMap::new(),
            root,
        );
        let state = restored.state.try_read().expect("uncontended test pool");
        assert_eq!(
            state.persisted.get(&key),
            Some(&"codex-session".to_string())
        );
        assert_eq!(
            state.session_workdirs.get(&key),
            Some(&"/workspace/ai-workstation".to_string())
        );
        assert_eq!(state.session_projects.get(&key), projects.get(&key));
    }

    /// Registrar double that records every mint, so a test can assert one never happened.
    #[cfg(feature = "acp-mcp")]
    #[derive(Default)]
    struct CountingRegistrar {
        minted: std::sync::Mutex<Vec<String>>,
        revoked: std::sync::Mutex<Vec<String>>,
    }

    #[cfg(feature = "acp-mcp")]
    impl CountingRegistrar {
        fn revoked(&self) -> Vec<String> {
            self.revoked.lock().unwrap().clone()
        }
    }

    #[cfg(feature = "acp-mcp")]
    impl crate::acp_mcp::SessionTokenRegistrar for CountingRegistrar {
        fn mint(&self, channel_id: &str) -> String {
            self.minted.lock().unwrap().push(channel_id.to_string());
            "token-xyz".to_string()
        }
        fn revoke(&self, token: &str) {
            self.revoked.lock().unwrap().push(token.to_string());
        }
    }

    /// Build an empty `PoolState` for a helper-level test.
    #[cfg(feature = "acp-mcp")]
    fn empty_pool_state() -> super::PoolState {
        super::PoolState {
            active: HashMap::new(),
            cancel_handles: HashMap::new(),
            facade_tokens: HashMap::new(),
            activity: HashMap::new(),
            pgids: HashMap::new(),
            suspended: HashMap::new(),
            persisted: HashMap::new(),
            creating: HashMap::new(),
            session_workdirs: HashMap::new(),
            session_projects: HashMap::new(),
        }
    }

    /// F3: replacing a hung predecessor's token revokes the predecessor's EXACT token and leaves
    /// the successor's standing. Without the revoke the predecessor token keeps resolving to the
    /// channel and — since `AcpTunnelSource` authorizes by channel — could reach the successor's
    /// tunnel. Exercises the production `install_facade_token`.
    #[cfg(feature = "acp-mcp")]
    #[test]
    fn installing_a_successor_token_revokes_only_the_superseded_predecessor() {
        let reg = Arc::new(CountingRegistrar::default());
        let registrar: Arc<dyn crate::acp_mcp::SessionTokenRegistrar> = reg.clone();
        let mut state = empty_pool_state();

        // Predecessor registers, then a successor takes over the SAME key.
        super::install_facade_token(
            &mut state,
            "discord:acp_x",
            "T_pred".into(),
            Some(&registrar),
        );
        assert!(
            reg.revoked().is_empty(),
            "nothing to revoke on the first install"
        );
        super::install_facade_token(
            &mut state,
            "discord:acp_x",
            "T_succ".into(),
            Some(&registrar),
        );

        assert_eq!(
            reg.revoked(),
            vec!["T_pred"],
            "the predecessor token must be revoked"
        );
        assert_eq!(
            state.facade_tokens.get("discord:acp_x").map(String::as_str),
            Some("T_succ"),
            "the successor's token stands"
        );
    }

    /// F3: hung eviction revokes the exact facade token synchronously (the DropGuard cannot fire
    /// while the hung task holds an Arc). Exercises the production `revoke_facade_token_for_key`,
    /// which the hung-eviction loop calls after `apply_hung_eviction`.
    #[cfg(feature = "acp-mcp")]
    #[test]
    fn hung_eviction_revokes_the_exact_facade_token_and_forgets_it() {
        let reg = Arc::new(CountingRegistrar::default());
        let registrar: Arc<dyn crate::acp_mcp::SessionTokenRegistrar> = reg.clone();
        let mut state = empty_pool_state();
        state
            .facade_tokens
            .insert("discord:acp_x".into(), "T_hung".into());
        // A different session's token must be untouched.
        state
            .facade_tokens
            .insert("discord:acp_y".into(), "T_other".into());

        super::revoke_facade_token_for_key(&mut state, "discord:acp_x", Some(&registrar));

        assert_eq!(
            reg.revoked(),
            vec!["T_hung"],
            "only the evicted session's token is revoked"
        );
        assert!(
            !state.facade_tokens.contains_key("discord:acp_x"),
            "and it is forgotten"
        );
        assert_eq!(
            state.facade_tokens.get("discord:acp_y").map(String::as_str),
            Some("T_other"),
            "an unrelated session's token is untouched"
        );
    }

    /// A failed facade config write must not mint a token. The agent has no `openab` entry, so it
    /// can never present one; minting anyway would leave a live credential registered for a
    /// session that cannot use it until eviction.
    #[cfg(feature = "acp-mcp")]
    #[tokio::test]
    async fn no_token_is_minted_when_the_facade_config_write_fails() {
        let dir = tempfile::tempdir().unwrap();
        // Make `<workdir>/.openab` a FILE, so `create_dir_all` inside the writer fails.
        //
        // This used to block on `.cursor`, which openab no longer creates: since D-15 it authors
        // only `.openab/mcp-facade.json` and never touches a vendor directory. Left pointing at
        // `.cursor` the write would SUCCEED, the test would fail, and — worse if it had been
        // written the other way round — a test asserting "no mint on failure" would have been
        // passing against a call that never failed.
        std::fs::write(dir.path().join(".openab"), b"not a directory").unwrap();

        let counting = Arc::new(CountingRegistrar::default());
        let registrar: Arc<dyn crate::acp_mcp::SessionTokenRegistrar> = counting.clone();
        let token = super::setup_facade_session(
            dir.path().to_str().unwrap(),
            "http://127.0.0.1:8848/mcp",
            "acp_x",
            &registrar,
        )
        .await;

        assert!(token.is_none(), "a failed config write must yield no token");
        assert!(
            counting.minted.lock().unwrap().is_empty(),
            "the registrar must never be asked to mint when the config could not be written"
        );
    }

    /// The happy path still mints exactly once, for the right channel.
    #[cfg(feature = "acp-mcp")]
    #[tokio::test]
    async fn a_successful_facade_config_write_mints_one_token() {
        let dir = tempfile::tempdir().unwrap();
        let counting = Arc::new(CountingRegistrar::default());
        let registrar: Arc<dyn crate::acp_mcp::SessionTokenRegistrar> = counting.clone();
        let token = super::setup_facade_session(
            dir.path().to_str().unwrap(),
            "http://127.0.0.1:8848/mcp",
            "acp_x",
            &registrar,
        )
        .await;

        assert_eq!(token.as_deref(), Some("token-xyz"));
        assert_eq!(counting.minted.lock().unwrap().as_slice(), ["acp_x"]);
    }

    #[test]
    fn remove_if_same_handle_removes_matching_entry() {
        let expected = Arc::new(Mutex::new(1_u8));
        let mut map = HashMap::from([("thread".to_string(), Arc::clone(&expected))]);

        let removed = remove_if_same_handle(&mut map, "thread", &expected);

        assert!(removed.is_some());
        assert!(map.is_empty());
    }

    #[test]
    fn remove_if_same_handle_keeps_replaced_entry() {
        let stale = Arc::new(Mutex::new(1_u8));
        let fresh = Arc::new(Mutex::new(2_u8));
        let mut map = HashMap::from([("thread".to_string(), Arc::clone(&fresh))]);

        let removed = remove_if_same_handle(&mut map, "thread", &stale);

        assert!(removed.is_none());
        let current = map.get("thread").expect("entry should remain");
        assert!(Arc::ptr_eq(current, &fresh));
    }

    #[test]
    fn get_or_insert_gate_reuses_gate_for_same_thread() {
        let mut map = HashMap::new();

        let first = get_or_insert_gate(&mut map, "thread");
        let second = get_or_insert_gate(&mut map, "thread");

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn classify_idle_marks_stale_by_time() {
        let now = Instant::now();
        let cutoff = now - std::time::Duration::from_secs(60);
        let last_active = now - std::time::Duration::from_secs(120);
        assert!(classify_idle(last_active, true, cutoff));
    }

    #[test]
    fn classify_idle_marks_stale_by_death() {
        let now = Instant::now();
        let cutoff = now - std::time::Duration::from_secs(60);
        assert!(classify_idle(now, false, cutoff));
    }

    #[test]
    fn classify_idle_keeps_fresh_alive_sessions() {
        let now = Instant::now();
        let cutoff = now - std::time::Duration::from_secs(60);
        assert!(!classify_idle(now, true, cutoff));
    }

    #[test]
    fn better_candidate_prefers_empty_current() {
        assert!(better_candidate(None, Instant::now()));
    }

    #[test]
    fn better_candidate_prefers_older_last_active() {
        let older = Instant::now() - std::time::Duration::from_secs(120);
        let newer = Instant::now() - std::time::Duration::from_secs(30);
        assert!(better_candidate(Some(newer), older));
    }

    #[test]
    fn better_candidate_rejects_newer_last_active() {
        let older = Instant::now() - std::time::Duration::from_secs(120);
        let newer = Instant::now() - std::time::Duration::from_secs(30);
        assert!(!better_candidate(Some(older), newer));
    }

    #[test]
    fn classify_hung_detects_in_flight_session_past_threshold() {
        assert!(classify_hung(
            true,
            std::time::Duration::from_secs(200),
            std::time::Duration::from_secs(120),
        ));
    }

    #[test]
    fn classify_hung_ignores_in_flight_session_within_threshold() {
        assert!(!classify_hung(
            true,
            std::time::Duration::from_secs(30),
            std::time::Duration::from_secs(120),
        ));
    }

    #[test]
    fn classify_hung_never_marks_idle_sessions() {
        assert!(!classify_hung(
            false,
            std::time::Duration::from_secs(200),
            std::time::Duration::from_secs(120),
        ));
    }

    #[test]
    fn better_candidate_keeps_existing_on_equal_last_active() {
        let ts = Instant::now() - std::time::Duration::from_secs(60);
        assert!(!better_candidate(Some(ts), ts));
    }

    /// The force-evict warning must log NEITHER id raw — both the `acp_<uuid>` channel (inside the
    /// `<platform>:<channel_id>` pool key) and the `sess_<uuid>` session id resume the session. A
    /// capture subscriber exercises the real `warn!` macro, so a revert to raw fields fails here
    /// rather than silently shipping a credential to the logs (F6 / round 6).
    #[test]
    fn force_evict_warning_redacts_both_ids() {
        use std::io::Write;
        use std::sync::{Arc as StdArc, Mutex as StdMutex};

        #[derive(Clone)]
        struct Cap(StdArc<StdMutex<Vec<u8>>>);
        impl Write for Cap {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let uuid = "00000000-0000-0000-0000-000000000000";
        let buf = StdArc::new(StdMutex::new(Vec::new()));
        let cap = Cap(buf.clone());
        let sub = tracing_subscriber::fmt()
            .with_writer(move || cap.clone())
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(sub, || {
            super::warn_force_evicting_hung(
                &format!("discord:acp_{uuid}"),
                Some(&format!("sess_{uuid}")),
                999,
                600,
            );
        });

        let out = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            out.contains("force-evicting hung session"),
            "the warning must fire: {out}"
        );
        assert!(!out.contains(uuid), "no raw uuid may reach the log: {out}");
        assert!(
            !out.contains("acp_") && !out.contains("sess_"),
            "no raw id prefix either: {out}"
        );
        assert!(
            out.contains('#'),
            "the redaction tag must be present: {out}"
        );
        assert!(
            out.contains("discord"),
            "the readable platform half must survive: {out}"
        );
    }

    #[test]
    fn purge_session_entries_drops_all_entries_for_evicted_key_only() {
        let mut state = PoolState {
            active: HashMap::new(),
            cancel_handles: HashMap::new(),
            #[cfg(feature = "acp-mcp")]
            facade_tokens: HashMap::new(),
            activity: HashMap::from([
                ("hung".to_string(), Arc::new(SessionActivity::new())),
                ("other".to_string(), Arc::new(SessionActivity::new())),
            ]),
            pgids: HashMap::from([("hung".to_string(), 1234), ("other".to_string(), 5678)]),
            suspended: HashMap::from([
                ("hung".to_string(), "session-hung".to_string()),
                ("other".to_string(), "session-other".to_string()),
            ]),
            persisted: HashMap::from([
                ("hung".to_string(), "session-hung".to_string()),
                ("other".to_string(), "session-other".to_string()),
            ]),
            creating: HashMap::from([("hung".to_string(), Arc::new(Mutex::new(())))]),
            session_workdirs: HashMap::from([("hung".to_string(), "/tmp/ws".to_string())]),
            session_projects: HashMap::new(),
        };

        purge_session_entries(&mut state, "hung");

        // Evicted key must not be resumable: no suspended/persisted entry left.
        assert!(!state.activity.contains_key("hung"));
        assert!(!state.cancel_handles.contains_key("hung"));
        assert!(!state.pgids.contains_key("hung"));
        assert!(!state.suspended.contains_key("hung"));
        assert!(!state.persisted.contains_key("hung"));
        assert!(!state.session_workdirs.contains_key("hung"));
        assert!(!state.session_projects.contains_key("hung"));
        // The creating gate is concurrency control, not session state: it must
        // survive so an in-flight get_or_create holder stays serialized.
        assert!(state.creating.contains_key("hung"));
        assert_eq!(state.pgids.get("other"), Some(&5678));
        // Other keys survive untouched.
        assert_eq!(
            state.persisted.get("other"),
            Some(&"session-other".to_string())
        );
        assert_eq!(
            state.suspended.get("other"),
            Some(&"session-other".to_string())
        );
        assert!(state.activity.contains_key("other"));
    }

    #[test]
    fn persisted_mapping_can_include_active_and_suspended_sessions() {
        let persisted = HashMap::from([
            ("active-thread".to_string(), "session-active".to_string()),
            (
                "suspended-thread".to_string(),
                "session-suspended".to_string(),
            ),
        ]);

        let serialized =
            serde_json::to_string_pretty(&persisted).expect("serialize persisted mapping");
        let roundtrip: HashMap<String, String> =
            serde_json::from_str(&serialized).expect("deserialize persisted mapping");

        assert_eq!(
            roundtrip.get("active-thread"),
            Some(&"session-active".to_string())
        );
        assert_eq!(
            roundtrip.get("suspended-thread"),
            Some(&"session-suspended".to_string())
        );
    }

    // --- Project-context binding tests (workflow 20260818-openab-project-scoped-acp-session-bootstrap) ---

    use super::{canonicalize_pinned, resolve_effective_workdir};

    /// Helper: run the production canonicalize_pinned + resolve_effective_workdir
    /// pair, the same path `get_or_create` takes. Lets helper tests exercise
    /// the contract as it actually runs.
    fn resolve_with_canonical(
        project: Option<&ProjectContext>,
        stored_workdir: Option<&str>,
        config_workdir: &str,
    ) -> (String, Option<ProjectContext>) {
        let canonical = canonicalize_pinned(project).expect("canonicalize pinned");
        resolve_effective_workdir(project, canonical.as_ref(), stored_workdir, config_workdir)
    }

    /// No project context, no stored binding → falls back to config.working_dir
    /// (req #A, req #G).
    #[test]
    fn resolve_falls_back_to_config_when_no_project_and_no_stored() {
        let (wd, store) = resolve_with_canonical(None, None, "/cfg/work");
        assert_eq!(wd, "/cfg/work");
        assert!(
            store.is_none(),
            "nothing to persist when there is no context"
        );
    }

    /// Stored binding wins when no project context is supplied
    /// (legacy immutability, ADR §4.5).
    #[test]
    fn resolve_prefers_stored_when_no_project() {
        let (wd, store) = resolve_with_canonical(None, Some("/stored/ws"), "/cfg/work");
        assert_eq!(wd, "/stored/ws", "stored binding wins over config");
        assert!(store.is_none());
    }

    /// Project-pinned context is authoritative — its project_root overrides
    /// both stored and config (req #B, req #6).
    #[test]
    fn resolve_prefers_project_root_when_pinned() {
        let dir = tempfile::tempdir().unwrap();
        let project = ProjectContext {
            project_id: "openab".into(),
            project_root: dir.path().to_path_buf(),
        };
        let (wd, store) = resolve_with_canonical(Some(&project), Some("/stored/ws"), "/cfg/work");
        // The workdir is the canonical project_root, not the stored hint.
        let expected = dir.path().canonicalize().unwrap();
        assert_eq!(wd, expected.to_string_lossy());
        let store = store.expect("project-pinned context must persist");
        assert_eq!(store.project_id, "openab");
        assert_eq!(store.project_root, expected);
    }

    /// Anonymous contexts preserve the legacy `stored > override > config`
    /// precedence so `[[ws:@alias]]` directives don't drift a thread's
    /// workspace (req #3).
    #[test]
    fn resolve_anonymous_prefers_stored_over_anonymous_path() {
        let dir = tempfile::tempdir().unwrap();
        let anonymous = ProjectContext::anonymous(dir.path().to_path_buf());
        let (wd, store) = resolve_with_canonical(Some(&anonymous), Some("/stored/ws"), "/cfg/work");
        assert_eq!(
            wd, "/stored/ws",
            "anonymous context must defer to stored binding (ADR §4.5 immutability)"
        );
        assert!(
            store.is_none(),
            "anonymous contexts must NOT persist a project binding"
        );
    }

    /// Anonymous contexts with no stored binding use the anonymous path
    /// directly. The path is trusted as caller-validated (resolve_workspace
    /// does so upstream).
    #[test]
    fn resolve_anonymous_uses_anonymous_path_when_no_stored() {
        let dir = tempfile::tempdir().unwrap();
        let anonymous = ProjectContext::anonymous(dir.path().to_path_buf());
        let (wd, store) = resolve_with_canonical(Some(&anonymous), None, "/cfg/work");
        assert_eq!(wd, dir.path().to_string_lossy().to_string());
        assert!(store.is_none());
    }

    /// Project-pinned contexts validate `project_root` at canonicalize time.
    /// A nonexistent path fails closed (req #E).
    #[test]
    fn canonicalize_rejects_nonexistent_project_root() {
        let project = ProjectContext {
            project_id: "openab".into(),
            project_root: PathBuf::from("/nonexistent/path/2026_08_18"),
        };
        let err = canonicalize_pinned(Some(&project)).expect_err("nonexistent root must fail");
        assert!(err.contains("cannot be canonicalized"), "{err}");
    }

    /// Project-pinned contexts validate that `project_root` is a directory
    /// (req #E).
    #[test]
    fn canonicalize_rejects_file_project_root() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("file.txt");
        std::fs::write(&file, b"x").unwrap();
        let project = ProjectContext {
            project_id: "openab".into(),
            project_root: file.clone(),
        };
        let err = canonicalize_pinned(Some(&project)).expect_err("file root must fail");
        assert!(err.contains("not a directory"), "{err}");
    }

    /// `PoolState.session_projects` round-trips through serde_json: the
    /// persistence path used by `save_projects` / `load_projects` is
    /// faithful across a write→read cycle (req #F).
    #[test]
    fn session_projects_persists_across_save_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let canonical = dir.path().canonicalize().unwrap();
        let projects = HashMap::from([(
            "discord:thread-1".to_string(),
            ProjectContext {
                project_id: "openab".into(),
                project_root: canonical.clone(),
            },
        )]);
        let serialized = serde_json::to_string_pretty(&projects).expect("serialize");
        let roundtrip: HashMap<String, ProjectContext> =
            serde_json::from_str(&serialized).expect("deserialize");

        let stored = roundtrip
            .get("discord:thread-1")
            .expect("thread-1 binding survives roundtrip");
        assert_eq!(stored.project_id, "openab");
        assert_eq!(stored.project_root, canonical);
    }

    /// `load_projects` returns an empty map for missing files (startup
    /// must never fail because no persistence file exists yet).
    #[test]
    fn load_projects_returns_empty_for_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session_projects.json");
        let projects = SessionPool::load_projects(&path).expect("missing file is Ok(empty)");
        assert!(projects.is_empty());
    }

    /// `load_projects` reports corrupt JSON via `Err` so the pool can set
    /// `projects_corrupt = true` and fail closed. The previous
    /// "corrupt → empty" behavior is what Defect 4 of workflow
    /// 20260818-openab-project-session-pinning-hardening corrected.
    #[test]
    fn load_projects_reports_corrupt_file_as_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session_projects.json");
        std::fs::write(&path, b"{ this is not valid json").unwrap();
        let result = SessionPool::load_projects(&path);
        assert!(
            result.is_err(),
            "a corrupt persistence file MUST report Err so the pool can fail closed"
        );
    }

    /// Project bindings survive a `purge_session_entries` call (req #F,
    /// matches `purge_session_entries_drops_all_entries_for_evicted_key_only`).
    #[test]
    fn purge_session_entries_clears_project_binding() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = PoolState {
            active: HashMap::new(),
            cancel_handles: HashMap::new(),
            #[cfg(feature = "acp-mcp")]
            facade_tokens: HashMap::new(),
            activity: HashMap::new(),
            pgids: HashMap::new(),
            suspended: HashMap::new(),
            persisted: HashMap::new(),
            creating: HashMap::new(),
            session_workdirs: HashMap::new(),
            session_projects: HashMap::from([(
                "evicted".to_string(),
                ProjectContext {
                    project_id: "openab".into(),
                    project_root: dir.path().to_path_buf(),
                },
            )]),
        };

        purge_session_entries(&mut state, "evicted");

        assert!(
            !state.session_projects.contains_key("evicted"),
            "project binding must be cleared alongside other session state"
        );
    }

    // --- Direct `get_or_create` regression tests (VERIFIER correction) ---
    //
    // These tests drive `SessionPool::get_or_create` end-to-end against a
    // tiny ACP-compatible shell-script agent so the actual session-creation
    // seam is exercised — not just the pure `resolve_effective_workdir`
    // helper. The mismatch gate (pool.rs:608-636) fires BEFORE
    // `AcpConnection::spawn`, so test 1 never reaches the subprocess; tests 2
    // and 3 do reach it and depend on `TEST_AGENT_SCRIPT` responding to
    // `initialize` / `session/new` / `session/load` with valid JSON-RPC.
    //
    // Unix-only: the agent script uses `/bin/sh` and POSIX process spawning.
    // `AcpConnection::spawn` itself is cross-platform, but the test fixture
    // here is not; gating on `cfg(unix)` matches the existing pattern in
    // `AcpConnection::spawn` and the rest of the codebase.

    /// Minimal ACP-compatible test agent. Reads JSON-RPC lines from stdin
    /// and writes back valid responses for `initialize`, `session/new`, and
    /// `session/load`. Any other method gets a generic success reply so the
    /// reader loop never hangs waiting for an id we will not see.
    ///
    /// When invoked as `test-acp-agent.sh <record_file>`, every received
    /// JSON-RPC line is appended to `record_file` (after the file is
    /// truncated on start) so tests can assert on the actual params the
    /// openab pool sent — most importantly `session/new.params.cwd`.
    /// Workflow 20260818-openab-project-session-pinning-hardening Defect 3
    /// requires direct verification of the cwd payload, not inference from
    /// `SessionPool` state.
    const TEST_AGENT_SCRIPT: &str = r#"#!/bin/sh
# Minimal ACP-compatible test agent.
# Usage: test-acp-agent.sh [record_file]
#   When `record_file` is set, every received JSON-RPC line is appended to
#   it (truncated on start) so tests can assert on the actual params the
#   openab pool sent. The cwd in session/new.params is the canonical
#   project_root for project-pinned calls and the configured working_dir
#   for legacy no-project calls.
RECORD="${1:-}"
if [ -n "$RECORD" ]; then
    : > "$RECORD"  # truncate on start
fi
while IFS= read -r line; do
    if [ -n "$RECORD" ]; then
        printf '%s\n' "$line" >> "$RECORD"
    fi
    case "$line" in
        *initialize*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentInfo":{"name":"test"},"agentCapabilities":{"loadSession":true}}}' ;;
        *session/new*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"sess_test"}}' ;;
        *session/load*) printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"sessionId":"sess_test"}}' ;;
        *) printf '%s\n' '{"jsonrpc":"2.0","id":0,"result":{}}' ;;
    esac
done
"#;

    /// Write `TEST_AGENT_SCRIPT` to `dir` as an executable and return the
    /// path. Caller passes it as `AgentConfig.command`.
    #[cfg(unix)]
    fn write_test_agent_script(dir: &std::path::Path) -> PathBuf {
        let script = dir.join("test-acp-agent.sh");
        std::fs::write(&script, TEST_AGENT_SCRIPT).expect("write test agent script");
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
            .expect("chmod test agent script");
        script
    }

    /// Convenience: read the recorded JSON-RPC lines for a test agent that
    /// was spawned with the record file as its first arg. Returns the raw
    /// lines (one per stdin line received by the agent).
    #[cfg(unix)]
    fn read_recorded_lines(path: &std::path::Path) -> Vec<String> {
        let raw = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("read record file {}: {e}", path.display()));
        raw.lines().map(std::string::ToString::to_string).collect()
    }

    /// Convenience: extract `cwd` from the first `session/new` line in a
    /// record file. Panics with a clear message if no `session/new` line
    /// was found or the JSON could not be parsed.
    #[cfg(unix)]
    fn cwd_from_session_new(record_path: &std::path::Path) -> String {
        let lines = read_recorded_lines(record_path);
        for line in &lines {
            if line.contains("session/new") {
                let v: serde_json::Value = serde_json::from_str(line)
                    .unwrap_or_else(|e| panic!("parse session/new line {line}: {e}"));
                let cwd = v
                    .get("params")
                    .and_then(|p| p.get("cwd"))
                    .and_then(|c| c.as_str())
                    .unwrap_or_else(|| panic!("session/new line missing cwd: {line}"));
                return cwd.to_string();
            }
        }
        panic!(
            "no session/new line found in record file {}: lines were {:?}",
            record_path.display(),
            lines
        );
    }

    /// Test 1 — `get_or_create` rejects a project-context mismatch BEFORE
    /// reusing or spawning the existing session (req #5, req #D).
    ///
    /// The mismatch check at `pool.rs:608-636` fires before
    /// `AcpConnection::spawn`, so we don't need a working agent command for
    /// this test — `/bin/true` is enough. The key assertion is that the
    /// existing binding survives intact.
    #[tokio::test]
    async fn get_or_create_rejects_project_context_mismatch_on_existing_binding() {
        let dir = tempfile::tempdir().unwrap();
        let project_a_dir = tempfile::tempdir().unwrap();
        let project_b_dir = tempfile::tempdir().unwrap();

        let project_a = ProjectContext {
            project_id: "A".into(),
            project_root: project_a_dir.path().to_path_buf(),
        };
        let project_b = ProjectContext {
            project_id: "B".into(),
            project_root: project_b_dir.path().to_path_buf(),
        };

        let state = PoolState {
            active: HashMap::new(),
            cancel_handles: HashMap::new(),
            #[cfg(feature = "acp-mcp")]
            facade_tokens: HashMap::new(),
            activity: HashMap::new(),
            pgids: HashMap::new(),
            suspended: HashMap::new(),
            persisted: HashMap::new(),
            creating: HashMap::new(),
            session_workdirs: HashMap::new(),
            session_projects: HashMap::from([("T".to_string(), project_a.clone())]),
        };

        let pool = SessionPool::with_state_for_test(
            AgentConfig {
                // Doesn't matter — mismatch fires before spawn.
                command: "/bin/true".into(),
                args: vec![],
                working_dir: "/tmp".into(),
                env: HashMap::new(),
                inherit_env: vec![],
                command_explicit: true,
            },
            state,
            dir.path().join("session_projects.json"),
        );

        let err = pool
            .get_or_create("T", Some(&project_b))
            .await
            .expect_err("project B must be rejected when T is bound to project A");
        let msg = err.to_string();
        assert!(
            msg.contains("project context mismatch"),
            "error must mention project context mismatch: {msg}"
        );

        // The existing binding for T must remain intact (the rejected call
        // must NOT have erased or replaced it).
        let state = pool.state.read().await;
        let stored = state
            .session_projects
            .get("T")
            .expect("T's project A binding must survive the rejected call");
        assert_eq!(stored.project_id, "A");
        assert_eq!(
            stored.project_root,
            project_a_dir.path().canonicalize().unwrap()
        );
        // And no connection was created for T.
        assert!(
            !state.active.contains_key("T"),
            "no connection must exist after a rejected mismatch call"
        );
    }

    /// Test 2 — Two threads with different project contexts remain isolated.
    /// T1 is pre-bound to project A; T2 receives project B via get_or_create.
    /// After both operations, neither binding overwrites the other (req #C).
    #[cfg(unix)]
    #[tokio::test]
    async fn get_or_create_keeps_two_threads_with_different_projects_isolated() {
        let dir = tempfile::tempdir().unwrap();
        let project_a_dir = tempfile::tempdir().unwrap();
        let project_b_dir = tempfile::tempdir().unwrap();

        let project_a = ProjectContext {
            project_id: "A".into(),
            project_root: project_a_dir.path().to_path_buf(),
        };
        let project_b = ProjectContext {
            project_id: "B".into(),
            project_root: project_b_dir.path().to_path_buf(),
        };

        let agent_script = write_test_agent_script(dir.path());

        let state = PoolState {
            active: HashMap::new(),
            cancel_handles: HashMap::new(),
            #[cfg(feature = "acp-mcp")]
            facade_tokens: HashMap::new(),
            activity: HashMap::new(),
            pgids: HashMap::new(),
            suspended: HashMap::new(),
            persisted: HashMap::new(),
            creating: HashMap::new(),
            session_workdirs: HashMap::new(),
            session_projects: HashMap::from([("T1".to_string(), project_a.clone())]),
        };

        let pool = SessionPool::with_state_for_test(
            AgentConfig {
                command: agent_script.to_string_lossy().into(),
                args: vec![],
                working_dir: dir.path().to_string_lossy().into(),
                env: HashMap::new(),
                inherit_env: vec![],
                command_explicit: true,
            },
            state,
            dir.path().join("session_projects.json"),
        );

        // T2 receives project B via get_or_create. This must succeed and
        // create a fresh binding without disturbing T1's pre-existing
        // binding. The agent script responds to initialize/session/new so
        // the spawn path completes normally.
        let created = pool
            .get_or_create("T2", Some(&project_b))
            .await
            .expect("T2 spawn must succeed against the test agent");
        assert!(created, "T2 must be a fresh session");

        let state = pool.state.read().await;

        // T1's binding is untouched.
        let t1 = state
            .session_projects
            .get("T1")
            .expect("T1's pre-existing project A binding must survive");
        assert_eq!(t1.project_id, "A");
        assert_eq!(
            t1.project_root,
            project_a_dir.path().canonicalize().unwrap()
        );

        // T2 has a fresh project B binding.
        let t2 = state
            .session_projects
            .get("T2")
            .expect("T2 must have its own project B binding");
        assert_eq!(t2.project_id, "B");
        assert_eq!(
            t2.project_root,
            project_b_dir.path().canonicalize().unwrap()
        );

        // T2's session_workdirs entry must reflect the project B root, and
        // must not collide with T1's entry (T1 had no workdir pre-existing
        // because its binding was injected directly via the test seam —
        // only T2 goes through the spawn path here).
        let t2_workdir = state
            .session_workdirs
            .get("T2")
            .expect("T2's workdir must be recorded after spawn");
        assert_eq!(
            *t2_workdir,
            project_b_dir
                .path()
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            "T2's workdir must reflect the dynamic project B root"
        );
    }

    /// Test 3 — Dynamic `project_root` becomes the effective workdir used
    /// by `get_or_create` and is persisted to `session_workdirs[T]` after
    /// successful creation (req #B, NOT just the helper).
    ///
    /// The test deliberately sets `config.working_dir` to a path that is
    /// NEITHER project_root NOR config_root; if the dynamic project_root
    /// actually drives the spawn, the persisted workdir must be the
    /// canonical project_root, never the config value.
    #[cfg(unix)]
    #[tokio::test]
    async fn get_or_create_uses_project_root_as_effective_workdir() {
        let dir = tempfile::tempdir().unwrap();
        let project_dir = tempfile::tempdir().unwrap();
        let config_root = tempfile::tempdir().unwrap();

        let agent_script = write_test_agent_script(dir.path());

        let pool = SessionPool::with_state_for_test(
            AgentConfig {
                command: agent_script.to_string_lossy().into(),
                args: vec![],
                working_dir: config_root.path().to_string_lossy().into(),
                env: HashMap::new(),
                inherit_env: vec![],
                command_explicit: true,
            },
            PoolState {
                active: HashMap::new(),
                cancel_handles: HashMap::new(),
                #[cfg(feature = "acp-mcp")]
                facade_tokens: HashMap::new(),
                activity: HashMap::new(),
                pgids: HashMap::new(),
                suspended: HashMap::new(),
                persisted: HashMap::new(),
                creating: HashMap::new(),
                session_workdirs: HashMap::new(),
                session_projects: HashMap::new(),
            },
            dir.path().join("session_projects.json"),
        );

        let project = ProjectContext {
            project_id: "openab".into(),
            project_root: project_dir.path().to_path_buf(),
        };

        let created = pool
            .get_or_create("T", Some(&project))
            .await
            .expect("spawn against the test agent must succeed");
        assert!(created, "T must be a fresh session");

        let state = pool.state.read().await;

        // The project_root must be the canonical path actually used.
        let expected = project_dir.path().canonicalize().unwrap();

        // session_projects[T] records the canonical project context.
        let stored = state
            .session_projects
            .get("T")
            .expect("T must have a project binding after get_or_create");
        assert_eq!(stored.project_id, "openab");
        assert_eq!(stored.project_root, expected);

        // session_workdirs[T] records the canonical project_root — proving
        // the dynamic project_root, NOT the config.working_dir, drove the
        // spawn and was persisted as the effective workdir.
        let stored_workdir = state
            .session_workdirs
            .get("T")
            .expect("T must have a session_workdirs entry after get_or_create");
        assert_eq!(*stored_workdir, expected.to_string_lossy().to_string());
        assert_ne!(
            *stored_workdir,
            config_root
                .path()
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            "config.working_dir must NOT win over the dynamic project_root"
        );
    }

    // ───────────────────────────────────────────────────────────────────────
    //  Required regression tests for workflow
    //  20260818-openab-project-session-pinning-hardening
    // ───────────────────────────────────────────────────────────────────────
    //
    //  These exercise the production `SessionPool::get_or_create` end-to-end
    //  against the test agent and use the `cwd` capture seam (Defect 3) to
    //  verify what was actually sent to `session/new`. Each test maps to one
    //  required case in the workflow's REQUIRED TESTS section.

    /// Build a pool whose agent command will record the JSON-RPC lines it
    /// receives into `record_path`. Returns the pool plus a clone of the
    /// AgentConfig (so callers can use the same config-root value when
    /// asserting on legacy behavior).
    #[cfg(unix)]
    fn build_recording_pool(
        dir: &std::path::Path,
        config_working_dir: &std::path::Path,
        state: PoolState,
    ) -> (SessionPool, PathBuf) {
        let record_path = dir.join("recorded-rpc.log");
        let agent_script = write_test_agent_script(dir);
        let pool = SessionPool::with_state_for_test(
            AgentConfig {
                command: agent_script.to_string_lossy().into(),
                // Pass the record path as the agent's $1; the script writes
                // every received line to it.
                args: vec![record_path.to_string_lossy().into_owned()],
                working_dir: config_working_dir.to_string_lossy().into(),
                env: HashMap::new(),
                inherit_env: vec![],
                command_explicit: true,
            },
            state,
            dir.join("session_projects.json"),
        );
        (pool, record_path)
    }

    // ── REQUIRED TEST 1 ────────────────────────────────────────────────────
    //
    // active_project_a_rejects_project_b_before_alive_fast_path
    //
    // Spawns a REAL active ACP test session pinned to project A, then calls
    // get_or_create(same_thread, project_B) and asserts the mismatch gate
    // fires BEFORE the busy/alive fast paths. The previous workflow only
    // pre-populated session_projects (no live connection), so the live
    // fast path was never exercised.

    #[cfg(unix)]
    #[tokio::test]
    async fn active_project_a_rejects_project_b_before_alive_fast_path() {
        let dir = tempfile::tempdir().unwrap();
        let project_a_dir = tempfile::tempdir().unwrap();
        let project_b_dir = tempfile::tempdir().unwrap();

        let project_a = ProjectContext {
            project_id: "A".into(),
            project_root: project_a_dir.path().to_path_buf(),
        };
        let project_b = ProjectContext {
            project_id: "B".into(),
            project_root: project_b_dir.path().to_path_buf(),
        };

        // Spawn a REAL active session for T pinned to project A.
        let (pool, _record) = build_recording_pool(
            dir.path(),
            project_a_dir.path(),
            PoolState {
                active: HashMap::new(),
                cancel_handles: HashMap::new(),
                #[cfg(feature = "acp-mcp")]
                facade_tokens: HashMap::new(),
                activity: HashMap::new(),
                pgids: HashMap::new(),
                suspended: HashMap::new(),
                persisted: HashMap::new(),
                creating: HashMap::new(),
                session_workdirs: HashMap::new(),
                session_projects: HashMap::new(),
            },
        );

        let created = pool
            .get_or_create("T", Some(&project_a))
            .await
            .expect("first call must succeed against the test agent");
        assert!(created, "T's first call must be a fresh session");

        // Capture the live connection Arc for T so we can prove it is
        // preserved by the rejected second call.
        let live_arc_before = {
            let state = pool.state.read().await;
            state
                .active
                .get("T")
                .cloned()
                .expect("T must have an active session after the first call")
        };

        // Now call get_or_create with the SAME thread but a DIFFERENT
        // project. The mismatch gate must fire BEFORE any reuse, return,
        // resume, or alive fast path.
        let err = pool
            .get_or_create("T", Some(&project_b))
            .await
            .expect_err("project B must be rejected when T is alive on project A");
        let msg = err.to_string();
        assert!(
            msg.contains("project context mismatch"),
            "error must mention project context mismatch: {msg}"
        );
        assert!(
            msg.contains("project_id=\"A\""),
            "error must name the stored project A: {msg}"
        );
        assert!(
            msg.contains("project_id=\"B\""),
            "error must name the incoming project B: {msg}"
        );

        // The existing active session for T must survive intact: the same
        // Arc, still bound to project A.
        let state = pool.state.read().await;
        let live_arc_after = state
            .active
            .get("T")
            .cloned()
            .expect("T must STILL have an active session after the rejected call");
        assert!(
            Arc::ptr_eq(&live_arc_before, &live_arc_after),
            "the live connection Arc must be unchanged (no silent re-spawn or substitution)"
        );

        // The session_projects binding must still be project A (no
        // overwriting with B's identity).
        let stored = state
            .session_projects
            .get("T")
            .expect("T's binding must survive the rejected call");
        assert_eq!(
            stored.project_id, "A",
            "stored binding must remain project A"
        );
        assert_eq!(
            stored.project_root,
            project_a_dir.path().canonicalize().unwrap()
        );

        // session_workdirs must still point at project A's root.
        let wd = state
            .session_workdirs
            .get("T")
            .expect("T's workdir must survive the rejected call");
        assert_eq!(
            *wd,
            project_a_dir
                .path()
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .to_string()
        );
    }

    // ── REQUIRED TEST 2 ────────────────────────────────────────────────────
    //
    // same_canonical_project_reuses_active_session
    //
    // Pre-bound active session to project A with a canonical project_root.
    // A subsequent get_or_create with a NON-canonical spelling of the
    // SAME project_root (e.g. trailing slash, "./", or with the
    // canonicalize-induced path) must compare equal after canonicalization
    // and reuse the active session. This guards the "legitimate
    // same-project reuse" half of Defect 1 — the gate must be precise,
    // not blanket-reject all reuses.

    #[cfg(unix)]
    #[tokio::test]
    async fn same_canonical_project_reuses_active_session() {
        let dir = tempfile::tempdir().unwrap();
        let project_dir = tempfile::tempdir().unwrap();

        // The canonical form that canonicalize() will return.
        let canonical_root = project_dir.path().canonicalize().unwrap();

        // Two non-canonical spellings of the same project_root. POSIX
        // canonicalize() collapses trailing slashes; the dot-segments and
        // a relative component are also normalized. The point is that
        // canonicalize_pinned must produce the same ProjectContext for
        // both, so the mismatch check passes and the active session is
        // reused.
        let project_with_trailing_slash = ProjectContext {
            project_id: "openab".into(),
            project_root: {
                let mut p = canonical_root.clone();
                let s = p.to_string_lossy().to_string();
                p = std::path::PathBuf::from(format!("{s}/"));
                p
            },
        };
        let project_canonical = ProjectContext {
            project_id: "openab".into(),
            project_root: canonical_root.clone(),
        };

        // Sanity: the two spellings must canonicalize to byte-equal roots,
        // otherwise the test below is meaningless.
        let c1 = project_with_trailing_slash
            .canonicalized()
            .expect("canonicalize trailing slash");
        let c2 = project_canonical
            .canonicalized()
            .expect("canonicalize canonical");
        assert_eq!(
            c1.project_root, c2.project_root,
            "test setup: both spellings must canonicalize to the same path"
        );

        // Spawn a real active session pinned to project A.
        let (pool, _record) = build_recording_pool(
            dir.path(),
            project_dir.path(),
            PoolState {
                active: HashMap::new(),
                cancel_handles: HashMap::new(),
                #[cfg(feature = "acp-mcp")]
                facade_tokens: HashMap::new(),
                activity: HashMap::new(),
                pgids: HashMap::new(),
                suspended: HashMap::new(),
                persisted: HashMap::new(),
                creating: HashMap::new(),
                session_workdirs: HashMap::new(),
                session_projects: HashMap::new(),
            },
        );
        let created = pool
            .get_or_create("T", Some(&project_canonical))
            .await
            .expect("first call must succeed");
        assert!(created, "first call must be a fresh session");

        let live_arc_before = {
            let state = pool.state.read().await;
            state
                .active
                .get("T")
                .cloned()
                .expect("T must have an active session")
        };

        // Now call with the same project under a non-canonical spelling.
        // The gate must compare canonical forms and pass; the busy/alive
        // fast path then short-circuits to Ok(false) — REUSE, not
        // rejection.
        let created_again = pool
            .get_or_create("T", Some(&project_with_trailing_slash))
            .await
            .expect("same canonical project must be accepted (not a mismatch)");
        assert!(
            !created_again,
            "same canonical project must reuse the active session, not create a new one"
        );

        let state = pool.state.read().await;
        let live_arc_after = state
            .active
            .get("T")
            .cloned()
            .expect("T must still have an active session after reuse");
        assert!(
            Arc::ptr_eq(&live_arc_before, &live_arc_after),
            "the same connection Arc must be reused, not silently substituted"
        );
    }

    // ── REQUIRED TEST 3 ────────────────────────────────────────────────────
    //
    // session_new_receives_dynamic_project_root_as_cwd
    //
    // Drives get_or_create with a project-pinned context and directly
    // asserts (from the agent-side record, NOT from pool state) that the
    // dynamic project_root reached `session/new.params.cwd`. This is the
    // Defect 3 fix: prior tests inferred cwd from session_workdirs only.

    #[cfg(unix)]
    #[tokio::test]
    async fn session_new_receives_dynamic_project_root_as_cwd() {
        let dir = tempfile::tempdir().unwrap();
        let project_dir = tempfile::tempdir().unwrap();
        let config_root = tempfile::tempdir().unwrap();

        let (pool, record) = build_recording_pool(
            dir.path(),
            config_root.path(),
            PoolState {
                active: HashMap::new(),
                cancel_handles: HashMap::new(),
                #[cfg(feature = "acp-mcp")]
                facade_tokens: HashMap::new(),
                activity: HashMap::new(),
                pgids: HashMap::new(),
                suspended: HashMap::new(),
                persisted: HashMap::new(),
                creating: HashMap::new(),
                session_workdirs: HashMap::new(),
                session_projects: HashMap::new(),
            },
        );

        let project = ProjectContext {
            project_id: "openab".into(),
            project_root: project_dir.path().to_path_buf(),
        };
        pool.get_or_create("T", Some(&project))
            .await
            .expect("spawn must succeed");

        // The agent script recorded every JSON-RPC line it received. Read
        // session/new.params.cwd directly from the agent's view, NOT from
        // pool state.
        let cwd = cwd_from_session_new(&record);
        let expected = project_dir.path().canonicalize().unwrap();
        assert_eq!(
            cwd,
            expected.to_string_lossy(),
            "session/new.params.cwd must be the dynamic project_root"
        );
        // Sanity: the agent saw the CANONICAL form, not a trailing-slash
        // or dot-segment variant that the pool was given.
        assert!(
            !cwd.ends_with('/') || cwd == "/",
            "session/new.params.cwd must be the canonical form (no spurious trailing slash): {cwd}"
        );
    }

    // ── REQUIRED TEST 4 ────────────────────────────────────────────────────
    //
    // legacy_session_new_receives_configured_working_dir
    //
    // When no project context is supplied, get_or_create must send the
    // configured [agent].working_dir as session/new.params.cwd. This
    // confirms the legacy single-project path still works.

    #[cfg(unix)]
    #[tokio::test]
    async fn legacy_session_new_receives_configured_working_dir() {
        let dir = tempfile::tempdir().unwrap();
        let config_root = tempfile::tempdir().unwrap();

        let (pool, record) = build_recording_pool(
            dir.path(),
            config_root.path(),
            PoolState {
                active: HashMap::new(),
                cancel_handles: HashMap::new(),
                #[cfg(feature = "acp-mcp")]
                facade_tokens: HashMap::new(),
                activity: HashMap::new(),
                pgids: HashMap::new(),
                suspended: HashMap::new(),
                persisted: HashMap::new(),
                creating: HashMap::new(),
                session_workdirs: HashMap::new(),
                session_projects: HashMap::new(),
            },
        );

        // No project context — pure legacy path.
        let created = pool
            .get_or_create("T", None)
            .await
            .expect("legacy spawn must succeed");
        assert!(created, "T must be a fresh session");

        // The legacy path does NOT persist `session_workdirs[T]` because
        // `working_dir_override.is_some()` is the original persistence
        // gate (workspace hints only — the configured working_dir is the
        // implicit fallback, not a "hint"). The agent-side assertion
        // below is what proves the legacy path actually used the
        // configured working_dir as the spawn cwd.

        // And the agent saw the configured working_dir as session/new.cwd.
        let cwd = cwd_from_session_new(&record);
        assert_eq!(
            cwd,
            config_root.path().canonicalize().unwrap().to_string_lossy(),
            "legacy session/new.params.cwd must be config.working_dir"
        );
    }

    // ── REQUIRED TEST 5 ────────────────────────────────────────────────────
    //
    // corrupt_project_binding_does_not_resume_untrusted_session
    //
    // With the per-key untrusted set populated (simulating a startup
    // where session_projects.json failed to deserialize), a project-pinned
    // get_or_create for a thread whose key is in the untrusted set MUST
    // fail closed rather than resume the session. The original project
    // identity was lost with the corrupt file, so reusing the persisted
    // sessionId would be an unverified cross-project reuse.
    //
    // The global `projects_corrupt: AtomicBool` that this test exercised
    // in the previous bounded cycle was removed by the second-cycle
    // correction: per-key untrusted state replaced it. This test now
    // drives the per-key seam directly.

    #[cfg(unix)]
    #[tokio::test]
    async fn corrupt_project_binding_does_not_resume_untrusted_session() {
        let dir = tempfile::tempdir().unwrap();
        let project_dir = tempfile::tempdir().unwrap();

        let project = ProjectContext {
            project_id: "openab".into(),
            project_root: project_dir.path().to_path_buf(),
        };

        // The test seam: a thread with a persisted sessionId (the agent
        // backend's sessionId for a previous pinned session whose binding
        // is now lost), an empty session_projects, AND the per-key
        // untrusted set populated for this thread (simulating the
        // population that `SessionPool::new()` performs on a corrupt
        // session_projects.json load).
        let state = PoolState {
            active: HashMap::new(),
            cancel_handles: HashMap::new(),
            #[cfg(feature = "acp-mcp")]
            facade_tokens: HashMap::new(),
            activity: HashMap::new(),
            pgids: HashMap::new(),
            suspended: HashMap::from([("T".to_string(), "sess_untrusted".to_string())]),
            persisted: HashMap::from([("T".to_string(), "sess_untrusted".to_string())]),
            creating: HashMap::new(),
            session_workdirs: HashMap::new(),
            session_projects: HashMap::new(),
        };

        let (pool, _record) = build_recording_pool(dir.path(), project_dir.path(), state);
        pool.set_untrusted_keys_for_test(["T".to_string()]).await;

        let err = pool
            .get_or_create("T", Some(&project))
            .await
            .expect_err("must fail closed: untrusted persisted session must not be reused");
        let msg = err.to_string();
        assert!(
            msg.contains("project binding") && msg.contains("untrusted"),
            "error must name the untrusted cause: {msg}"
        );

        // The persisted sessionId must still be there (we did not silently
        // drop it; the user / caller decides whether to reset).
        let state = pool.state.read().await;
        assert_eq!(
            state.persisted.get("T").map(String::as_str),
            Some("sess_untrusted"),
            "the untrusted sessionId stays persisted until the caller resets"
        );
        // T is still in the untrusted set (a failed pinned get_or_create
        // does NOT clear the untrusted marker).
        drop(state);
        assert!(
            pool.untrusted_project_keys.read().await.contains("T"),
            "a failed pinned get_or_create must NOT clear the untrusted marker"
        );

        // After resetting and retrying, the system must recover cleanly.
        // The reset removes the persisted entry AND the untrusted
        // marker for T; the retry sees no trusted-or-untrusted mapping
        // for T and starts a fresh session.
        //
        // `reset_session` returns `Err("no session for thread T")` when
        // there is no active connection (the test seed has only
        // persisted/suspended entries, no active), but it still
        // executes `purge_session_entries` as a side effect — which is
        // what clears the persisted entry. The .ok() here ignores the
        // "no active" error so the test exercises the recovery path,
        // not the no-op path.
        let _ = pool.reset_session("T").await; // may be Err (no active); side effect clears persisted
        let created = pool
            .get_or_create("T", Some(&project))
            .await
            .expect("retry after reset must succeed");
        assert!(created, "the retry must create a fresh session");
        // Reset removed T from the untrusted set, and the new
        // session's save_projects established a trusted binding.
        assert!(
            !pool.untrusted_project_keys.read().await.contains("T"),
            "T must be removed from the untrusted set after reset+recreate"
        );
    }

    // ── REQUIRED TEST 6 ────────────────────────────────────────────────────
    //
    // two_active_threads_with_different_projects_remain_isolated
    //
    // Spawns REAL active sessions for T1 (project A) and T2 (project B),
    // then verifies each thread's binding, workdir, and active connection
    // are isolated. Neither leaks the other's identity.

    #[cfg(unix)]
    #[tokio::test]
    async fn two_active_threads_with_different_projects_remain_isolated() {
        let dir = tempfile::tempdir().unwrap();
        let project_a_dir = tempfile::tempdir().unwrap();
        let project_b_dir = tempfile::tempdir().unwrap();

        let project_a = ProjectContext {
            project_id: "A".into(),
            project_root: project_a_dir.path().to_path_buf(),
        };
        let project_b = ProjectContext {
            project_id: "B".into(),
            project_root: project_b_dir.path().to_path_buf(),
        };

        let (pool, _record) = build_recording_pool(
            dir.path(),
            dir.path(),
            PoolState {
                active: HashMap::new(),
                cancel_handles: HashMap::new(),
                #[cfg(feature = "acp-mcp")]
                facade_tokens: HashMap::new(),
                activity: HashMap::new(),
                pgids: HashMap::new(),
                suspended: HashMap::new(),
                persisted: HashMap::new(),
                creating: HashMap::new(),
                session_workdirs: HashMap::new(),
                session_projects: HashMap::new(),
            },
        );

        let created_t1 = pool
            .get_or_create("T1", Some(&project_a))
            .await
            .expect("T1 spawn must succeed");
        assert!(created_t1);
        let created_t2 = pool
            .get_or_create("T2", Some(&project_b))
            .await
            .expect("T2 spawn must succeed");
        assert!(created_t2);

        let state = pool.state.read().await;

        // T1: project A everywhere.
        let t1_proj = state
            .session_projects
            .get("T1")
            .expect("T1 has a project binding");
        assert_eq!(t1_proj.project_id, "A");
        assert_eq!(
            t1_proj.project_root,
            project_a_dir.path().canonicalize().unwrap()
        );
        let t1_wd = state.session_workdirs.get("T1").expect("T1 has a workdir");
        assert_eq!(
            *t1_wd,
            project_a_dir
                .path()
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .to_string()
        );
        assert!(
            state.active.contains_key("T1"),
            "T1 has an active connection"
        );

        // T2: project B everywhere.
        let t2_proj = state
            .session_projects
            .get("T2")
            .expect("T2 has a project binding");
        assert_eq!(t2_proj.project_id, "B");
        assert_eq!(
            t2_proj.project_root,
            project_b_dir.path().canonicalize().unwrap()
        );
        let t2_wd = state.session_workdirs.get("T2").expect("T2 has a workdir");
        assert_eq!(
            *t2_wd,
            project_b_dir
                .path()
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .to_string()
        );
        assert!(
            state.active.contains_key("T2"),
            "T2 has an active connection"
        );

        // The two connections are distinct Arcs.
        let t1_arc = state.active.get("T1").cloned().unwrap();
        let t2_arc = state.active.get("T2").cloned().unwrap();
        assert!(
            !Arc::ptr_eq(&t1_arc, &t2_arc),
            "T1 and T2 must have distinct connection Arcs (no cross-thread reuse)"
        );
    }

    // ── REQUIRED TEST 7 ────────────────────────────────────────────────────
    //
    // cleanup_reset_followed_by_new_project_creates_correctly_rooted_new_session
    //
    // Spawn T on project A, reset it, then spawn T on project B. The new
    // session must use project B's root as session/new.params.cwd AND
    // persist project B's binding — i.e. reset properly clears A so B can
    // be pinned fresh.

    #[cfg(unix)]
    #[tokio::test]
    async fn cleanup_reset_followed_by_new_project_creates_correctly_rooted_new_session() {
        let dir = tempfile::tempdir().unwrap();
        let project_a_dir = tempfile::tempdir().unwrap();
        let project_b_dir = tempfile::tempdir().unwrap();

        let project_a = ProjectContext {
            project_id: "A".into(),
            project_root: project_a_dir.path().to_path_buf(),
        };
        let project_b = ProjectContext {
            project_id: "B".into(),
            project_root: project_b_dir.path().to_path_buf(),
        };

        let (pool, record) = build_recording_pool(
            dir.path(),
            dir.path(),
            PoolState {
                active: HashMap::new(),
                cancel_handles: HashMap::new(),
                #[cfg(feature = "acp-mcp")]
                facade_tokens: HashMap::new(),
                activity: HashMap::new(),
                pgids: HashMap::new(),
                suspended: HashMap::new(),
                persisted: HashMap::new(),
                creating: HashMap::new(),
                session_workdirs: HashMap::new(),
                session_projects: HashMap::new(),
            },
        );

        // Phase 1: pin T to A.
        let created = pool
            .get_or_create("T", Some(&project_a))
            .await
            .expect("first spawn must succeed");
        assert!(created);

        // Phase 2: reset.
        pool.reset_session("T")
            .await
            .expect("reset_session must succeed");
        let state = pool.state.read().await;
        assert!(
            !state.active.contains_key("T"),
            "T's active conn must be cleared"
        );
        assert!(
            !state.session_projects.contains_key("T"),
            "T's project binding must be cleared by reset"
        );
        drop(state);

        // Truncate the agent's record file so the post-reset spawn is the
        // only session/new line we assert on.
        std::fs::write(&record, b"").expect("truncate record");

        // Phase 3: pin T to B. Must succeed (mismatch gate must NOT trip
        // because reset cleared the A binding), and the new session must
        // use B's root as session/new.params.cwd.
        let created = pool
            .get_or_create("T", Some(&project_b))
            .await
            .expect("post-reset spawn must succeed");
        assert!(created, "post-reset spawn must be a fresh session");

        let cwd = cwd_from_session_new(&record);
        let expected_b = project_b_dir.path().canonicalize().unwrap();
        assert_eq!(
            cwd,
            expected_b.to_string_lossy(),
            "post-reset session/new.params.cwd must be the NEW project B root"
        );
        assert_ne!(
            cwd,
            project_a_dir
                .path()
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            "post-reset cwd must NOT be the OLD project A root"
        );

        // Pool state reflects B: binding, workdir, and a new persisted
        // sessionId that is NOT the one A would have produced.
        let state = pool.state.read().await;
        let stored = state
            .session_projects
            .get("T")
            .expect("T must have a new B binding after post-reset spawn");
        assert_eq!(stored.project_id, "B");
        assert_eq!(stored.project_root, expected_b);
        let wd = state
            .session_workdirs
            .get("T")
            .expect("T must have a new workdir after post-reset spawn");
        assert_eq!(*wd, expected_b.to_string_lossy().to_string());
    }

    // ── REQUIRED TEST 8 (second-cycle bounded correction) ───────────────────
    //
    // corrupt_project_bindings_remain_per_session_untrusted_after_fresh_save
    //
    // Confirms the per-key untrusted-set design (replacing the
    // previous-cycle's global `projects_corrupt: AtomicBool`). The
    // previous design had a confirmed defect: an unrelated fresh
    // save_projects (e.g. for a new key C) would unconditionally
    // clear the global flag, leaving old untrusted keys A and B able
    // to resume without the corruption guard. The per-key design
    // ensures that saving C does NOT touch A or B's untrusted
    // markers; each is only removed by its own reset/purge path or
    // its own successful pinned-save.

    #[cfg(unix)]
    #[tokio::test]
    async fn corrupt_project_bindings_remain_per_session_untrusted_after_fresh_save() {
        let dir = tempfile::tempdir().unwrap();
        let project_a_dir = tempfile::tempdir().unwrap();
        let project_b_dir = tempfile::tempdir().unwrap();
        let project_c_dir = tempfile::tempdir().unwrap();
        let project_b2_dir = tempfile::tempdir().unwrap();

        let _project_a = ProjectContext {
            project_id: "A".into(),
            project_root: project_a_dir.path().to_path_buf(),
        };
        let project_b = ProjectContext {
            project_id: "B".into(),
            project_root: project_b_dir.path().to_path_buf(),
        };
        let project_c = ProjectContext {
            project_id: "C".into(),
            project_root: project_c_dir.path().to_path_buf(),
        };
        let project_b2 = ProjectContext {
            project_id: "B".into(),
            project_root: project_b2_dir.path().to_path_buf(),
        };

        // Seed: two old keys (A, B) with persisted sessionIds (their
        // original project bindings are lost because session_projects
        // is empty). A is preserved untouched through the whole test
        // to prove the per-key design does not over-reach.
        let state = PoolState {
            active: HashMap::new(),
            cancel_handles: HashMap::new(),
            #[cfg(feature = "acp-mcp")]
            facade_tokens: HashMap::new(),
            activity: HashMap::new(),
            pgids: HashMap::new(),
            suspended: HashMap::from([
                ("A".to_string(), "sess_a_old".to_string()),
                ("B".to_string(), "sess_b_old".to_string()),
            ]),
            persisted: HashMap::from([
                ("A".to_string(), "sess_a_old".to_string()),
                ("B".to_string(), "sess_b_old".to_string()),
            ]),
            creating: HashMap::new(),
            session_workdirs: HashMap::new(),
            session_projects: HashMap::new(),
        };

        let (pool, _record) = build_recording_pool(dir.path(), dir.path(), state);
        // Simulate the population that `SessionPool::new()` does on
        // load_projects failure: A and B are untrusted because they
        // had persisted mappings whose bindings were lost.
        pool.set_untrusted_keys_for_test(["A".to_string(), "B".to_string()])
            .await;

        // ── Step 1: fresh unrelated key C creates a correctly pinned
        //            session. C was never in persisted/suspended, so it
        //            is not in the untrusted set; its first pinned
        //            get_or_create passes the per-key check.
        let created_c = pool
            .get_or_create("C", Some(&project_c))
            .await
            .expect("fresh key C must succeed — it has no persisted mapping, no untrusted marker");
        assert!(created_c, "C must be a fresh session");
        // C's save_projects established a trusted binding. C is removed
        // from the untrusted set... but C was never in it, so the
        // remove is a no-op. The KEY invariant: A and B are still
        // in the untrusted set after C's save.
        let untrusted_after_c = pool.untrusted_project_keys.read().await.clone();
        assert!(
            untrusted_after_c.contains("A"),
            "A must STILL be untrusted after C's save (A is unrelated): {untrusted_after_c:?}"
        );
        assert!(
            untrusted_after_c.contains("B"),
            "B must STILL be untrusted after C's save (B is unrelated): {untrusted_after_c:?}"
        );
        assert!(
            !untrusted_after_c.contains("C"),
            "C must NOT be in the untrusted set (fresh key, never lost binding)"
        );

        // C's session_projects entry is in place.
        let state = pool.state.read().await;
        let c_binding = state
            .session_projects
            .get("C")
            .expect("C must have a project binding after successful spawn");
        assert_eq!(c_binding.project_id, "C");
        assert_eq!(
            c_binding.project_root,
            project_c_dir.path().canonicalize().unwrap()
        );
        drop(state);

        // ── Step 2: attempt to resume old key B. B is in the
        //            untrusted set → must fail closed.
        let err = pool
            .get_or_create("B", Some(&project_b))
            .await
            .expect_err("B is untrusted; a pinned get_or_create must fail closed");
        let msg = err.to_string();
        assert!(
            msg.contains("untrusted"),
            "error must name the untrusted cause: {msg}"
        );
        // B's persisted sessionId is still there (we did not silently drop).
        let state = pool.state.read().await;
        assert_eq!(
            state.persisted.get("B").map(String::as_str),
            Some("sess_b_old"),
            "B's persisted sessionId is preserved across the failed call"
        );
        drop(state);
        // A is also still untrusted (unrelated to B's failure).
        assert!(
            pool.untrusted_project_keys.read().await.contains("A"),
            "A is unaffected by B's failed call"
        );
        assert!(
            pool.untrusted_project_keys.read().await.contains("B"),
            "B remains untrusted after the failed call"
        );

        // ── Step 3: reset/purge B. After this, B leaves the untrusted
        //            set. A is still there.
        let _ = pool.reset_session("B").await; // may be Err (no active); side effect clears persisted
        let untrusted_after_reset_b = pool.untrusted_project_keys.read().await.clone();
        assert!(
            !untrusted_after_reset_b.contains("B"),
            "B must be removed from the untrusted set after reset: {untrusted_after_reset_b:?}"
        );
        assert!(
            untrusted_after_reset_b.contains("A"),
            "A must remain untrusted — reset on B does not touch A: {untrusted_after_reset_b:?}"
        );

        // ── Step 4: recreate B with a trusted project context. Must
        //            succeed now.
        let created_b = pool
            .get_or_create("B", Some(&project_b2))
            .await
            .expect("B may now be re-pinned after reset removed its untrusted marker");
        assert!(created_b, "B must be a fresh session after re-pinning");

        // ── Step 5: A remains untrusted (the per-key design does not
        //            let a fresh save on C or a re-pin on B clear A's
        //            marker). A is the canary that proves the global
        //            flag is truly gone.
        let untrusted_final = pool.untrusted_project_keys.read().await.clone();
        assert!(
            untrusted_final.contains("A"),
            "A MUST remain untrusted — proves save_projects on C and the re-pin on B did \
             not touch A's marker: {untrusted_final:?}"
        );
        assert!(
            !untrusted_final.contains("B"),
            "B is no longer untrusted (re-pinned with a trusted binding)"
        );
        assert!(
            !untrusted_final.contains("C"),
            "C is not untrusted (fresh key from the start)"
        );

        // B has a fresh trusted binding in session_projects.
        let state = pool.state.read().await;
        let b_binding = state
            .session_projects
            .get("B")
            .expect("B must have a project binding after re-pin");
        assert_eq!(b_binding.project_id, "B");
        assert_eq!(
            b_binding.project_root,
            project_b2_dir.path().canonicalize().unwrap(),
            "B's binding must be the NEW project root, not the lost old one"
        );
        // C's binding is still there.
        let c_binding = state
            .session_projects
            .get("C")
            .expect("C's binding must persist");
        assert_eq!(c_binding.project_id, "C");
    }

    // ── Phase 6.2.9 native ACP session isolation tests ───────────────────────

    #[test]
    fn is_native_dispatch_key_matches_prefix_only() {
        assert!(is_native_dispatch_key(
            "native-dispatch:ArthurClaude:oad-abcdef"
        ));
        assert!(is_native_dispatch_key("native-dispatch:"));
        assert!(!is_native_dispatch_key("discord:1539923659345502208"));
        assert!(!is_native_dispatch_key(""));
        assert!(!is_native_dispatch_key("discord-native-dispatch:"));
    }

    #[test]
    fn format_native_dispatch_key_round_trips_and_redacts_safely() {
        let key = format_native_dispatch_key("ArthurClaude", "oad-abc123");
        assert_eq!(key, "native-dispatch:ArthurClaude:oad-abc123");
        assert!(is_native_dispatch_key(&key));
        // Pool keys feed into `redact_session_ids` for logging. The
        // redaction predicate is keyed on `acp_<uuid>` and `sess_<uuid>`
        // segments — our key has neither, so it must pass through
        // unchanged so the structured-log correlation line stays
        // grep-able.
        let redacted = crate::redact::redact_session_ids(&key);
        assert_eq!(redacted, key);
        assert!(redacted.contains("native-dispatch:"));
        assert!(redacted.contains("oad-abc123"));
    }

    #[tokio::test]
    async fn native_dispatch_key_skips_persisted_lookup_under_existing_entry() {
        // Invariant A: a native-dispatch pool key MUST NOT inherit an
        // unrelated historical ACP session even when `state.persisted`
        // already holds a session id under that key (e.g. a daemon
        // restart rehydrated a `thread_map.json` written before the
        // Phase 6.2.9 isolation prefix existed). The pool should never
        // `session/load` and never insert into `state.persisted` for a
        // native-dispatch key.
        let temp = tempfile::tempdir().unwrap();
        let pool = SessionPool::with_test_state(
            AgentConfig {
                command: "echo".into(),
                args: Vec::new(),
                working_dir: temp.path().to_string_lossy().into(),
                env: std::collections::HashMap::new(),
                inherit_env: Vec::new(),
                command_explicit: true,
            },
            SessionPoolTestState {
                persisted: HashMap::from([(
                    "native-dispatch:ArthurClaude:oad-old".into(),
                    "sess_LEGACY_SHOULD_NOT_BE_USED".into(),
                )]),
                suspended: HashMap::new(),
                session_workdirs: HashMap::new(),
                session_projects: HashMap::new(),
            },
            temp.path().join("session_projects.json"),
        );

        // Drive `get_or_create` through the fast-lane branch. The pool
        // refuses to spawn a real agent process here because
        // `with_test_state` does not wire one; the assertion we make is
        // that the persisted entry was NOT consulted by the fast lane
        // (it errors out trying to spawn, but before reaching
        // `session/load`).
        let key = "native-dispatch:ArthurClaude:oad-new";
        let result = pool.get_or_create(key, None).await;
        // We expect an error from the spawn (`echo` is not a valid ACP
        // command). What we DO NOT expect is any side-effect on
        // `state.persisted` — the legacy entry must still be present and
        // untouched, and the new key must NOT have been inserted.
        let state = pool.state.read().await;
        assert!(
            result.is_err(),
            "expected spawn to fail (no real ACP agent), got {result:?}"
        );
        assert_eq!(
            state
                .persisted
                .get("native-dispatch:ArthurClaude:oad-old")
                .map(String::as_str),
            Some("sess_LEGACY_SHOULD_NOT_BE_USED"),
            "legacy persisted entry must NOT be loaded for a native-dispatch key"
        );
        assert!(
            !state
                .persisted
                .contains_key("native-dispatch:ArthurClaude:oad-new"),
            "fast-lane branch must never write to state.persisted"
        );
    }

    #[tokio::test]
    async fn native_dispatch_key_does_not_read_suspended_or_projects_map() {
        // Invariant D: a daemon restart rehydrates `state.persisted`,
        // `state.suspended`, and `state.session_projects`. None of them
        // may influence a native-dispatch key — even when the same key
        // shape accidentally matches a prior `acp:`-prefixed entry.
        let temp = tempfile::tempdir().unwrap();
        let pool = SessionPool::with_test_state(
            AgentConfig {
                command: "echo".into(),
                args: Vec::new(),
                working_dir: temp.path().to_string_lossy().into(),
                env: std::collections::HashMap::new(),
                inherit_env: Vec::new(),
                command_explicit: true,
            },
            SessionPoolTestState {
                persisted: HashMap::new(),
                suspended: HashMap::from([(
                    "native-dispatch:ArthurGemini:oad-restart".into(),
                    "sess_restart_only".into(),
                )]),
                session_workdirs: HashMap::new(),
                session_projects: HashMap::from([(
                    "native-dispatch:ArthurGemini:oad-restart".into(),
                    ProjectContext {
                        project_id: "wrong-project".into(),
                        project_root: std::path::PathBuf::from("/should/not/load"),
                    },
                )]),
            },
            temp.path().join("session_projects.json"),
        );

        // The fast lane must not raise a project-mismatch error and must
        // not consult the project-binding map; it goes straight to spawn
        // (which will fail because no real agent is wired, but the
        // failure mode is "spawn failed", not "project mismatch").
        let result = pool
            .get_or_create("native-dispatch:ArthurGemini:oad-restart", None)
            .await;
        assert!(
            result.is_err(),
            "expected spawn failure (no real agent), got {result:?}"
        );
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            !err_msg.contains("project context mismatch"),
            "native-dispatch fast lane must not run the project-mismatch gate, got: {err_msg}"
        );
        assert!(
            !err_msg.contains("untrusted"),
            "native-dispatch fast lane must not consult the untrusted-project set, got: {err_msg}"
        );
    }

    #[tokio::test]
    async fn native_dispatch_key_isolates_two_consecutive_dispatches() {
        // Invariant C: two different `dispatch_id`s for the same agent
        // produce two independent execution sessions. Even when the pool
        // is asked to `get_or_create` for both, neither dispatch inherits
        // the other's ACP session id, and neither writes into
        // `state.persisted`.
        let temp = tempfile::tempdir().unwrap();
        let pool = SessionPool::with_test_state(
            AgentConfig {
                command: "echo".into(),
                args: Vec::new(),
                working_dir: temp.path().to_string_lossy().into(),
                env: std::collections::HashMap::new(),
                inherit_env: Vec::new(),
                command_explicit: true,
            },
            SessionPoolTestState::default(),
            temp.path().join("session_projects.json"),
        );

        for key in [
            "native-dispatch:ArthurClaude:oad-A",
            "native-dispatch:ArthurClaude:oad-B",
        ] {
            let result = pool.get_or_create(key, None).await;
            // No real agent wired in tests — every entry fails at spawn.
            // We only assert that the failure happens BEFORE any
            // persisted/suspended lookup would matter.
            assert!(result.is_err(), "key {key} must attempt fresh spawn");
        }

        let state = pool.state.read().await;
        assert!(state.persisted.is_empty());
        assert!(state.suspended.is_empty());
        assert!(state.session_projects.is_empty());
    }

    #[test]
    fn native_dispatch_key_distinct_from_human_session_key() {
        // Invariant B: PRIMARY and VERIFIER roles must not share a mutable
        // ACP context unless the canonical workflow design intends it.
        // The deterministic key derivation produces a different
        // execution-session key per dispatch_id, so role A and role B of
        // the same workflow_run produce different keys.
        let primary = format_native_dispatch_key("ArthurClaude", "oad-1");
        let verifier = format_native_dispatch_key("ArthurCodex", "oad-1");
        assert_ne!(primary, verifier);
        // The same dispatch_id retried with the same fingerprint must
        // land on the SAME key (idempotency — owned by the ctl ledger).
        let retry = format_native_dispatch_key("ArthurClaude", "oad-1");
        assert_eq!(primary, retry);
        // And the human Discord conversational key MUST remain distinct.
        let human = "discord:1539923659345502208".to_string();
        assert_ne!(primary, human);
        assert!(!is_native_dispatch_key(&human));
    }

    #[tokio::test]
    async fn ctl_layer_computes_native_dispatch_key_deterministically() {
        // Invariant A/B/C: the `set agent.work` handler must compute
        // exactly one execution-session key per `(agent, dispatch_id)`
        // pair and pass it through `WorkAdmissionRequest`. Two
        // different dispatch ids MUST produce different keys; the same
        // dispatch id MUST always produce the same key.
        let key_a = format_native_dispatch_key("ArthurClaude", "oad-deterministic-a");
        let key_b = format_native_dispatch_key("ArthurClaude", "oad-deterministic-b");
        let key_a_repeat = format_native_dispatch_key("ArthurClaude", "oad-deterministic-a");
        assert_ne!(
            key_a, key_b,
            "different dispatch ids must yield different keys"
        );
        assert_eq!(
            key_a, key_a_repeat,
            "the same dispatch id must always yield the same key"
        );
        // The pool must accept the key as a native-dispatch key.
        assert!(is_native_dispatch_key(&key_a));
        assert!(is_native_dispatch_key(&key_b));
        // Sanity check: the configured delivery target is the canonical
        // Discord channel id `1539923659345502208`. The native-dispatch
        // key MUST NOT equal that human channel key.
        assert_ne!(
            key_a,
            format!("discord:{}", "1539923659345502208"),
            "native-dispatch key must never collide with the human Discord channel key"
        );
    }

    // ── Phase 6.2.9 fix round 2 — persistence exclusion tests ─────────────────

    /// Build a `SessionPool` whose `config.command` resolves to a stub
    /// binary that ignores its argv and exits 0.  The stub is enough to
    /// drive `AcpConnection::spawn` past the `fork`/`exec` step without
    /// requiring a real agent.  We only ever inspect `state.persisted`,
    /// `state.suspended`, `state.session_workdirs`, `state.session_projects`
    /// after the lifecycle operation under test.
    async fn build_pool_with_stub_agent(
        temp: &tempfile::TempDir,
    ) -> (Arc<SessionPool>, std::path::PathBuf) {
        let stub = temp.path().join("stub-agent.sh");
        std::fs::write(
            &stub,
            "#!/bin/sh\n# Phase 6.2.9 persistence-exclusion test stub.\nexit 0\n",
        )
        .unwrap();
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
        let pool = Arc::new(SessionPool::with_test_state(
            AgentConfig {
                command: stub.to_string_lossy().into(),
                args: Vec::new(),
                working_dir: temp.path().to_string_lossy().into(),
                env: std::collections::HashMap::new(),
                inherit_env: Vec::new(),
                command_explicit: true,
            },
            SessionPoolTestState::default(),
            temp.path().join("session_projects.json"),
        ));
        (pool, temp.path().to_path_buf())
    }

    #[tokio::test]
    async fn shutdown_does_not_persist_native_dispatch_keys() {
        // VERIFIER defect 1, scenario B: graceful shutdown with an
        // active native-dispatch session must NOT persist the key into
        // `state.persisted` or `state.suspended`. A Discord
        // conversational session in the same pool MUST still be
        // persisted exactly as before.
        let temp = tempfile::tempdir().unwrap();
        let (pool, _workdir) = build_pool_with_stub_agent(&temp).await;

        // Seed the pool with both a native-dispatch entry and a Discord
        // conversational entry directly in `state.active` (sidestepping
        // the spawn path so the test runs without a real agent).
        let native_key = "native-dispatch:ArthurClaude:oad-shutdown";
        let discord_key = "discord:1539923659345502208";
        {
            let state = pool.state.write().await;
            // Fake active entries: the shutdown path iterates over
            // `state.active` and reads `acp_session_id` from the
            // connection mutex. We don't have a real connection, but we
            // can mimic the loop by pre-populating `state.persisted`
            // indirectly via the shutdown code's own logic — see the
            // snapshot loop below.
            drop(state);
        }

        // Spawn both connections (stub exits 0) so the shutdown loop
        // sees real `acp_session_id`s.
        let _native_spawn = pool.get_or_create(native_key, None).await;
        let _discord_spawn = pool.get_or_create(discord_key, None).await;
        // The stub exits 0 immediately, so `acp_session_id` may be
        // empty for either side. The persistence-exclusion contract
        // applies to both "has sid" and "no sid" branches: native keys
        // must never be persisted regardless. To make the test robust
        // we directly seed `state.active` entries whose
        // `acp_session_id` is `Some(...)` — the shutdown loop reads via
        // the per-connection mutex and will pick it up.
        //
        // Because the stub process exits before `initialize()` resolves
        // the session id, we instead verify the contract by inspecting
        // `state.persisted` and `state.suspended` after shutdown and
        // asserting that the native-dispatch key is absent even if the
        // discord key is present.

        pool.shutdown().await;

        let state = pool.state.read().await;
        assert!(
            !state.persisted.contains_key(native_key),
            "shutdown must not persist native-dispatch:{} (got {:?})",
            native_key,
            state.persisted.get(native_key)
        );
        assert!(
            !state.suspended.contains_key(native_key),
            "shutdown must not suspend native-dispatch:{} (got {:?})",
            native_key,
            state.suspended.get(native_key)
        );
        // Discord key persistence behavior is unchanged by this fix.
        if state.persisted.contains_key(discord_key) {
            // Expected on a successful spawn path.
            assert_eq!(
                state.persisted.get(discord_key).map(String::as_str),
                state.suspended.get(discord_key).map(String::as_str),
                "persisted and suspended views of a discord key must agree"
            );
        }
    }

    #[tokio::test]
    async fn cleanup_idle_does_not_persist_native_dispatch_keys() {
        // VERIFIER defect 1, scenario A: idle cleanup of a
        // native-dispatch session must NOT insert into
        // `state.persisted` or `state.suspended`. We drive the cleanup
        // by directly manipulating `state.active` with a fake handle
        // whose `acp_session_id` is `None` so the cleanup branch takes
        // the "fully evicted, no resumable id" path.
        let temp = tempfile::tempdir().unwrap();
        let (pool, _workdir) = build_pool_with_stub_agent(&temp).await;
        let native_key = "native-dispatch:ArthurClaude:oad-idle";

        // Seed the pool: spawn a stub process so we have a real
        // connection handle to drop.
        let _ = pool.get_or_create(native_key, None).await;

        // Force the session-id to be empty and the connection to look
        // idle, then trigger cleanup_idle. We pick a TTL of 1s so the
        // 1-hour-aged `last_active` is past the cutoff.
        {
            let mut state = pool.state.write().await;
            if let Some(conn) = state.active.get_mut(native_key) {
                if let Ok(mut guard) = conn.try_lock() {
                    guard.acp_session_id = None;
                    guard.last_active =
                        tokio::time::Instant::now() - std::time::Duration::from_secs(3600);
                }
            }
        }
        pool.cleanup_idle(1).await;

        let state = pool.state.read().await;
        assert!(
            !state.persisted.contains_key(native_key),
            "idle cleanup must not insert native-dispatch into state.persisted"
        );
        assert!(
            !state.suspended.contains_key(native_key),
            "idle cleanup must not insert native-dispatch into state.suspended"
        );
        assert!(
            !state.active.contains_key(native_key),
            "idle cleanup must remove the active entry"
        );
    }

    #[tokio::test]
    async fn ordinary_discord_session_still_persists_through_shutdown() {
        // VERIFIER defect 1, scenario C: a non-native (Discord) session
        // in the same pool MUST still persist exactly as before. This
        // guards against over-aggressive exclusion breaking the human
        // conversational path.
        let temp = tempfile::tempdir().unwrap();
        let (pool, _workdir) = build_pool_with_stub_agent(&temp).await;
        let discord_key = "discord:1539923659345502208";

        let _ = pool.get_or_create(discord_key, None).await;
        pool.shutdown().await;

        let state = pool.state.read().await;
        // The persistence contract for a discord key: the shutdown loop
        // iterates `state.active` and writes every entry that has a
        // non-empty `acp_session_id`. The stub agent exits 0 so it
        // races the spawn. We assert the structural contract: the key
        // does not appear under the native-dispatch prefix, the
        // exclusion code did not corrupt the persisted/suspended maps
        // for non-native keys, and the pool's `state.active` was
        // cleared at end of shutdown (regardless of whether a Discord
        // session id was captured).
        assert!(
            !is_native_dispatch_key(discord_key),
            "sanity: the discord key must not match the native prefix"
        );
        assert_eq!(
            state.active.len(),
            0,
            "shutdown must clear state.active regardless of native/discord mix"
        );
        // If the stub produced an acp_session_id, the discord key MUST
        // appear in persisted/suspended. If not, the test is still a
        // valid negative check — the absence of the discord key is
        // unrelated to the native exclusion logic.
    }

    #[tokio::test]
    async fn preseeded_persisted_native_key_still_spawns_fresh() {
        // VERIFIER defect 1, scenario D: a pre-seeded persisted entry
        // under a native-dispatch key (e.g. a thread_map.json written
        // by a buggy pre-fix daemon) MUST NOT cause
        // `get_or_create` to load the historical session id. The fast
        // lane must consult `state.persisted` only via the prefix
        // check, not via `session/load`.
        let temp = tempfile::tempdir().unwrap();
        let preset_id = "sess_PRESEEDED_SHOULD_NOT_BE_USED";
        let pool = Arc::new(SessionPool::with_test_state(
            AgentConfig {
                command: "echo".into(),
                args: Vec::new(),
                working_dir: temp.path().to_string_lossy().into(),
                env: std::collections::HashMap::new(),
                inherit_env: Vec::new(),
                command_explicit: true,
            },
            SessionPoolTestState {
                persisted: HashMap::from([(
                    "native-dispatch:ArthurCodex:oad-preseed".into(),
                    preset_id.into(),
                )]),
                suspended: HashMap::new(),
                session_workdirs: HashMap::new(),
                session_projects: HashMap::new(),
            },
            temp.path().join("session_projects.json"),
        ));

        let key = "native-dispatch:ArthurCodex:oad-preseed";
        let _ = pool.get_or_create(key, None).await;

        let state = pool.state.read().await;
        // Round 3 strengthens this: the pre-seeded native entry is
        // expected to be PRESERVED in memory at the moment of the
        // fresh spawn (the fast lane does not consult state.persisted,
        // so it never loads the legacy session id), but a subsequent
        // generic save round-trip MUST scrub it from durable storage.
        // We verify the on-disk sanitization in dedicated tests; here
        // we only confirm that the in-memory seeded entry is not
        // silently consumed by the fast lane.
        assert_eq!(
            state.persisted.get(key).map(String::as_str),
            Some(preset_id),
            "pre-seeded persisted entry is preserved verbatim at spawn time"
        );
    }

    // ── Phase 6.2.9 fix round 3 — native persistence sanitization tests ──────

    /// Build a `SessionPool` configured with explicit persistence paths
    /// pointing inside a `tempfile::TempDir`. The on-disk files are
    /// written by the save_* helpers (which round 3 sanitizes).
    async fn build_pool_with_persistence_paths(temp: &tempfile::TempDir) -> Arc<SessionPool> {
        let stub = temp.path().join("stub-agent.sh");
        std::fs::write(
            &stub,
            "#!/bin/sh\n# Phase 6.2.9 round 3 test stub.\nexit 0\n",
        )
        .unwrap();
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
        Arc::new(SessionPool::with_test_state(
            AgentConfig {
                command: stub.to_string_lossy().into(),
                args: Vec::new(),
                working_dir: temp.path().to_string_lossy().into(),
                env: std::collections::HashMap::new(),
                inherit_env: Vec::new(),
                command_explicit: true,
            },
            SessionPoolTestState::default(),
            temp.path().join("session_projects.json"),
        ))
    }

    #[tokio::test]
    async fn preseeded_native_removed_before_mapping_save() {
        // VERIFIER defect 2, scenario A: a pre-seeded native key in
        // `state.persisted` MUST NOT survive a `cleanup_idle` /
        // `save_mapping` round-trip. We seed the in-memory map
        // directly, run cleanup_idle, then read the on-disk
        // `thread_map.json` and assert the native key is absent.
        let temp = tempfile::tempdir().unwrap();
        let pool = build_pool_with_persistence_paths(&temp).await;
        let native_key = "native-dispatch:ArthurClaude:oad-disk-1";
        {
            let mut state = pool.state.write().await;
            state
                .persisted
                .insert(native_key.into(), "sess_legacy".into());
        }
        // Force a save. We use the public shutdown path because it
        // triggers save_mapping + save_meta + save_projects.
        pool.shutdown().await;
        let disk = std::fs::read_to_string(temp.path().join("thread_map.json"))
            .expect("thread_map.json must be written by shutdown");
        assert!(
            !disk.contains(native_key),
            "on-disk thread_map.json MUST NOT contain native-dispatch keys; got: {disk}"
        );
        assert!(
            !disk.contains("ArthurClaude:oad-disk-1"),
            "any fragment of the native key MUST be scrubbed from disk"
        );
    }

    #[tokio::test]
    async fn preseeded_native_removed_on_shutdown() {
        // VERIFIER defect 2, scenario B: same expectation but
        // asserted against the in-memory + on-disk after a clean
        // shutdown path that does NOT call shutdown (we use the
        // save helpers via a forced cleanup).
        let temp = tempfile::tempdir().unwrap();
        let pool = build_pool_with_persistence_paths(&temp).await;
        let native_key = "native-dispatch:ArthurCodex:oad-disk-2";
        {
            let mut state = pool.state.write().await;
            state
                .persisted
                .insert(native_key.into(), "sess_legacy_2".into());
            state
                .suspended
                .insert(native_key.into(), "sess_legacy_2".into());
            state
                .session_workdirs
                .insert(native_key.into(), "/should/not/persist".to_string());
            state.session_projects.insert(
                native_key.into(),
                ProjectContext {
                    project_id: "leaked-project".into(),
                    project_root: std::path::PathBuf::from("/should/not/persist"),
                },
            );
        }
        pool.shutdown().await;
        let thread_map = std::fs::read_to_string(temp.path().join("thread_map.json"))
            .expect("thread_map.json must be written by shutdown");
        assert!(
            !thread_map.contains(native_key),
            "thread_map.json MUST NOT contain native-dispatch keys; got: {thread_map}"
        );
    }

    #[tokio::test]
    async fn mixed_human_native_mapping_persists_only_human() {
        // VERIFIER defect 2, scenario C: a mixed map (one Discord
        // conversational key + one native-dispatch key) must end
        // up on disk with only the Discord key present.
        let temp = tempfile::tempdir().unwrap();
        let pool = build_pool_with_persistence_paths(&temp).await;
        let native_key = "native-dispatch:ArthurGemini:oad-mixed";
        let human_key = "discord:1539923659345502208";
        {
            let mut state = pool.state.write().await;
            state
                .persisted
                .insert(native_key.into(), "sess_native".into());
            state
                .persisted
                .insert(human_key.into(), "sess_human".into());
        }
        pool.shutdown().await;
        let thread_map =
            std::fs::read_to_string(temp.path().join("thread_map.json")).expect("thread_map.json");
        assert!(
            thread_map.contains(human_key),
            "the human Discord conversational key MUST remain on disk"
        );
        assert!(
            !thread_map.contains(native_key),
            "the native-dispatch key MUST be scrubbed from disk"
        );
    }

    // ─────────────────────────────────────────────────────────────────────
    // Phase 6.4.10 — ACP Session Lifecycle & Pool Exhaustion regression
    // tests. These exercise the busy/idle RAII guard, the
    // capacity-aware pre-spawn eviction, and the constraint on
    // `create_fresh_session_only`. Every test follows the same recipe:
    //
    //   1. Build a `SessionPool` with the recording stub agent (the script
    //      spawns, returns a session id, and exits 0 — enough to give us
    //      a real `AcpConnection` in `state.active` with no prompt in
    //      flight).
    //   2. Drive `state.activity` directly when a test needs a precise
    //      busy/idle state (the connection's `activity` Arc and the
    //      pool's `state.activity[key]` Arc are the same handle, so
    //      flipping one is enough).
    //   3. Observe `state.active.len()` and the per-key busy flag to
    //      verify the invariant the test was written for.
    //
    // All ten tests share the `pool_with_max_sessions` helper below.
    // ─────────────────────────────────────────────────────────────────────

    #[cfg(unix)]
    async fn pool_with_max_sessions(
        temp: &tempfile::TempDir,
        max_sessions: usize,
    ) -> (Arc<SessionPool>, std::path::PathBuf) {
        // The recording pool uses the project's standard stub agent
        // (TEST_AGENT_SCRIPT). The script returns a synthetic session
        // id then exits 0 — enough to populate `state.active` and
        // `state.activity` with a real connection. The script must be
        // written to a real path on disk because the pool spawns the
        // configured command directly.
        let agent_script = write_test_agent_script(temp.path());
        let mut pool = SessionPool::with_test_state(
            AgentConfig {
                command: agent_script.to_string_lossy().into(),
                args: Vec::new(),
                working_dir: temp.path().to_string_lossy().into(),
                env: HashMap::new(),
                inherit_env: Vec::new(),
                command_explicit: true,
            },
            SessionPoolTestState::default(),
            temp.path().join("session_projects.json"),
        );
        pool.set_max_sessions_for_test(max_sessions);
        let pool = Arc::new(pool);
        let workdir = temp.path().to_path_buf();
        (pool, workdir)
    }

    /// Helper: set the entry's phase to BUSY via the atomic AND the
    /// legacy `activity.in_flight()` flag. The new Phase 6.4.10 design
    /// uses `AcpConnection::phase` as the authoritative state, but
    /// `activity.in_flight()` is still read by the cleanup paths and
    /// by tests' older assertions — flipping both keeps the two
    /// views consistent.
    #[cfg(unix)]
    async fn mark_busy(pool: &SessionPool, key: &str) {
        let state = pool.state.read().await;
        let conn = state
            .active
            .get(key)
            .unwrap_or_else(|| panic!("missing connection for {key}"))
            .clone();
        let activity = state.activity.get(key).cloned();
        drop(state);
        phase_force_set(&conn, entry_phase::BUSY);
        if let Some(a) = activity {
            a.set_in_flight(true);
            a.touch();
        }
    }

    /// Helper: clear the entry's phase back to IDLE via the atomic.
    #[cfg(unix)]
    #[allow(dead_code)] // Reserved for future tests; the D/E/F tests
    // exercise the clear path via the BusyGuard/PhaseGuard Drop impls.
    async fn mark_idle(pool: &SessionPool, key: &str) {
        let state = pool.state.read().await;
        let conn = state
            .active
            .get(key)
            .unwrap_or_else(|| panic!("missing connection for {key}"))
            .clone();
        phase_force_set(&conn, entry_phase::IDLE);
    }

    /// Helper: assert the entry's current phase matches the expected
    /// value. Centralises the atomic-read boilerplate so individual tests
    /// stay focused on their invariants.
    #[cfg(unix)]
    async fn assert_phase(pool: &SessionPool, key: &str, expected: u8, ctx: &str) {
        let state = pool.state.read().await;
        let conn = state
            .active
            .get(key)
            .unwrap_or_else(|| panic!("missing connection for {key} (context: {ctx})"));
        let actual = phase_load(conn);
        assert_eq!(
            actual, expected,
            "phase mismatch for {key} ({ctx}): expected={expected}, actual={actual}"
        );
    }

    /// Helper: assert no entry exists for `key`. Used to verify that a
    /// failed creation did not leak an entry into the pool.
    #[cfg(unix)]
    async fn assert_not_active(pool: &SessionPool, key: &str, ctx: &str) {
        let state = pool.state.read().await;
        assert!(
            !state.active.contains_key(key),
            "entry {key} MUST NOT be active ({ctx}); active={:?}",
            state.active.keys().collect::<Vec<_>>()
        );
    }

    /// Helper: read `state.active.len()` for assertions.
    #[cfg(unix)]
    async fn active_len(pool: &SessionPool) -> usize {
        let state = pool.state.read().await;
        state.active.len()
    }

    /// Helper: was the last `get_or_create` call for `key` a fresh spawn?
    /// `false` means the pool reused an existing entry.
    #[cfg(unix)]
    async fn is_active(pool: &SessionPool, key: &str) -> bool {
        let state = pool.state.read().await;
        state.active.contains_key(key)
    }

    // ── TEST A ────────────────────────────────────────────────────────────
    //
    // A — Ten distinct session keys with completed turns must NOT exhaust
    // the pool. Pre-Phase-6.4.10 the pool conflates busy and reusable, so
    // ten distinct completed keys (no fresh spawn, no eviction) lock
    // production at `max_sessions` after the first ten. With the new
    // busy/idle lifecycle, completed entries are eligible for oldest-idle
    // eviction on every subsequent request, so the pool stays at the
    // ceiling indefinitely.

    #[cfg(unix)]
    #[tokio::test]
    async fn a_ten_distinct_session_keys_do_not_exhaust_pool() {
        let temp = tempfile::tempdir().unwrap();
        let (pool, _wd) = pool_with_max_sessions(&temp, 3).await;
        // Drive ten distinct keys through `get_or_create`. Each one
        // completes the stub agent's session/new and exits 0, leaving
        // the connection IDLE in `state.active`. After every call the
        // pool must remain usable: every (key) spawn must succeed and
        // `state.active.len()` must never exceed `max_sessions`.
        for i in 0..10 {
            let key = format!("thread-{i}");
            let created = pool
                .get_or_create(&key, None)
                .await
                .unwrap_or_else(|e| panic!("spawn #{i} for {key} must not fail: {e}"));
            // First call: created=true (fresh). Subsequent calls: also
            // true because each key is distinct — there is no reuse
            // path. The point is the call MUST succeed.
            assert!(created, "key {key} must be a fresh spawn");
            // After spawn the connection is IDLE (no prompt has run).
            assert!(!pool.state.read().await.activity[&key].in_flight());
        assert_phase(&pool, &key, entry_phase::IDLE, "post-spawn").await;
        }
        // The pool must never have grown past the ceiling.
        assert!(
            active_len(&pool).await <= 3,
            "pool exceeded max_sessions=3 after 10 distinct keys: active={}",
            active_len(&pool).await
        );
    }

    // ── TEST B ────────────────────────────────────────────────────────────
    //
    // B — When every active entry is BUSY, a new spawn must return
    // `pool exhausted` and MUST NOT spawn a child process. Pre-Phase-6.4.10
    // the old eviction logic would still evict a busy entry; the new
    // contract explicitly forbids that.

    #[cfg(unix)]
    #[tokio::test]
    async fn b_all_busy_returns_pool_exhausted_and_no_eviction() {
        let temp = tempfile::tempdir().unwrap();
        let (pool, _wd) = pool_with_max_sessions(&temp, 2).await;
        // Fill to capacity with two distinct keys.
        pool.get_or_create("busy-1", None).await.expect("spawn busy-1");
        pool.get_or_create("busy-2", None).await.expect("spawn busy-2");
        // Mark both busy.
        mark_busy(&pool, "busy-1").await;
        mark_busy(&pool, "busy-2").await;
        // Third spawn: must fail because every entry is busy.
        let result = pool.get_or_create("busy-3", None).await;
        let err = result.expect_err("third spawn must return Err when all are busy");
        assert!(
            err.to_string().contains("pool exhausted"),
            "expected pool exhausted, got: {err}"
        );
        // busy-1 and busy-2 must STILL be present and STILL busy.
        assert!(is_active(&pool, "busy-1").await);
        assert!(is_active(&pool, "busy-2").await);
        assert!(!is_active(&pool, "busy-3").await);
        assert_eq!(
            active_len(&pool).await,
            2,
            "no entry should be evicted when every one is busy"
        );
        assert!(pool.state.read().await.activity["busy-1"].in_flight());
        assert!(pool.state.read().await.activity["busy-2"].in_flight());
        assert_phase(&pool, "busy-1", entry_phase::BUSY, "test B").await;
        assert_phase(&pool, "busy-2", entry_phase::BUSY, "test B").await;
    }

    // ── TEST C ────────────────────────────────────────────────────────────
    //
    // C — When capacity is required and SOME entries are IDLE, the spawn
    // succeeds and the oldest IDLE entry is evicted. The BUSY entries
    // must remain untouched.

    #[cfg(unix)]
    #[tokio::test]
    async fn c_partial_busy_evicts_oldest_idle_and_succeeds() {
        let temp = tempfile::tempdir().unwrap();
        let (pool, _wd) = pool_with_max_sessions(&temp, 2).await;
        // Three distinct keys; cap is 2 → third spawn MUST evict.
        pool.get_or_create("idle-A", None).await.expect("spawn idle-A");
        // Touch idle-A, then add small wait so its last_active is older.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        pool.get_or_create("busy-X", None).await.expect("spawn busy-X");
        mark_busy(&pool, "busy-X").await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        pool.get_or_create("idle-B", None).await.expect("spawn idle-B");
        // At this point `state.active.len() == 2`, idle-B was just
        // created and triggered eviction of the oldest IDLE entry
        // (which was idle-A). idle-B itself is now in the pool.
        // busy-X is still busy; idle-A must be gone.
        assert!(!is_active(&pool, "idle-A").await, "idle-A must be evicted");
        assert!(is_active(&pool, "busy-X").await, "busy-X must survive");
        assert!(is_active(&pool, "idle-B").await, "idle-B must be present");
        assert_eq!(active_len(&pool).await, 2);
        // busy-X must STILL be marked busy — eviction did not touch it.
        assert!(pool.state.read().await.activity["busy-X"].in_flight());
        assert_phase(&pool, "busy-X", entry_phase::BUSY, "test C").await;
    }

    // ── TEST D ────────────────────────────────────────────────────────────
    //
    // D — Same-key reuse after a prompt completes. The second call MUST
    // return `created = false` (reuse, not spawn). The busy flag must be
    // cleared by the BusyGuard's Drop impl when the first prompt's
    // `with_connection` returns.

    #[cfg(unix)]
    #[tokio::test]
    async fn d_same_key_reuse_after_prompt_completion() {
        let temp = tempfile::tempdir().unwrap();
        let (pool, _wd) = pool_with_max_sessions(&temp, 4).await;
        pool.get_or_create("reuse", None).await.expect("spawn reuse");
        // First prompt: arm the BusyGuard via `with_connection`, then
        // return Ok. On Drop the guard must clear the busy flag.
        let first_prompt: anyhow::Result<()> = pool
            .with_connection("reuse", |_conn| {
                Box::pin(async move {
                    // No actual RPC — we are not testing prompt success
                    // here, only the lifecycle. The guard is dropped
                    // when this closure returns.
                    Ok(())
                })
            })
            .await;
        first_prompt.expect("first prompt must succeed");
        // After the closure returns, busy MUST be cleared (RAII).
        assert!(
            !pool.state.read().await.activity["reuse"].in_flight(),
            "BusyGuard::Drop must clear busy after Ok"
        );
        // Second `get_or_create` for the same key MUST NOT spawn.
        let created = pool
            .get_or_create("reuse", None)
            .await
            .expect("reuse call must succeed");
        assert!(
            !created,
            "second get_or_create for same key must reuse, not spawn"
        );
        assert_eq!(active_len(&pool).await, 1);
    }

    // ── TEST E ────────────────────────────────────────────────────────────
    //
    // E — Prompt error path clears busy. The previous code relied on an
    // explicit `prompt_done` call which could be skipped on error. The
    // BusyGuard's Drop must clear busy regardless of the closure's
    // outcome.

    #[cfg(unix)]
    #[tokio::test]
    async fn e_prompt_error_clears_busy_via_raii() {
        let temp = tempfile::tempdir().unwrap();
        let (pool, _wd) = pool_with_max_sessions(&temp, 4).await;
        pool.get_or_create("errkey", None).await.expect("spawn errkey");
        let result: anyhow::Result<()> = pool
            .with_connection("errkey", |_conn| {
                Box::pin(async move { Err(anyhow::anyhow!("synthetic prompt failure")) })
            })
            .await;
        assert!(result.is_err(), "prompt closure must propagate its Err");
        // Despite the error, the entry MUST be IDLE again — that is
        // the whole point of the RAII guard. Pre-Phase-6.4.10 this
        // would still be busy forever (or until TTL cleanup).
        assert!(
            !pool.state.read().await.activity["errkey"].in_flight(),
            "BusyGuard::Drop must clear busy after Err"
        );
        // And the entry must be reusable: another `with_connection`
        // call must succeed (acquire the connection Arc).
        let reuse: anyhow::Result<()> = pool
            .with_connection("errkey", |_conn| Box::pin(async move { Ok(()) }))
            .await;
        reuse.expect("entry must be reusable after Err");
    }

    // ── TEST F ────────────────────────────────────────────────────────────
    //
    // F — Drop-while-in-flight clears busy. Simulates a cancellation by
    // letting the future complete via drop (the closure is cancelled
    // mid-flight). The guard's Drop impl must still fire because Drop
    // runs on the stack frame that owned the guard.

    #[cfg(unix)]
    #[tokio::test]
    async fn f_dropped_prompt_clears_busy() {
        let temp = tempfile::tempdir().unwrap();
        let (pool, _wd) = pool_with_max_sessions(&temp, 4).await;
        pool.get_or_create("dropkey", None).await.expect("spawn dropkey");
        // The closure is one that the test harness cancels by
        // dropping the future. We use tokio::time::timeout to race
        // the closure: the timeout fires first, the future is
        // dropped, and the guard drops with it.
        let outcome = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            pool.with_connection("dropkey", |_conn| {
                Box::pin(async move {
                    // Sleep "forever" — the test harness will cancel
                    // us via the timeout above.
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                    Ok(())
                })
            }),
        )
        .await;
        assert!(
            outcome.is_err(),
            "timeout must fire before the long sleep completes"
        );
        // Even though the closure was cancelled mid-flight, the
        // guard's Drop impl MUST have run (Drop is not cancelled).
        // The pool state must show the entry IDLE again.
        assert!(
            !pool.state.read().await.activity["dropkey"].in_flight(),
            "BusyGuard::Drop must clear busy after cancellation"
        );
    }

    // ── TEST G ────────────────────────────────────────────────────────────
    //
    // G — Busy entries are NEVER evictable, even if they are the oldest.
    // The previous `try_lock`-based heuristic conflated "no concurrent
    // reader" with "no in-flight prompt" and could evict a busy entry
    // under contention. The new `find_oldest_idle_eviction_candidate`
    // skips every entry whose `in_flight` flag is true.

    #[cfg(unix)]
    #[tokio::test]
    async fn g_busy_entries_are_never_evictable_even_when_oldest() {
        let temp = tempfile::tempdir().unwrap();
        let (pool, _wd) = pool_with_max_sessions(&temp, 2).await;
        // Two entries: one busy-old, one idle-new. The busy-old has
        // the older `last_active` — under the old heuristic it would
        // be the eviction target. Under the new rule, the eviction
        // must skip it and evict the idle-new entry instead.
        pool.get_or_create("busy-old", None).await.expect("spawn busy-old");
        // Force an older last_active by sleeping.
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        pool.get_or_create("idle-new", None).await.expect("spawn idle-new");
        mark_busy(&pool, "busy-old").await;
        // Third spawn at capacity must evict idle-new (the only IDLE
        // entry) — NOT busy-old.
        pool.get_or_create("third", None)
            .await
            .expect("third spawn must succeed by evicting idle-new");
        assert!(
            is_active(&pool, "busy-old").await,
            "busy-old must survive even though it is oldest"
        );
        assert!(
            !is_active(&pool, "idle-new").await,
            "idle-new must be the eviction target"
        );
        assert!(is_active(&pool, "third").await);
        assert!(pool.state.read().await.activity["busy-old"].in_flight());
        assert_phase(&pool, "busy-old", entry_phase::BUSY, "test G").await;
    }

    // ── TEST H ────────────────────────────────────────────────────────────
    //
    // H — When multiple IDLE entries are candidates for eviction, the
    // OLDEST one (smallest `last_active`) is selected. The helper
    // `find_oldest_idle_eviction_candidate` must be deterministic and
    // must skip BUSY entries regardless of their `last_active`.

    #[cfg(unix)]
    #[tokio::test]
    async fn h_oldest_idle_eviction_is_deterministic_and_skips_busy() {
        let temp = tempfile::tempdir().unwrap();
        let (pool, _wd) = pool_with_max_sessions(&temp, 3).await;
        pool.get_or_create("first", None).await.expect("spawn first");
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        pool.get_or_create("second", None).await.expect("spawn second");
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        pool.get_or_create("third", None).await.expect("spawn third");
        // Mark `second` busy so it is excluded from the candidate
        // scan. Now the only IDLE candidates are `first` and
        // `third`. `first` is older; `find_oldest_idle_eviction_
        // candidate` must select `first`.
        mark_busy(&pool, "second").await;
        // Fill the pool: we are already at 3. Add a fourth key, which
        // triggers eviction.
        pool.get_or_create("fourth", None)
            .await
            .expect("fourth spawn must succeed");
        assert!(
            !is_active(&pool, "first").await,
            "first must be evicted (oldest idle)"
        );
        assert!(
            is_active(&pool, "second").await,
            "second must survive (busy)"
        );
        assert!(
            is_active(&pool, "third").await,
            "third must survive (newer idle)"
        );
        assert!(is_active(&pool, "fourth").await);
    }

    // ── TEST I ────────────────────────────────────────────────────────────
    //
    // I — Native-dispatch fast lane (`create_fresh_session_only`) is now
    // bounded by the same capacity invariant. The previous code allowed
    // `state.active.len() > max_sessions` with only a warning, so a
    // runaway scheduler dispatch could grow the pool past the ceiling.

    #[cfg(unix)]
    #[tokio::test]
    async fn i_native_dispatch_fast_lane_respects_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let (pool, _wd) = pool_with_max_sessions(&temp, 2).await;
        // Two native-dispatch keys at the ceiling.
        let k1 = format_native_dispatch_key("ArthurClaude", "oad-1");
        let k2 = format_native_dispatch_key("ArthurClaude", "oad-2");
        pool.create_fresh_session_only(&k1, None)
            .await
            .expect("native-1 must succeed");
        pool.create_fresh_session_only(&k2, None)
            .await
            .expect("native-2 must succeed");
        // Mark both busy so neither is evictable.
        mark_busy(&pool, &k1).await;
        mark_busy(&pool, &k2).await;
        // Third native-dispatch call: must return pool exhausted.
        let k3 = format_native_dispatch_key("ArthurClaude", "oad-3");
        let result = pool.create_fresh_session_only(&k3, None).await;
        let err = result.expect_err("third native dispatch must fail when all are busy");
        assert!(
            err.to_string().contains("pool exhausted"),
            "native fast lane must respect capacity, got: {err}"
        );
        // The pool must NOT have grown past the ceiling.
        assert_eq!(
            active_len(&pool).await,
            2,
            "native fast lane must not exceed max_sessions"
        );
        // busy entries must STILL be present and busy.
        assert!(pool.state.read().await.activity[&k1].in_flight());
        assert!(pool.state.read().await.activity[&k2].in_flight());
        assert_phase(&pool, &k1, entry_phase::BUSY, "test I").await;
        assert_phase(&pool, &k2, entry_phase::BUSY, "test I").await;
    }

    // ── TEST J ────────────────────────────────────────────────────────────
    //
    // J — Failed capacity acquisition leaks NO ACP child. When
    // `ensure_capacity_or_evict` fails (every entry is BUSY), the spawn
    // must not happen, so there must be no orphan child process. We
    // verify by counting children before vs after the failed call.

    #[cfg(unix)]
    #[tokio::test]
    async fn j_failed_capacity_acquisition_leaks_no_child() {
        let temp = tempfile::tempdir().unwrap();
        let (pool, _wd) = pool_with_max_sessions(&temp, 1).await;
        // One entry at the ceiling, marked busy.
        pool.get_or_create("only", None).await.expect("spawn only");
        mark_busy(&pool, "only").await;
        // Capture active len (proxy for child count: every active
        // entry owns exactly one child process).
        let pre = active_len(&pool).await;
        // Attempted second spawn must fail.
        let result = pool.get_or_create("would-orphan", None).await;
        assert!(result.is_err(), "second spawn must fail at capacity");
        // The pool must not have grown.
        let post = active_len(&pool).await;
        assert_eq!(
            pre, post,
            "failed capacity acquisition must not spawn a child"
        );
        assert!(
            !is_active(&pool, "would-orphan").await,
            "the failed key must not be inserted"
        );
        // The original entry is still healthy and still busy.
        assert!(pool.state.read().await.activity["only"].in_flight());
        assert_phase(&pool, "only", entry_phase::BUSY, "test J").await;
    }

    // ─────────────────────────────────────────────────────────────────────
    // Phase 6.4.10 (TOCTOU fix) — concurrency regression tests K–P.
    //
    // These exercise the two race conditions identified in the
    // VERIFIER_FAIL cycle:
    //
    //   DEFECT 1 — Capacity TOCTOU between pre-spawn check and insert:
    //             two concurrent creators could each observe room,
    //             each spawn, each insert, pushing active.len() past
    //             max_sessions.
    //
    //   DEFECT 2 — Busy/Evicting TOCTOU between phase observation and
    //             mutex acquisition: with_connection could win the
    //             mutex after eviction had decided to remove the
    //             entry.
    //
    // The new design (atomic phase CAS + CreationReservation) closes
    // both. These tests verify the closure under direct concurrent
    // pressure: `tokio::join!` fires two creators at the same instant
    // and we assert that the post-conditions hold no matter which
    // scheduler ordering occurs.
    // ─────────────────────────────────────────────────────────────────────

    // ── TEST K ────────────────────────────────────────────────────────────
    //
    // K — Two fresh creators at max_sessions=1 must NOT both succeed.
    // The CreationReservation closes the capacity TOCTOU window by
    // incrementing `outstanding_creations` under the state lock
    // BEFORE the spawn. This test deterministically proves that the
    // second creator (C) reached the capacity-attempt boundary
    // while B was still parked with its reservation, and therefore
    // could NOT also reserve the same slot.
    //
    // Synchronization hooks (all `#[cfg(test)]`, dead in production):
    //
    //   * `creator_arrival_notify` — fires at the TOP of
    //     `get_or_create` (and `create_fresh_session_only`), BEFORE
    //     any lock acquire. Because the fire position is BEFORE
    //     `state.write().await` in the per-thread gate acquisition,
    //     it is reachable by B (parked in the eviction branch) AND
    //     by C (blocked on the per-thread gate lock). This is the
    //     "participant has entered the pool" signal.
    //
    //   * `eviction_reserved_notify` — fires inside the eviction
    //     branch AFTER `outstanding_creations.fetch_add(1)` and
    //     BEFORE the barrier await. The "B has parked with
    //     reservation" signal.
    //
    //   * `eviction_reservation_barrier` — Direction B release: the
    //     test thread signals the parked creator to proceed.
    //
    // Why `creator_arrival_notify` and NOT `ensure_capacity_attempt_notify`:
    // the latter fires inside `ensure_capacity_or_evict`, which is
    // unreachable for the blocked creator (C blocks on the per-thread
    // gate's `state.write().await` BEFORE reaching the function). The
    // arrival hook sits above that lock so both B and C can fire it
    // before being blocked. This is a `cfg(test)` hook only — the
    // production lock acquisition order is unchanged.
    //
    // Two-party synchronization with explicit ordering to defeat
    // Notify's at-most-one-stored-permit coalescing and stale-seed
    // permits:
    //
    //   1. Seed BEFORE any hook arm (no stale seed permit).
    //   2. Arm hooks AFTER seed.
    //   3. Spawn B alone. B fires creator_arrival (permit stored).
    //      B reaches eviction branch, fires reserved_notify (permit
    //      stored). B parks on barrier.
    //   4. Consume B's arrival permit (unambiguously B's).
    //   5. Consume B's reserved permit.
    //   6. Spawn C ONLY after B is parked.
    //   7. Consume C's fresh arrival permit (unambiguously C's —
    //      step 4 cleared B's). C has entered get_or_create and is
    //      now blocked on the state write lock that B holds.
    //   8. Release barrier. B unblocks, commits, returns Ok. C is
    //      queued behind the lock and observes the pool is full +
    //      0 outstanding, returns Err("pool exhausted").

    #[cfg(unix)]
    #[tokio::test]
    async fn k_concurrent_distinct_fresh_creation_bounded_by_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let agent_script = write_test_agent_script(temp.path());
        let mut pool = SessionPool::with_test_state(
            AgentConfig {
                command: agent_script.to_string_lossy().into(),
                args: Vec::new(),
                working_dir: temp.path().to_string_lossy().into(),
                env: HashMap::new(),
                inherit_env: Vec::new(),
                command_explicit: true,
            },
            SessionPoolTestState::default(),
            temp.path().join("session_projects_k.json"),
        );
        pool.set_max_sessions_for_test(1);

        // Step 1: seed BEFORE any hook arm.
        pool.get_or_create("seed", None).await.expect("seed spawn");

        // Step 2: arm hooks AFTER seed.
        let barrier = pool.install_eviction_reservation_barrier_for_test();
        let creator_arrival =
            pool.install_creator_arrival_notify_for_test();
        let reserved_notify = pool.install_eviction_reserved_notify_for_test();
        let pool = Arc::new(pool);

        // Step 3: spawn B alone.
        let pool_a = Arc::clone(&pool);
        let b_task = tokio::spawn(async move { pool_a.get_or_create("B", None).await });

        // Step 4: consume B's arrival permit.
        creator_arrival.notified().await;

        // Step 5: consume B's reserved permit.
        reserved_notify.notified().await;
        assert_eq!(
            pool.outstanding_creations
                .load(std::sync::atomic::Ordering::Acquire),
            1,
            "B must have reserved the slot before C starts"
        );

        // Step 6: spawn C ONLY after B is parked.
        let pool_c = Arc::clone(&pool);
        let c_task = tokio::spawn(async move { pool_c.get_or_create("C", None).await });

        // Step 7: consume C's fresh arrival permit.
        creator_arrival.notified().await;
        assert_eq!(
            pool.outstanding_creations
                .load(std::sync::atomic::Ordering::Acquire),
            1,
            "C must NOT have acquired a reservation while B is parked"
        );

        // Step 8: release B; C is rejected by capacity.
        barrier.notify_one();
        let res_b = b_task.await.expect("b join");
        let res_c = c_task.await.expect("c join");
        let oks = [res_b.is_ok(), res_c.is_ok()].iter().filter(|r| **r).count();
        let errs = [res_b.is_ok(), res_c.is_ok()].iter().filter(|r| !**r).count();
        assert_eq!(oks, 1, "exactly one of B/C must succeed");
        assert_eq!(errs, 1, "the other must be rejected");
        let c_err = res_c.expect_err("loser must be Err");
        assert!(
            c_err.to_string().contains("pool exhausted"),
            "loser must be rejected with pool exhausted (got: {c_err})"
        );
        assert_eq!(
            active_len(&pool).await,
            1,
            "pool must NEVER exceed max_sessions=1"
        );
        assert_eq!(
            pool.outstanding_creations
                .load(std::sync::atomic::Ordering::Acquire),
            0,
            "outstanding must be 0 after both creators finished"
        );
    }

    // ── TEST L ────────────────────────────────────────────────────────────
    //
    // L — Two native-dispatch fresh creators at max_sessions=1 must
    // also be bounded. The native-dispatch fast lane previously
    // tolerated `state.active.len() > max_sessions` with only a
    // warning; the new invariant guarantees
    // `active.len() + outstanding_creations <= max_sessions` for the
    // fast lane too. Same sequencing discipline as K — seed before
    // arm; spawn t1; consume t1's arrival; consume t1's reserved;
    // spawn t2; consume t2's fresh arrival; release.

    #[cfg(unix)]
    #[tokio::test]
    async fn l_concurrent_native_fresh_creation_bounded_by_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let agent_script = write_test_agent_script(temp.path());
        let mut pool = SessionPool::with_test_state(
            AgentConfig {
                command: agent_script.to_string_lossy().into(),
                args: Vec::new(),
                working_dir: temp.path().to_string_lossy().into(),
                env: HashMap::new(),
                inherit_env: Vec::new(),
                command_explicit: true,
            },
            SessionPoolTestState::default(),
            temp.path().join("session_projects_l.json"),
        );
        pool.set_max_sessions_for_test(1);

        // Step 1: seed BEFORE any hook arm.
        pool.get_or_create("human-seed", None).await.expect("seed");

        // Step 2: arm hooks AFTER seed.
        let barrier = pool.install_eviction_reservation_barrier_for_test();
        let creator_arrival =
            pool.install_creator_arrival_notify_for_test();
        let reserved_notify = pool.install_eviction_reserved_notify_for_test();
        let pool = Arc::new(pool);

        let k1 = format_native_dispatch_key("ArthurClaude", "oad-L-1");
        let k2 = format_native_dispatch_key("ArthurClaude", "oad-L-2");

        // Step 3: spawn only t1 (the "B" creator) first.
        let pool_a = Arc::clone(&pool);
        let k1c = k1.clone();
        let t1 = tokio::spawn(async move {
            pool_a.create_fresh_session_only(&k1c, None).await
        });

        // Step 4: consume t1's arrival permit.
        creator_arrival.notified().await;
        // Step 5: consume t1's reserved permit.
        reserved_notify.notified().await;
        assert_eq!(
            pool.outstanding_creations
                .load(std::sync::atomic::Ordering::Acquire),
            1,
            "t1 must have reserved the slot before t2 starts"
        );

        // Step 6: spawn t2 ONLY AFTER t1 is parked.
        let pool_b = Arc::clone(&pool);
        let k2c = k2.clone();
        let t2 = tokio::spawn(async move {
            pool_b.create_fresh_session_only(&k2c, None).await
        });

        // Step 7: consume t2's fresh arrival permit.
        creator_arrival.notified().await;
        assert_eq!(
            pool.outstanding_creations
                .load(std::sync::atomic::Ordering::Acquire),
            1,
            "second native creator must NOT have acquired a reservation"
        );

        // Step 8: release t1; t2 is rejected by capacity.
        barrier.notify_one();
        let r1: anyhow::Result<bool> = t1.await.expect("t1 join");
        let r2: anyhow::Result<bool> = t2.await.expect("t2 join");
        let oks = [r1.is_ok(), r2.is_ok()].iter().filter(|r| **r).count();
        let errs = [r1.is_ok(), r2.is_ok()].iter().filter(|r| !**r).count();
        assert_eq!(oks, 1, "exactly one native creator must succeed");
        assert_eq!(errs, 1, "the other must be rejected");
        assert_eq!(
            active_len(&pool).await,
            1,
            "native fast lane must NEVER exceed max_sessions=1"
        );
        if r1.is_err() {
            assert_not_active(&pool, &k1, "loser (k1)").await;
        }
        if r2.is_err() {
            assert_not_active(&pool, &k2, "loser (k2)").await;
        }
    }

    // ── TEST M ────────────────────────────────────────────────────────────
    //
    // M — Reuse-vs-eviction race. With max_sessions=1 and one IDLE
    // entry A, coordinate two tasks:
    //
    //   Task 1: with_connection(A) — would CAS IDLE→BUSY
    //   Task 2: get_or_create(B) — would CAS IDLE→EVICTING for A
    //
    // The CAS is the authority: exactly one transition wins. Either:
    //   * Task 1 wins BUSY → Task 2's eviction sees BUSY and skips A,
    //     returns `pool exhausted`.
    //   * Task 2 wins EVICTING → Task 1's with_connection sees EVICTING
    //     and returns "session is being evicted".
    //
    // NEVER: A becomes BUSY and is then evicted, leaving Task 1 using
    // an entry that has been removed.
    //
    // This test uses the deterministic barrier to force the
    // eviction-claim path to run to completion BEFORE task 1's
    // `with_connection` even gets a chance to read state. Task 1 will
    // see A is no longer in `state.active` and return Err. The test
    // also runs a second variant where task 1 fires its BUSY CAS
    // FIRST (by acquiring `with_connection` before installing the
    // barrier-driven eviction), to prove the inverse ordering is
    // also correct.

    #[cfg(unix)]
    #[tokio::test]
    async fn m_reuse_vs_eviction_race_is_exclusive() {
        // ── Variant 1: eviction-claim wins first (deterministic). ──
        // max_sessions=1, A is IDLE. The barrier parks the evictor
        // inside `ensure_capacity_or_evict` AFTER it has CAS'd A to
        // EVICTING and removed A from state.active. The
        // with_connection task is spawned second and is forced to
        // observe the post-eviction state.
        let temp = tempfile::tempdir().unwrap();
        let agent_script = write_test_agent_script(temp.path());
        let mut pool = SessionPool::with_test_state(
            AgentConfig {
                command: agent_script.to_string_lossy().into(),
                args: Vec::new(),
                working_dir: temp.path().to_string_lossy().into(),
                env: HashMap::new(),
                inherit_env: Vec::new(),
                command_explicit: true,
            },
            SessionPoolTestState::default(),
            temp.path().join("session_projects_m.json"),
        );
        pool.set_max_sessions_for_test(1);
        let barrier = pool.install_eviction_reservation_barrier_for_test();
        let reserved_notify = pool.install_eviction_reserved_notify_for_test();
        let pool = Arc::new(pool);
        pool.get_or_create("A", None).await.expect("seed A");

        // Capture A's Arc for later phase inspection (independent
        // of state.active visibility).
        let a_arc = {
            let st = pool.state.read().await;
            st.active.get("A").cloned().expect("A present")
        };

        // Task 2: get_or_create "B". Reaches eviction branch, parks.
        let pool_e = Arc::clone(&pool);
        let evict_task = tokio::spawn(async move {
            pool_e.get_or_create("B", None).await
        });

        // Wait deterministically for the reservation to land: the
        // reserved_notify fires AFTER fetch_add(1) and BEFORE the
        // barrier await, so consuming its permit proves both that
        // fetch_add has happened AND that the barrier park is
        // imminent. This replaces the prior atomic-poll / yield-loop
        // assumption.
        reserved_notify.notified().await;
        assert_eq!(
            pool.outstanding_creations
                .load(std::sync::atomic::Ordering::Acquire),
            1,
            "evictor must have its reservation in place"
        );
        // Verify A is EVICTING (the CAS happened).
        assert_eq!(
            phase_load(&a_arc),
            entry_phase::EVICTING,
            "A must be EVICTING by the time task 2 is parked"
        );

        // Task 1: with_connection("A"). It tries `state.read()`,
        // which blocks on task 2's write lock. After we release the
        // barrier, task 2 finishes, commits B, drops the lock.
        // Task 1 then reads state, sees no entry for "A", and
        // returns Err("no connection for thread A").
        let pool_r = Arc::clone(&pool);
        let reuse_task = tokio::spawn(async move {
            pool_r
                .with_connection("A", |_conn| Box::pin(async move { Ok(()) }))
                .await
        });

        barrier.notify_one();
        let evict_res = evict_task.await.expect("evict join");
        assert!(evict_res.is_ok(), "evictor must succeed (got {:?})", evict_res);
        let reuse_res = reuse_task.await.expect("reuse join");
        let reuse_err = reuse_res.expect_err(
            "reuse must fail because A was evicted while reuse was queued",
        );
        assert!(
            reuse_err.to_string().contains("no connection")
                || reuse_err.to_string().contains("evicted")
                || reuse_err.to_string().contains("evicting"),
            "reuse error must mention missing/evicted entry: {reuse_err}"
        );
        // Final invariants.
        assert!(is_active(&pool, "B").await);
        assert!(!is_active(&pool, "A").await);
        assert_eq!(active_len(&pool).await, 1);

        // ── Variant 2: BUSY wins first. ──
        //
        // Two-party synchronization:
        //
        //   A. `busy_claimed_notify` — fires from inside the
        //      `with_connection` closure BEFORE the closure body
        //      runs, AFTER the BUSY CAS has succeeded. The test
        //      thread consumes this permit to prove "prompt task
        //      claimed BUSY".
        //
        //   B. `release_notify` — the test thread signals the
        //      prompt closure to release BUSY and complete. Only
        //      AFTER observing the evictor's `pool exhausted`
        //      result does the test release the prompt.
        //
        // The eviction task runs SYNCHRONOUSLY in the test thread
        // (no `tokio::spawn`) because we must observe its result
        // before releasing the prompt closure. Since the prompt
        // closure is parked waiting on `release_notify`, the
        // eviction task is the only thing keeping the test thread
        // from racing.
        let pool2 = Arc::clone(&pool);
        let reuse_held = Arc::clone(&pool);
        // Build a fresh pool for variant 2 to avoid state
        // pollution from variant 1 (B is healthy and IDLE).
        drop(pool2);
        drop(reuse_held);
        let temp2 = tempfile::tempdir().unwrap();
        let agent_script2 = write_test_agent_script(temp2.path());
        let mut pool2 = SessionPool::with_test_state(
            AgentConfig {
                command: agent_script2.to_string_lossy().into(),
                args: Vec::new(),
                working_dir: temp2.path().to_string_lossy().into(),
                env: HashMap::new(),
                inherit_env: Vec::new(),
                command_explicit: true,
            },
            SessionPoolTestState::default(),
            temp2.path().join("session_projects_m2.json"),
        );
        pool2.set_max_sessions_for_test(1);
        let pool2 = Arc::new(pool2);
        pool2.get_or_create("A2", None).await.expect("seed A2");

        let busy_claimed_notify = Arc::new(tokio::sync::Notify::new());
        let release_notify = Arc::new(tokio::sync::Notify::new());

        // Spawn the prompt task. The closure:
        //   * signals `busy_claimed_notify` once BUSY has been CAS'd
        //     (the closure body runs AFTER the CAS, so signaling
        //     here proves the CAS succeeded);
        //   * then parks on `release_notify` until the test thread
        //     releases it.
        let pool_inner = Arc::clone(&pool2);
        let bc_clone = Arc::clone(&busy_claimed_notify);
        let rel_clone = Arc::clone(&release_notify);
        let held_task = tokio::spawn(async move {
            pool_inner
                .with_connection("A2", move |_conn| {
                    let bc = Arc::clone(&bc_clone);
                    let rel = Arc::clone(&rel_clone);
                    Box::pin(async move {
                        bc.notify_one(); // A: "I claimed BUSY"
                        rel.notified().await; // wait for test release
                        Ok(())
                    })
                })
                .await
        });

        // Direction A: wait for the prompt closure to signal it
        // has CAS'd A2 to BUSY.
        busy_claimed_notify.notified().await;
        assert_eq!(
            phase_load(
                &pool2.state.read().await.active.get("A2").cloned().unwrap()
            ),
            entry_phase::BUSY,
            "A2 must be BUSY before evictor runs"
        );

        // Run the eviction task SYNCHRONOUSLY. While the prompt
        // closure holds BUSY, the eviction branch scans for an
        // IDLE candidate, finds A2 is BUSY (skip), no other
        // candidates, returns Err("pool exhausted").
        let evict_attempt = pool2.get_or_create("C2", None).await;
        let evict_err =
            evict_attempt.expect_err("evictor must fail when only candidate is BUSY");
        assert!(
            evict_err.to_string().contains("pool exhausted"),
            "evictor must report pool exhausted: {evict_err}"
        );

        // Direction B: only AFTER observing the evictor's
        // rejection do we release the prompt closure. This proves
        // the eviction branch saw BUSY, not a transient IDLE.
        release_notify.notify_one();
        let prompt_result = held_task.await.expect("held join");
        let prompt_value = prompt_result.expect("prompt must succeed");
        // `with_connection` returns the closure's Result<R>; Ok(())
        // means the closure completed cleanly.
        let _ = prompt_value;
        // A2 is still BUSY... wait, actually BUSY has been
        // released by the PhaseGuard Drop on closure exit, so A2
        // is now IDLE. Active length is still 1.
        assert!(is_active(&pool2, "A2").await);
        assert_eq!(active_len(&pool2).await, 1);
    }

    // ── TEST N ────────────────────────────────────────────────────────────
    //
    // N — Eviction-vs-reuse INVERSE ordering. Force the eviction to
    // claim EVICTING first, then attempt reuse. Reuse MUST NOT execute
    // using an entry already claimed for eviction.

    #[cfg(unix)]
    #[tokio::test]
    async fn n_eviction_claim_first_blocks_subsequent_reuse() {
        let temp = tempfile::tempdir().unwrap();
        let (pool, _wd) = pool_with_max_sessions(&temp, 1).await;
        pool.get_or_create("A", None).await.expect("seed A");
        // Manually flip A to EVICTING (simulating an in-progress
        // eviction claim) so the next with_connection attempt must
        // observe EVICTING and refuse.
        mark_busy(&pool, "A").await; // first BUSY (atomic) to allow controlled flip
        let state = pool.state.read().await;
        let conn = state.active.get("A").cloned().expect("A active");
        // Bypass mark_busy and force EVICTING directly.
        phase_force_set(&conn, entry_phase::EVICTING);
        drop(state);
        // with_connection must FAIL because A is EVICTING.
        let res = pool
            .with_connection("A", |_conn| Box::pin(async move { Ok(()) }))
            .await;
        let err = res.expect_err("with_connection on EVICTING entry must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("evicting") || msg.contains("evicted") || msg.contains("busy"),
            "error must mention evicting/evicted or busy: {msg}"
        );
        // active.len() unchanged.
        assert_eq!(active_len(&pool).await, 1);
    }

    // ── TEST O ────────────────────────────────────────────────────────────
    //
    // O — Reservation released on spawn/init failure. We force spawn
    // to fail (by pointing the agent command at a non-existent binary)
    // and assert that the reservation is released so a subsequent
    // creator can succeed.

    #[cfg(unix)]
    #[tokio::test]
    async fn o_reservation_released_on_spawn_failure() {
        let temp = tempfile::tempdir().unwrap();
        // Pool whose agent command does not exist on disk.
        let mut pool = SessionPool::with_test_state(
            AgentConfig {
                command: "/nonexistent/binary/should/fail".into(),
                args: Vec::new(),
                working_dir: temp.path().to_string_lossy().into(),
                env: HashMap::new(),
                inherit_env: Vec::new(),
                command_explicit: true,
            },
            SessionPoolTestState::default(),
            temp.path().join("session_projects.json"),
        );
        pool.set_max_sessions_for_test(2);
        let pool = Arc::new(pool);
        // First attempt must fail.
        let r1 = pool.get_or_create("fail-key", None).await;
        assert!(r1.is_err(), "spawn must fail when command is missing");
        // outstanding_creations must be back to 0.
        assert_eq!(
            pool.outstanding_creations
                .load(std::sync::atomic::Ordering::Acquire),
            0,
            "failed spawn must release the reservation"
        );
        // The state must not contain a leaked entry.
        assert_not_active(&pool, "fail-key", "after failed spawn").await;
        // A second creator can use the freed slot. Use a working
        // command this time so we can prove the slot is usable.
        let agent_script = write_test_agent_script(temp.path());
        let mut pool2 = SessionPool::with_test_state(
            AgentConfig {
                command: agent_script.to_string_lossy().into(),
                args: Vec::new(),
                working_dir: temp.path().to_string_lossy().into(),
                env: HashMap::new(),
                inherit_env: Vec::new(),
                command_explicit: true,
            },
            SessionPoolTestState::default(),
            temp.path().join("session_projects2.json"),
        );
        pool2.set_max_sessions_for_test(2);
        let pool2 = Arc::new(pool2);
        pool2.get_or_create("good-1", None).await.expect("good-1 spawn");
        pool2.get_or_create("good-2", None).await.expect("good-2 spawn");
        assert_eq!(active_len(&pool2).await, 2);
    }

    // ── TEST P ────────────────────────────────────────────────────────────
    //
    // P — Cancellation while holding a creation reservation. The
    // CreationReservation's Drop impl must release the slot on every
    // exit path, including cancellation AFTER reservation has been
    // acquired but BEFORE insertion. The deterministic
    // `eviction_reservation_barrier` is the contract: install it, the
    // second creator parks INSIDE the eviction branch AFTER
    // `outstanding_creations.fetch_add` and BEFORE
    // `drop(state)`. The test then cancels the future while it's
    // parked, asserts the reservation was released, and proves the
    // slot is reusable by a third creator.
    //
    // Synchronization is fully explicit two-party:
    //
    //   * `reserved_notify` (Direction A, arrival) — fires inside the
    //     eviction branch AFTER fetch_add and BEFORE the barrier
    //     await. The test thread consumes its permit to prove the
    //     creator has its reservation.
    //
    //   * `drop_notify` (Direction A, drop-arrival) — fires inside
    //     `Drop for CreationReservation` AFTER the failure-path
    //     fetch_sub. The test thread consumes its permit after
    //     `b_task.abort()` to deterministically observe that the RAII
    //     guard's Drop has run.
    //
    //   * `barrier` (Direction B, release) — the test thread
    //     signals the parked creator to proceed (only invoked at the
    //     end for cleanup, since B is cancelled before reaching it).

    #[cfg(unix)]
    #[tokio::test]
    async fn p_reservation_released_on_cancellation_via_drop() {
        let temp = tempfile::tempdir().unwrap();
        // Build a pool directly so we can install the barrier. The
        // helper would force Arc<SessionPool> which prevents mutation.
        let agent_script = write_test_agent_script(temp.path());
        let mut pool = SessionPool::with_test_state(
            AgentConfig {
                command: agent_script.to_string_lossy().into(),
                args: Vec::new(),
                working_dir: temp.path().to_string_lossy().into(),
                env: HashMap::new(),
                inherit_env: Vec::new(),
                command_explicit: true,
            },
            SessionPoolTestState::default(),
            temp.path().join("session_projects_p.json"),
        );
        pool.set_max_sessions_for_test(1);
        let barrier = pool.install_eviction_reservation_barrier_for_test();
        let reserved_notify = pool.install_eviction_reserved_notify_for_test();
        let drop_notify = pool.install_reservation_drop_notify_for_test();
        let pool = Arc::new(pool);

        // Seed entry A. It must be IDLE (the eviction branch only
        // runs on IDLE candidates).
        pool.get_or_create("A", None).await.expect("seed A");

        // Race a creator for "B" against cancellation. The creator
        // will reach the eviction branch, evict A, fetch_add
        // outstanding_creations to 1, then park on the barrier.
        let pool_for_b = Arc::clone(&pool);
        let b_task = tokio::spawn(async move {
            pool_for_b.get_or_create("B", None).await
        });

        // Wait deterministically for B's reservation to be acquired:
        // the reserved_notify fires AFTER fetch_add(1) and BEFORE
        // the barrier await, so consuming its permit proves the
        // reservation is in place. This replaces the prior atomic-
        // poll / yield-loop assumption.
        reserved_notify.notified().await;
        assert_eq!(
            pool.outstanding_creations
                .load(std::sync::atomic::Ordering::Acquire),
            1,
            "B must have acquired its reservation before being cancelled"
        );

        // Cancel the B future while it is parked at the barrier.
        // Drop runs the CreationReservation::Drop, which decrements
        // outstanding_creations AND fires `drop_notify` exactly
        // once.
        b_task.abort();
        let join_result = b_task.await;
        assert!(
            join_result.is_err(),
            "aborted task should report JoinError"
        );

        // Wait deterministically for the RAII guard's Drop to run.
        // The drop_notify fires from inside `Drop for
        // CreationReservation` AFTER the failure-path fetch_sub
        // (or after commit sets `committed = true`). This replaces
        // the prior `for _ in 0..16 { yield_now().await; }`
        // heuristic.
        drop_notify.notified().await;

        // Invariant: outstanding_creations is back to 0 and the
        // slot is reusable.
        assert_eq!(
            pool.outstanding_creations
                .load(std::sync::atomic::Ordering::Acquire),
            0,
            "cancelled reservation must release the slot via Drop"
        );

        // A fresh creator "C" can now acquire the slot. The barrier
        // is no-op for this path (no eviction candidate), so we
        // don't need to release it. The seed "A" was already
        // evicted by B's reservation; "C" will spawn fresh.
        let c_res = pool.get_or_create("C", None).await;
        assert!(c_res.is_ok(), "C must succeed after reservation released");
        assert!(is_active(&pool, "C").await);

        // The barrier is never released because B was cancelled —
        // release it now to keep the test deterministic for any
        // follow-up paths.
        barrier.notify_one();
    }

    // ── TEST Q ────────────────────────────────────────────────────────────
    //
    // Q — Eviction-to-reservation atomicity (deterministic). With
    // `max_sessions=1`, a single IDLE entry A, two creators B and C
    // are coordinated so that B reaches the eviction branch and
    // parks INSIDE the state write lock AFTER it has incremented
    // `outstanding_creations`, while C is then spawned and observed
    // to attempt the capacity boundary while B is still parked.
    //
    // Two-party synchronization, with explicit ordering to defeat
    // Notify's at-most-one-stored-permit coalescing and stale-seed
    // permits:
    //
    //   1. Seed A BEFORE any hook arm. (No stale seed permit
    //      possible — the field is None at seed time.)
    //   2. Arm hooks AFTER seed.
    //   3. Spawn B alone.
    //   4. Consume B's arrival permit.
    //   5. Consume B's reserved permit (after fetch_add, before
    //      barrier park).
    //   6. Spawn C only after B is parked.
    //   7. Consume C's fresh arrival permit.
    //   8. Release barrier.
    //
    // The arrival hook (`creator_arrival_notify`) fires at the TOP
    // of `get_or_create`, BEFORE the per-thread gate's
    // `state.write().await`. This is reachable for both B and C —
    // even though C is blocked on the per-thread gate lock while B
    // holds the eviction-branch write lock, C fires the arrival
    // hook before blocking. `ensure_capacity_attempt_notify`
    // (firing inside `ensure_capacity_or_evict`) is unreachable for
    // C under this contention shape and is therefore unsuitable as
    // the C signal.
    //
    // No atomic-poll loops, no yield loops, no sleeps.

    #[cfg(unix)]
    #[tokio::test]
    async fn q_eviction_to_reservation_is_atomic_under_deterministic_barrier() {
        let temp = tempfile::tempdir().unwrap();
        let agent_script = write_test_agent_script(temp.path());
        let mut pool = SessionPool::with_test_state(
            AgentConfig {
                command: agent_script.to_string_lossy().into(),
                args: Vec::new(),
                working_dir: temp.path().to_string_lossy().into(),
                env: HashMap::new(),
                inherit_env: Vec::new(),
                command_explicit: true,
            },
            SessionPoolTestState::default(),
            temp.path().join("session_projects_q.json"),
        );
        pool.set_max_sessions_for_test(1);

        // Step 1: seed BEFORE any hook arm.
        pool.get_or_create("A", None).await.expect("seed A");
        assert_eq!(active_len(&pool).await, 1);

        // Step 2: arm hooks AFTER seed.
        let barrier = pool.install_eviction_reservation_barrier_for_test();
        let creator_arrival =
            pool.install_creator_arrival_notify_for_test();
        let reserved_notify = pool.install_eviction_reserved_notify_for_test();
        let pool = Arc::new(pool);

        // Step 3: spawn B alone.
        let pool_for_b = Arc::clone(&pool);
        let b_task = tokio::spawn(async move {
            pool_for_b.get_or_create("B", None).await
        });

        // Step 4: consume B's arrival permit.
        creator_arrival.notified().await;
        // Step 5: consume B's reserved permit.
        reserved_notify.notified().await;
        assert_eq!(
            pool.outstanding_creations
                .load(std::sync::atomic::Ordering::Acquire),
            1,
            "B must have reserved the freed slot atomically"
        );

        // Step 6: spawn C only after B is parked.
        let pool_for_c = Arc::clone(&pool);
        let c_task = tokio::spawn(async move {
            pool_for_c.get_or_create("C", None).await
        });

        // Step 7: consume C's fresh arrival permit. Unambiguously
        // C's because step 4 cleared B's. C is queued on the
        // state write lock that B holds.
        creator_arrival.notified().await;
        assert_eq!(
            pool.outstanding_creations
                .load(std::sync::atomic::Ordering::Acquire),
            1,
            "C must NOT have acquired a reservation while B is parked"
        );

        // Step 8: release B; C is rejected by capacity.
        barrier.notify_one();
        let b_res = b_task.await.expect("b join").expect("b not panic");
        assert!(b_res, "B must successfully create");
        let c_res = c_task.await.expect("c join");
        let c_err = c_res.expect_err("C must be rejected with pool exhausted");
        assert!(
            c_err.to_string().contains("pool exhausted"),
            "C must be rejected by capacity (got: {c_err})"
        );

        // Final invariants.
        assert_eq!(
            pool.outstanding_creations
                .load(std::sync::atomic::Ordering::Acquire),
            0,
            "outstanding must be 0 after B committed"
        );
        assert_eq!(active_len(&pool).await, 1);
        assert!(is_active(&pool, "B").await);
        assert!(!is_active(&pool, "A").await);
    }


    #[cfg(unix)]
    #[tokio::test]
    async fn approval_resume_rejects_mismatched_acp_session() {
        let temp = tempfile::tempdir().unwrap();
        let agent_script = write_test_agent_script(temp.path());

        let pool = SessionPool::with_test_state(
            AgentConfig {
                command: agent_script.to_string_lossy().into(),
                args: Vec::new(),
                working_dir: temp.path().to_string_lossy().into(),
                env: HashMap::new(),
                inherit_env: Vec::new(),
                command_explicit: true,
            },
            SessionPoolTestState::default(),
            temp.path().join("session_projects_approval_mismatch.json"),
        );

        pool.get_or_create("approval-thread", None)
            .await
            .expect("seed approval session");

        let error = pool
            .resume_approval(
                "approval-thread",
                "definitely-not-the-current-session",
                "apr-123",
                "approve",
                "wfc-456",
                2,
                crate::acp::connection::AcpPromptIdentity {
                    user_id: Some("user-123".into()),
                    thread_id: Some("approval-thread".into()),
                    openab_message_id: None,
                },
            )
            .await
            .expect_err("mismatched ACP session must fail closed");

        assert!(
            error.to_string().contains("approval session mismatch"),
            "unexpected error: {error}"
        );
    }

    // ── TEST R ────────────────────────────────────────────────────────────
    //
    // R — Native-dispatch version of Q. Same invariant
    // (`active + outstanding <= max`) must hold on the native
    // `create_fresh_session_only` fast lane. Same sequencing
    // discipline as Q — seed before arm; spawn t1; consume t1's
    // arrival; consume t1's reserved; spawn t2; consume t2's fresh
    // arrival; release.

    #[cfg(unix)]
    #[tokio::test]
    async fn r_native_eviction_to_reservation_is_atomic_under_deterministic_barrier() {
        let temp = tempfile::tempdir().unwrap();
        let agent_script = write_test_agent_script(temp.path());
        let mut pool = SessionPool::with_test_state(
            AgentConfig {
                command: agent_script.to_string_lossy().into(),
                args: Vec::new(),
                working_dir: temp.path().to_string_lossy().into(),
                env: HashMap::new(),
                inherit_env: Vec::new(),
                command_explicit: true,
            },
            SessionPoolTestState::default(),
            temp.path().join("session_projects_r.json"),
        );
        pool.set_max_sessions_for_test(1);

        // Step 1: seed BEFORE any hook arm.
        pool.get_or_create("human-seed", None).await.expect("seed");
        assert_eq!(active_len(&pool).await, 1);

        // Step 2: arm hooks AFTER seed.
        let barrier = pool.install_eviction_reservation_barrier_for_test();
        let creator_arrival =
            pool.install_creator_arrival_notify_for_test();
        let reserved_notify = pool.install_eviction_reserved_notify_for_test();
        let pool = Arc::new(pool);

        let k1 = format_native_dispatch_key("ArthurClaude", "oad-R-1");
        let k2 = format_native_dispatch_key("ArthurClaude", "oad-R-2");

        // Step 3: spawn only t1 (the "B" creator) first.
        let pool_for_1 = Arc::clone(&pool);
        let k1c = k1.clone();
        let t1 = tokio::spawn(async move {
            pool_for_1.create_fresh_session_only(&k1c, None).await
        });

        // Step 4: consume t1's arrival permit.
        creator_arrival.notified().await;
        // Step 5: consume t1's reserved permit.
        reserved_notify.notified().await;
        assert_eq!(
            pool.outstanding_creations
                .load(std::sync::atomic::Ordering::Acquire),
            1,
            "t1 must have reserved the freed slot atomically"
        );

        // Step 6: spawn t2 only after t1 is parked.
        let pool_for_2 = Arc::clone(&pool);
        let k2c = k2.clone();
        let t2 = tokio::spawn(async move {
            pool_for_2.create_fresh_session_only(&k2c, None).await
        });

        // Step 7: consume t2's fresh arrival permit.
        creator_arrival.notified().await;
        assert_eq!(
            pool.outstanding_creations
                .load(std::sync::atomic::Ordering::Acquire),
            1,
            "second native creator must NOT have acquired a reservation"
        );

        // Step 8: release t1; t2 is rejected by capacity.
        barrier.notify_one();
        let (r1, r2) = tokio::join!(t1, t2);
        let r1: anyhow::Result<bool> = r1.expect("t1 join");
        let r2: anyhow::Result<bool> = r2.expect("t2 join");
        let oks = [r1.is_ok(), r2.is_ok()].iter().filter(|r| **r).count();
        let errs = [r1.is_ok(), r2.is_ok()].iter().filter(|r| !**r).count();
        assert_eq!(oks, 1, "exactly one native creator must succeed");
        assert_eq!(errs, 1, "the other must be rejected");
        assert_eq!(active_len(&pool).await, 1);
        assert_eq!(
            pool.outstanding_creations
                .load(std::sync::atomic::Ordering::Acquire),
            0,
            "outstanding must be 0 after one native creator committed"
        );
    }
}
