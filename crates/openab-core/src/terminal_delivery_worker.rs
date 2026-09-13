//! Durable terminal delivery orchestration (M23 Phase 9C3).
//!
//! The worker deliberately has no Runtime execution capability: its only
//! Runtime operation is the immutable terminal-result GET represented by
//! `TerminalResultLookup`.

use async_trait::async_trait;
use chrono::{Duration, Utc};
use serde_json::{json, Value};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::adapter::{ChannelRef, ChatAdapter};
use crate::terminal_delivery::{
    validate_record_integrity, NewTerminalDeliveryRecord, TerminalDeliveryError,
    TerminalDeliveryRecordV1, TerminalDeliveryRepository, TerminalDeliveryState, TransitionUpdate,
};

pub const SAFE_FAILED_MESSAGE: &str =
    "⚠️ The requested work failed. Please contact the operator if you need assistance.";
pub const SAFE_CANCELLED_MESSAGE: &str = "⚠️ The requested work was cancelled.";
pub const MAX_EXTERNAL_ATTEMPTS: u64 = 5;
/// Maximum durable exact-message reads permitted while resolving an ambiguous
/// Discord terminal delivery. These are read-only verification attempts, not
/// delivery-send retries.
pub const MAX_AMBIGUOUS_RECONCILIATION_ATTEMPTS: usize = 3;
/// A delivery claim older than this is no longer treated as live.  Its
/// outbound outcome is unknown, so recovery may only declare it ambiguous.
pub const STALE_DELIVERING_AFTER_SECS: i64 = 300;
/// Bounded number of records handled by one startup or periodic scan.
pub const TERMINAL_RECONCILIATION_BATCH_LIMIT: usize = 50;
/// Fixed cadence for restart reconciliation.  This is intentionally code-owned
/// for Phase 9C4.6 rather than a new production configuration surface.
pub const TERMINAL_RECONCILIATION_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(45);
const RETRY_DELAYS_SECS: [u64; MAX_EXTERNAL_ATTEMPTS as usize] = [1, 5, 30, 120, 600];
/// Bounds one Serenity-managed terminal send, including any internal 429
/// sleep/retry. An expiry is ambiguous because a write may already be accepted.
const TERMINAL_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

async fn await_terminal_send_with_timeout<F>(
    send: F,
) -> Result<anyhow::Result<crate::adapter::MessageRef>, ()>
where
    F: Future<Output = anyhow::Result<crate::adapter::MessageRef>>,
{
    tokio::time::timeout(TERMINAL_SEND_TIMEOUT, send)
        .await
        .map_err(|_| ())
}

fn retry_delay_secs(attempt: u64) -> u64 {
    RETRY_DELAYS_SECS[(attempt as usize).saturating_sub(1).min(4)]
}

pub fn stale_delivering_before(now: chrono::DateTime<Utc>) -> chrono::DateTime<Utc> {
    now - Duration::seconds(STALE_DELIVERING_AFTER_SECS)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcpTerminalMetadata {
    pub runtime_run_id: String,
    pub legacy_run_id: Option<String>,
    pub acp_request_id: String,
    pub acp_session_id: String,
    pub workflow_run_id: Option<String>,
    pub conversation_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CaptureError {
    MissingRuntimeRunId,
    RunIdMismatch,
}

/// Extract only the canonical execution identity from the terminal ACP result.
/// `workflow_run_id` is intentionally never used as a fallback.
pub fn capture_terminal_metadata(
    result: &Value,
    request_id: u64,
    session_id: &str,
) -> Result<AcpTerminalMetadata, CaptureError> {
    let metadata = result.get("metadata").and_then(Value::as_object);
    let runtime_run_id = metadata
        .and_then(|m| m.get("runtime_run_id"))
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .ok_or(CaptureError::MissingRuntimeRunId)?
        .to_owned();
    let legacy_run_id = metadata
        .and_then(|m| m.get("run_id"))
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .map(str::to_owned);
    if legacy_run_id
        .as_deref()
        .is_some_and(|legacy| legacy != runtime_run_id)
    {
        return Err(CaptureError::RunIdMismatch);
    }
    Ok(AcpTerminalMetadata {
        runtime_run_id,
        legacy_run_id,
        acp_request_id: request_id.to_string(),
        acp_session_id: session_id.to_owned(),
        workflow_run_id: metadata
            .and_then(|m| m.get("workflow_run_id"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        conversation_id: metadata
            .and_then(|m| m.get("conversation_id"))
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned(),
    })
}

#[derive(Clone, Debug)]
pub enum LookupError {
    Forbidden,
    NotFound,
    Conflict,
    Invalid,
    Unavailable,
}
#[async_trait]
pub trait TerminalResultLookup: Send + Sync {
    async fn get_terminal_result(&self, runtime_run_id: &str) -> Result<Value, LookupError>;
}

/// HTTP implementation whose route is deliberately fixed to the one permitted GET.
#[derive(Clone)]
pub struct HttpTerminalResultLookup {
    client: reqwest::Client,
    base_url: String,
    bearer: String,
}
impl HttpTerminalResultLookup {
    pub fn new(base_url: String, bearer: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url,
            bearer,
        }
    }
}
#[async_trait]
impl TerminalResultLookup for HttpTerminalResultLookup {
    async fn get_terminal_result(&self, runtime_run_id: &str) -> Result<Value, LookupError> {
        let url = format!(
            "{}/v1/orchestrations/{}/terminal-result",
            self.base_url.trim_end_matches('/'),
            runtime_run_id
        );
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.bearer)
            .send()
            .await
            .map_err(|_| LookupError::Unavailable)?;
        match response.status().as_u16() {
            200 => response.json().await.map_err(|_| LookupError::Unavailable),
            403 => Err(LookupError::Forbidden),
            404 => Err(LookupError::NotFound),
            409 => Err(LookupError::Conflict),
            422 => Err(LookupError::Invalid),
            _ => Err(LookupError::Unavailable),
        }
    }
}

pub fn materialize_terminal_payload(snapshot: &Value) -> Value {
    let status = snapshot
        .get("terminal_status")
        .or_else(|| snapshot.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let result = snapshot.get("terminal_result").unwrap_or(snapshot);
    let content = match status {
        "completed" | "COMPLETED" => result
            .get("final_answer")
            .or_else(|| result.get("content"))
            .and_then(Value::as_str)
            .unwrap_or("_(no response)_")
            .to_owned(),
        "cancelled" | "CANCELLED" => SAFE_CANCELLED_MESSAGE.to_owned(),
        _ => SAFE_FAILED_MESSAGE.to_owned(),
    };
    json!({"content": content})
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiscordSendResult {
    DefiniteSuccess(String),
    DefiniteTransientFailure { classification: &'static str },
    DefinitePermanentFailure { classification: &'static str },
    Ambiguous { classification: &'static str },
}
#[async_trait]
pub trait DiscordTerminalDeliverySender: Send + Sync {
    async fn send_terminal(&self, channel: &ChannelRef, content: &str) -> DiscordSendResult;
}

/// Immutable evidence returned by an exact Discord message GET.  This is
/// deliberately narrower than `ChatAdapter`: reconciliation can inspect a
/// known message, but cannot create, edit, delete, or search messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscordTerminalMessage {
    pub message_id: String,
    pub channel_id: String,
    pub author_id: String,
    pub content: String,
}

/// Safe classifications for the one exact-message Discord read used by
/// ambiguous-delivery reconciliation.  None of the non-`Found` outcomes is
/// evidence that a terminal message was not delivered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiscordTerminalDeliveryReadResult {
    Found(DiscordTerminalMessage),
    NotFound,
    Forbidden,
    TransientFailure { classification: &'static str },
    Inconclusive { classification: &'static str },
}

#[async_trait]
pub trait DiscordTerminalDeliveryReader: Send + Sync {
    /// Stable Discord user identity for the bot authorized to produce the
    /// persisted terminal message. Display names are never identity evidence.
    fn expected_author_id(&self) -> &str;

    /// Read exactly `message_id` from exactly `channel_id`. Implementations
    /// must not list channel history or use content/time heuristics.
    async fn get_message(
        &self,
        channel_id: &str,
        message_id: &str,
    ) -> DiscordTerminalDeliveryReadResult;
}

/// Outcome of an AMBIGUOUS known-message verification attempt.  Only
/// `Delivered` mutates durable state; every other outcome leaves the record
/// unchanged and must never trigger a send.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KnownMessageVerificationOutcome {
    Delivered,
    Noop,
    NotFound,
    Forbidden,
    TransientFailure,
    Inconclusive,
    IdentityMismatch,
}

/// Structured result of the bounded AMBIGUOUS reconciliation policy.  Errors
/// remain `TerminalDeliveryError`s so a later manager never needs to parse
/// strings to distinguish a completed policy decision from a failed action.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AmbiguousReconciliationOutcome {
    Delivered,
    StillAmbiguous,
    Held,
    Noop,
}

/// Canonical terminal content sent by the existing Discord terminal path.
/// `ChannelId::say` receives this exact string: this path performs no split,
/// markdown transformation, escaping, or suffixing.
pub fn terminal_content_for_discord(payload: &Value) -> &str {
    payload
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or(SAFE_FAILED_MESSAGE)
}

/// Router-facing narrow port. Production delegates to the durable worker;
/// tests can record this handoff without duplicating delivery semantics.
#[async_trait]
pub trait TerminalDeliveryPort: Send + Sync {
    async fn capture_and_deliver(
        &self,
        metadata: AcpTerminalMetadata,
        channel: ChannelRef,
        inbound_message_id: &str,
        sender: &dyn DiscordTerminalDeliverySender,
    ) -> Result<TerminalDeliveryRecordV1, LookupError>;
}

/// Generic adapter seam: a successful adapter response must contain a message id;
/// opaque adapter errors are ambiguous because non-acceptance cannot be proven.
pub struct ChatAdapterTerminalSender {
    adapter: Arc<dyn ChatAdapter>,
}
impl ChatAdapterTerminalSender {
    pub fn new(adapter: Arc<dyn ChatAdapter>) -> Self {
        Self { adapter }
    }
}
#[async_trait]
impl DiscordTerminalDeliverySender for ChatAdapterTerminalSender {
    async fn send_terminal(&self, channel: &ChannelRef, content: &str) -> DiscordSendResult {
        if channel.platform != "discord" || channel.channel_id.parse::<u64>().is_err() {
            return DiscordSendResult::DefinitePermanentFailure {
                classification: "DISCORD_INVALID_DESTINATION",
            };
        }
        if content.trim().is_empty() {
            return DiscordSendResult::DefinitePermanentFailure {
                classification: "DISCORD_INVALID_PAYLOAD",
            };
        }
        match await_terminal_send_with_timeout(self.adapter.send_message(channel, content)).await {
            Err(_) => DiscordSendResult::Ambiguous {
                classification: "DISCORD_TERMINAL_SEND_TIMEOUT",
            },
            Ok(result) => match result {
                Ok(message) if !message.message_id.is_empty() => {
                    DiscordSendResult::DefiniteSuccess(message.message_id)
                }
                Ok(_) => DiscordSendResult::Ambiguous {
                    classification: "DISCORD_SUCCESS_MISSING_MESSAGE_ID",
                },
                Err(error) => classify_adapter_error(&error),
            },
        }
    }
}

/// Convert Serenity's typed write result into the conservative durable outcome.
/// Any error for which Serenity cannot prove that no write reached Discord is
/// deliberately ambiguous; it must never be retried by this phase.
pub fn classify_adapter_error(error: &anyhow::Error) -> DiscordSendResult {
    #[cfg(not(feature = "discord"))]
    let _ = error;
    #[cfg(feature = "discord")]
    if let Some(serenity::Error::Http(http_error)) = error.downcast_ref::<serenity::Error>() {
        use serenity::http::HttpError;
        match http_error {
            HttpError::UnsuccessfulRequest(response) => {
                let status = response.status_code.as_u16();
                if status == 403 || response.error.code == 50001 {
                    return DiscordSendResult::DefinitePermanentFailure {
                        classification: "DISCORD_MISSING_ACCESS",
                    };
                }
                if status == 400 || (400..500).contains(&status) && status != 429 {
                    return DiscordSendResult::DefinitePermanentFailure {
                        classification: "DISCORD_INVALID_REQUEST",
                    };
                }
                if status == 429 {
                    return DiscordSendResult::DefiniteTransientFailure {
                        classification: "DISCORD_RATE_LIMITED",
                    };
                }
                if (500..600).contains(&status) {
                    return DiscordSendResult::DefiniteTransientFailure {
                        classification: "DISCORD_HTTP_5XX_NO_ACCEPTANCE",
                    };
                }
            }
            HttpError::Request(request_error) if request_error.is_connect() => {
                return DiscordSendResult::DefiniteTransientFailure {
                    classification: "DISCORD_PREWRITE_CONNECT_FAILURE",
                };
            }
            _ => {}
        }
    }
    DiscordSendResult::Ambiguous {
        classification: "DISCORD_UNKNOWN_OR_POSTWRITE_TRANSPORT_FAILURE",
    }
}

pub struct TerminalDeliveryWorker {
    repo: Arc<TerminalDeliveryRepository>,
    lookup: Arc<dyn TerminalResultLookup>,
}

/// The externally observable result of one bounded restart-reconciliation
/// action.  A pending record deliberately reports only `Advanced`: sending is
/// deferred to a subsequent candidate scan after its durable state advance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReconciliationOutcome {
    Advanced,
    Delivered,
    Noop,
}

/// Per-record batch result. Errors are retained rather than being swallowed so
/// callers can log/classify the safe domain error while subsequent records
/// continue to reconcile.
#[derive(Debug)]
pub struct ReconciliationRecordResult {
    pub terminal_delivery_key: String,
    pub outcome: Result<ReconciliationOutcome, TerminalDeliveryError>,
}

#[derive(Debug, Default)]
pub struct ReconciliationBatchSummary {
    pub processed: usize,
    pub advanced: usize,
    pub delivered: usize,
    pub no_op: usize,
    pub failed: usize,
    pub results: Vec<ReconciliationRecordResult>,
}

/// Aggregate result for one lifecycle-manager scan.  The worker remains the
/// sole owner of delivery/reconciliation state policy; this type only reports
/// the manager's bounded routing work.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TerminalDeliveryReconciliationSummary {
    pub candidates: usize,
    pub advanced: usize,
    pub delivered: usize,
    pub held: usize,
    pub no_op: usize,
    pub failed: usize,
}

/// Lifecycle orchestration for durable terminal-delivery recovery.
///
/// This deliberately holds both Discord authorities but routes them through
/// separate worker APIs: only READY/due RETRY records see `sender`, and only
/// AMBIGUOUS records see `reader`.  Durable repository CAS/lease operations
/// remain the correctness authority across manager instances.
pub struct TerminalDeliveryReconciliationManager {
    repository: Arc<TerminalDeliveryRepository>,
    worker: Arc<TerminalDeliveryWorker>,
    sender: Arc<dyn DiscordTerminalDeliverySender>,
    reader: Arc<dyn DiscordTerminalDeliveryReader>,
    single_flight: Mutex<()>,
}

impl TerminalDeliveryReconciliationManager {
    pub fn new(
        repository: Arc<TerminalDeliveryRepository>,
        worker: Arc<TerminalDeliveryWorker>,
        sender: Arc<dyn DiscordTerminalDeliverySender>,
        reader: Arc<dyn DiscordTerminalDeliveryReader>,
    ) -> Self {
        Self {
            repository,
            worker,
            sender,
            reader,
            single_flight: Mutex::new(()),
        }
    }

    /// Process one deterministic, bounded repository snapshot.  A failed
    /// record is retained in the summary and does not prevent later records
    /// in this same batch from being processed.
    pub async fn reconcile_once(
        &self,
        now: chrono::DateTime<Utc>,
    ) -> Result<TerminalDeliveryReconciliationSummary, TerminalDeliveryError> {
        let _single_flight = self.single_flight.lock().await;
        let stale_before = stale_delivering_before(now);
        let candidates = self.repository.list_reconciliation_candidates(
            now,
            stale_before,
            TERMINAL_RECONCILIATION_BATCH_LIMIT,
        )?;
        let mut summary = TerminalDeliveryReconciliationSummary {
            candidates: candidates.len(),
            ..Default::default()
        };

        for record in candidates {
            let key = record.terminal_delivery_key.clone();
            match record.state {
                TerminalDeliveryState::Ambiguous => {
                    match self
                        .worker
                        .reconcile_ambiguous(record, self.reader.as_ref())
                        .await
                    {
                        Ok(AmbiguousReconciliationOutcome::Delivered) => summary.delivered += 1,
                        Ok(AmbiguousReconciliationOutcome::Held) => summary.held += 1,
                        Ok(
                            AmbiguousReconciliationOutcome::StillAmbiguous
                            | AmbiguousReconciliationOutcome::Noop,
                        ) => summary.no_op += 1,
                        Err(error) => {
                            summary.failed += 1;
                            warn!(terminal_delivery_key = %key, error = %error, "terminal delivery ambiguous reconciliation failed");
                        }
                    }
                }
                _ => match self
                    .worker
                    .reconcile_record(record, now, stale_before, self.sender.as_ref())
                    .await
                {
                    Ok(ReconciliationOutcome::Advanced) => summary.advanced += 1,
                    Ok(ReconciliationOutcome::Delivered) => summary.delivered += 1,
                    Ok(ReconciliationOutcome::Noop) => summary.no_op += 1,
                    Err(error) => {
                        summary.failed += 1;
                        warn!(terminal_delivery_key = %key, error = %error, "terminal delivery reconciliation failed");
                    }
                },
            }
        }
        Ok(summary)
    }

    /// Run the required startup pass, then one scan per fixed interval until
    /// the application's existing shutdown watch is signalled.
    pub fn spawn(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) -> JoinHandle<()> {
        tokio::spawn(async move {
            if *shutdown.borrow() {
                return;
            }
            if let Err(error) = self.reconcile_and_log("startup").await {
                error!(error = %error, "terminal delivery startup reconciliation scan failed");
            }

            let mut interval = tokio::time::interval_at(
                tokio::time::Instant::now() + TERMINAL_RECONCILIATION_INTERVAL,
                TERMINAL_RECONCILIATION_INTERVAL,
            );
            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        if *shutdown.borrow() {
                            break;
                        }
                        if let Err(error) = self.reconcile_and_log("periodic").await {
                            error!(error = %error, "terminal delivery periodic reconciliation scan failed");
                        }
                    }
                }
            }
            info!("terminal delivery reconciliation manager stopped");
        })
    }

    async fn reconcile_and_log(&self, phase: &'static str) -> Result<(), TerminalDeliveryError> {
        let summary = self.reconcile_once(Utc::now()).await?;
        info!(
            phase,
            candidates = summary.candidates,
            advanced = summary.advanced,
            delivered = summary.delivered,
            held = summary.held,
            no_op = summary.no_op,
            failed = summary.failed,
            "terminal delivery reconciliation scan completed"
        );
        Ok(())
    }
}

impl TerminalDeliveryWorker {
    pub fn new(
        repo: Arc<TerminalDeliveryRepository>,
        lookup: Arc<dyn TerminalResultLookup>,
    ) -> Self {
        Self { repo, lookup }
    }

    /// Shared durable store for composition of the lifecycle manager.  The
    /// repository still owns candidate ordering and all compare-and-swap
    /// transitions; callers receive no alternate mutation path.
    pub fn repository(&self) -> Arc<TerminalDeliveryRepository> {
        self.repo.clone()
    }
    pub async fn capture_and_deliver(
        &self,
        metadata: AcpTerminalMetadata,
        channel: ChannelRef,
        inbound_message_id: &str,
        sender: &dyn DiscordTerminalDeliverySender,
    ) -> Result<TerminalDeliveryRecordV1, LookupError> {
        let snapshot = self
            .lookup
            .get_terminal_result(&metadata.runtime_run_id)
            .await?;
        let payload = materialize_terminal_payload(&snapshot);
        let record = self
            .repo
            .create_or_reuse(NewTerminalDeliveryRecord {
                openab_inbound_turn_id: format!("discord:{inbound_message_id}"),
                platform: "discord".into(),
                channel_id: channel.channel_id.clone(),
                thread_id: channel.thread_id.clone(),
                acp_request_id: metadata.acp_request_id,
                acp_session_id: metadata.acp_session_id,
                runtime_run_id: metadata.runtime_run_id,
                workflow_run_id: metadata.workflow_run_id,
                conversation_id: metadata.conversation_id,
                response_sequence: 0,
                terminal_payload: payload,
            })
            .map_err(|_| LookupError::Unavailable)?;
        let ready = match record.state {
            TerminalDeliveryState::PendingResult => self
                .repo
                .transition(
                    &record.terminal_delivery_key,
                    record.state_revision,
                    TerminalDeliveryState::ReadyToDeliver,
                    TransitionUpdate::default(),
                    "RESULT_MATERIALIZED",
                )
                .map_err(|_| LookupError::Unavailable)?,
            _ => record,
        };
        self.deliver_ready(ready, channel, sender)
            .await
            .map_err(|_| LookupError::Unavailable)
    }

    /// Advance an already-materialized durable pending record after restart.
    /// `PENDING_RESULT` in this outbox already owns its immutable payload, so
    /// this operation deliberately performs no Runtime lookup or rendering.
    pub fn recover_pending_result(
        &self,
        record: TerminalDeliveryRecordV1,
    ) -> Result<TerminalDeliveryRecordV1, TerminalDeliveryError> {
        validate_record_integrity(&record)?;
        if record.state != TerminalDeliveryState::PendingResult {
            return Ok(record);
        }
        self.repo.transition(
            &record.terminal_delivery_key,
            record.state_revision,
            TerminalDeliveryState::ReadyToDeliver,
            TransitionUpdate::default(),
            "RECOVERY_PENDING_RESULT_READY",
        )
    }

    /// Resume an already durable ready/due-retry record through the same
    /// claim/send path as the live worker.  This is the only recovery send
    /// entry point; it neither consults Runtime nor re-renders the payload.
    pub async fn recover_ready_or_retry(
        &self,
        record: TerminalDeliveryRecordV1,
        channel: ChannelRef,
        sender: &dyn DiscordTerminalDeliverySender,
    ) -> Result<TerminalDeliveryRecordV1, TerminalDeliveryError> {
        self.deliver_ready(record, channel, sender).await
    }

    /// Reconcile one AMBIGUOUS record only when its durable Discord message ID
    /// is known. This method is intentionally read-only with respect to
    /// Discord: it never invokes a sender, Runtime lookup, payload
    /// materialization, history search, or resend path.
    pub async fn reconcile_ambiguous_with_known_message(
        &self,
        record: TerminalDeliveryRecordV1,
        reader: &dyn DiscordTerminalDeliveryReader,
    ) -> Result<KnownMessageVerificationOutcome, TerminalDeliveryError> {
        validate_record_integrity(&record)?;
        if record.state != TerminalDeliveryState::Ambiguous || record.platform != "discord" {
            return Ok(KnownMessageVerificationOutcome::Noop);
        }
        let Some(message_id) = record.discord_message_id.as_deref() else {
            return Ok(KnownMessageVerificationOutcome::Noop);
        };
        if message_id.is_empty() || reader.expected_author_id().is_empty() {
            return Ok(KnownMessageVerificationOutcome::Inconclusive);
        }

        match reader.get_message(&record.channel_id, message_id).await {
            DiscordTerminalDeliveryReadResult::NotFound => {
                Ok(KnownMessageVerificationOutcome::NotFound)
            }
            DiscordTerminalDeliveryReadResult::Forbidden => {
                Ok(KnownMessageVerificationOutcome::Forbidden)
            }
            DiscordTerminalDeliveryReadResult::TransientFailure { .. } => {
                Ok(KnownMessageVerificationOutcome::TransientFailure)
            }
            DiscordTerminalDeliveryReadResult::Inconclusive { .. } => {
                Ok(KnownMessageVerificationOutcome::Inconclusive)
            }
            DiscordTerminalDeliveryReadResult::Found(message) => {
                if message.message_id != message_id
                    || message.channel_id != record.channel_id
                    || message.author_id != reader.expected_author_id()
                    || message.content != terminal_content_for_discord(&record.terminal_payload)
                {
                    return Ok(KnownMessageVerificationOutcome::IdentityMismatch);
                }
                match self.repo.transition(
                    &record.terminal_delivery_key,
                    record.state_revision,
                    TerminalDeliveryState::Delivered,
                    TransitionUpdate::default(),
                    "DELIVERY_VERIFIED_BY_MESSAGE_ID",
                ) {
                    Ok(_) => Ok(KnownMessageVerificationOutcome::Delivered),
                    Err(
                        error @ (TerminalDeliveryError::RevisionConflict { .. }
                        | TerminalDeliveryError::ImmutableTerminalState(
                            TerminalDeliveryState::Delivered,
                        )),
                    ) => {
                        let authoritative = self
                            .repo
                            .get(&record.terminal_delivery_key)?
                            .ok_or_else(|| {
                                TerminalDeliveryError::Storage("lost delivery record".into())
                            })?;
                        if authoritative.state == TerminalDeliveryState::Delivered {
                            Ok(KnownMessageVerificationOutcome::Delivered)
                        } else {
                            Err(error)
                        }
                    }
                    Err(error) => Err(error),
                }
            }
        }
    }

    /// Apply the bounded, read-only reconciliation policy for one AMBIGUOUS
    /// record. It never sends, performs Runtime lookup, searches history, or
    /// changes the delivery-send attempt budget.
    pub async fn reconcile_ambiguous(
        &self,
        record: TerminalDeliveryRecordV1,
        reader: &dyn DiscordTerminalDeliveryReader,
    ) -> Result<AmbiguousReconciliationOutcome, TerminalDeliveryError> {
        validate_record_integrity(&record)?;
        if record.state != TerminalDeliveryState::Ambiguous {
            return Ok(AmbiguousReconciliationOutcome::Noop);
        }
        if record
            .discord_message_id
            .as_deref()
            .is_none_or(str::is_empty)
        {
            return self.hold_ambiguous(record, "AMBIGUOUS_UNRESOLVED");
        }
        if record.platform != "discord" || reader.expected_author_id().is_empty() {
            return self.hold_ambiguous(record, "AMBIGUOUS_UNRESOLVED");
        }

        if !self.repo.claim_ambiguous_reconciliation_read(
            &record.terminal_delivery_key,
            record.state_revision,
            MAX_AMBIGUOUS_RECONCILIATION_ATTEMPTS,
        )? {
            return self.hold_if_reconciliation_exhausted(record);
        }

        match self
            .reconcile_ambiguous_with_known_message(record.clone(), reader)
            .await?
        {
            KnownMessageVerificationOutcome::Delivered => {
                Ok(AmbiguousReconciliationOutcome::Delivered)
            }
            KnownMessageVerificationOutcome::Forbidden => {
                self.hold_ambiguous(record, "MESSAGE_VERIFICATION_FORBIDDEN")
            }
            KnownMessageVerificationOutcome::IdentityMismatch => {
                self.hold_ambiguous(record, "MESSAGE_IDENTITY_MISMATCH")
            }
            KnownMessageVerificationOutcome::NotFound
            | KnownMessageVerificationOutcome::TransientFailure
            | KnownMessageVerificationOutcome::Inconclusive
            | KnownMessageVerificationOutcome::Noop => {
                self.hold_if_reconciliation_exhausted(record)
            }
        }
    }

    fn hold_if_reconciliation_exhausted(
        &self,
        record: TerminalDeliveryRecordV1,
    ) -> Result<AmbiguousReconciliationOutcome, TerminalDeliveryError> {
        let authoritative = self
            .repo
            .get(&record.terminal_delivery_key)?
            .ok_or_else(|| TerminalDeliveryError::Storage("lost delivery record".into()))?;
        if authoritative.state != TerminalDeliveryState::Ambiguous {
            return Ok(AmbiguousReconciliationOutcome::Noop);
        }
        if self
            .repo
            .ambiguous_reconciliation_read_attempts(&authoritative.terminal_delivery_key)?
            < MAX_AMBIGUOUS_RECONCILIATION_ATTEMPTS
        {
            return Ok(AmbiguousReconciliationOutcome::StillAmbiguous);
        }
        self.hold_ambiguous(authoritative, "RECONCILIATION_EXHAUSTED")
    }

    fn hold_ambiguous(
        &self,
        record: TerminalDeliveryRecordV1,
        reason: &str,
    ) -> Result<AmbiguousReconciliationOutcome, TerminalDeliveryError> {
        match self.repo.transition(
            &record.terminal_delivery_key,
            record.state_revision,
            TerminalDeliveryState::OperatorHold,
            TransitionUpdate {
                operator_hold_reason: Some(Some(reason.to_owned())),
                ..Default::default()
            },
            reason,
        ) {
            Ok(_) => Ok(AmbiguousReconciliationOutcome::Held),
            Err(TerminalDeliveryError::RevisionConflict { .. }) => {
                let authoritative = self
                    .repo
                    .get(&record.terminal_delivery_key)?
                    .ok_or_else(|| TerminalDeliveryError::Storage("lost delivery record".into()))?;
                if authoritative.state == TerminalDeliveryState::OperatorHold {
                    Ok(AmbiguousReconciliationOutcome::Held)
                } else {
                    Ok(AmbiguousReconciliationOutcome::Noop)
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Convert a stale in-flight claim into an explicit ambiguous outcome.
    /// This recovery path never sends and never reads Runtime: a stale send
    /// may already have reached Discord, so it must not be retried here.
    pub fn recover_stale_delivering(
        &self,
        record: TerminalDeliveryRecordV1,
        stale_before: chrono::DateTime<Utc>,
    ) -> Result<TerminalDeliveryRecordV1, TerminalDeliveryError> {
        validate_record_integrity(&record)?;
        if record.state != TerminalDeliveryState::Delivering {
            return Ok(record);
        }
        let missing_started_at = record.delivery_started_at.is_none();
        if !missing_started_at
            && record
                .delivery_started_at
                .is_some_and(|started_at| started_at > stale_before)
        {
            return Ok(record);
        }
        let lease_token = record
            .delivery_lease_token
            .as_deref()
            .ok_or(TerminalDeliveryError::MissingDeliveryLeaseToken)?;
        let reason = if missing_started_at {
            "DELIVERING_MISSING_START_TIME_AMBIGUOUS"
        } else {
            "STALE_DELIVERING_DECLARED_AMBIGUOUS"
        };
        match self.repo.transition_with_lease(
            &record.terminal_delivery_key,
            record.state_revision,
            lease_token,
            TerminalDeliveryState::Ambiguous,
            TransitionUpdate {
                next_attempt_at: Some(None),
                delivery_lease_token: Some(None),
                ..Default::default()
            },
            reason,
        ) {
            Ok(recovered) => Ok(recovered),
            Err(
                error @ (TerminalDeliveryError::RevisionConflict { .. }
                | TerminalDeliveryError::LeaseTokenConflict),
            ) => {
                let authoritative = self
                    .repo
                    .get(&record.terminal_delivery_key)?
                    .ok_or_else(|| TerminalDeliveryError::Storage("lost delivery record".into()))?;
                if authoritative.state != TerminalDeliveryState::Delivering {
                    Ok(authoritative)
                } else {
                    Err(error)
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Reconcile one durable terminal-delivery record after restart without
    /// consulting Runtime or rematerializing the persisted payload.
    ///
    /// `PENDING_RESULT` stops after its durable READY transition. This gives
    /// each reconciliation invocation one durable action and leaves any send
    /// to a later candidate scan.
    pub async fn reconcile_record(
        &self,
        record: TerminalDeliveryRecordV1,
        now: chrono::DateTime<Utc>,
        stale_before: chrono::DateTime<Utc>,
        sender: &dyn DiscordTerminalDeliverySender,
    ) -> Result<ReconciliationOutcome, TerminalDeliveryError> {
        match record.state {
            TerminalDeliveryState::PendingResult => {
                self.recover_pending_result(record)?;
                Ok(ReconciliationOutcome::Advanced)
            }
            TerminalDeliveryState::ReadyToDeliver => {
                let channel = channel_from_record(&record);
                let recovered = self.recover_ready_or_retry(record, channel, sender).await?;
                Ok(if recovered.state == TerminalDeliveryState::Delivered {
                    ReconciliationOutcome::Delivered
                } else {
                    ReconciliationOutcome::Advanced
                })
            }
            TerminalDeliveryState::RetryScheduled => {
                if record.next_attempt_at.is_none_or(|due_at| due_at > now) {
                    return Ok(ReconciliationOutcome::Noop);
                }
                let channel = channel_from_record(&record);
                let recovered = self.recover_ready_or_retry(record, channel, sender).await?;
                Ok(if recovered.state == TerminalDeliveryState::Delivered {
                    ReconciliationOutcome::Delivered
                } else {
                    ReconciliationOutcome::Advanced
                })
            }
            TerminalDeliveryState::Delivering => {
                let before = record.state;
                let recovered = self.recover_stale_delivering(record, stale_before)?;
                Ok(if recovered.state != before {
                    ReconciliationOutcome::Advanced
                } else {
                    ReconciliationOutcome::Noop
                })
            }
            TerminalDeliveryState::Ambiguous
            | TerminalDeliveryState::OperatorHold
            | TerminalDeliveryState::Delivered
            | TerminalDeliveryState::PermanentRejected => Ok(ReconciliationOutcome::Noop),
        }
    }

    /// Reconcile at most `limit` repository candidates sequentially. This is
    /// intentionally a caller-driven, bounded helper: it creates no task,
    /// timer, or periodic background loop.
    pub async fn reconcile_candidates(
        &self,
        now: chrono::DateTime<Utc>,
        stale_before: chrono::DateTime<Utc>,
        limit: usize,
        sender: &dyn DiscordTerminalDeliverySender,
    ) -> Result<ReconciliationBatchSummary, TerminalDeliveryError> {
        let candidates = self
            .repo
            .list_reconciliation_candidates(now, stale_before, limit)?;
        let mut summary = ReconciliationBatchSummary::default();
        for record in candidates {
            let key = record.terminal_delivery_key.clone();
            let outcome = self
                .reconcile_record(record, now, stale_before, sender)
                .await;
            summary.processed += 1;
            match &outcome {
                Ok(ReconciliationOutcome::Advanced) => summary.advanced += 1,
                Ok(ReconciliationOutcome::Delivered) => summary.delivered += 1,
                Ok(ReconciliationOutcome::Noop) => summary.no_op += 1,
                Err(_) => summary.failed += 1,
            }
            summary.results.push(ReconciliationRecordResult {
                terminal_delivery_key: key,
                outcome,
            });
        }
        Ok(summary)
    }

    async fn deliver_ready(
        &self,
        record: TerminalDeliveryRecordV1,
        channel: ChannelRef,
        sender: &dyn DiscordTerminalDeliverySender,
    ) -> Result<TerminalDeliveryRecordV1, TerminalDeliveryError> {
        if !matches!(
            record.state,
            TerminalDeliveryState::ReadyToDeliver | TerminalDeliveryState::RetryScheduled
        ) {
            return Ok(record);
        }
        const MAX_CLAIM_RETRIES: usize = 3;
        let mut current_record = record;
        let mut attempts = 0;
        let claimed = loop {
            attempts += 1;
            let attempt = current_record.attempt_count + 1;
            match self.repo.transition(
                &current_record.terminal_delivery_key,
                current_record.state_revision,
                TerminalDeliveryState::Delivering,
                TransitionUpdate {
                    attempt_count: Some(attempt),
                    delivery_lease_token: Some(Some(uuid::Uuid::new_v4().to_string())),
                    delivery_started_at: Some(Some(Utc::now())),
                    ..Default::default()
                },
                "DELIVERY_CLAIMED",
            ) {
                Ok(value) => break value,
                Err(
                    TerminalDeliveryError::RevisionConflict { .. }
                    | TerminalDeliveryError::ImmutableTerminalState(_),
                ) => {
                    let authoritative = self
                        .repo
                        .get(&current_record.terminal_delivery_key)?
                        .ok_or_else(|| {
                            TerminalDeliveryError::Storage("lost delivery record".into())
                        })?;
                    if !matches!(
                        authoritative.state,
                        TerminalDeliveryState::ReadyToDeliver
                            | TerminalDeliveryState::RetryScheduled
                    ) {
                        return Ok(authoritative);
                    }
                    if attempts >= MAX_CLAIM_RETRIES {
                        return Err(TerminalDeliveryError::RevisionConflict {
                            expected: current_record.state_revision,
                            actual: authoritative.state_revision,
                        });
                    }
                    current_record = authoritative;
                }
                Err(error) if error.is_contention() => {
                    if let Ok(Some(authoritative)) =
                        self.repo.get(&current_record.terminal_delivery_key)
                    {
                        if !matches!(
                            authoritative.state,
                            TerminalDeliveryState::ReadyToDeliver
                                | TerminalDeliveryState::RetryScheduled
                        ) {
                            return Ok(authoritative);
                        }
                        current_record = authoritative;
                    }
                    if attempts >= MAX_CLAIM_RETRIES {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        };
        let content = terminal_content_for_discord(&claimed.terminal_payload);
        let attempt = claimed.attempt_count;
        let outcome = sender.send_terminal(&channel, content).await;
        let lease_token = claimed
            .delivery_lease_token
            .as_deref()
            .ok_or(TerminalDeliveryError::InvalidInput("delivery_lease_token"))?;
        match outcome {
            DiscordSendResult::DefiniteSuccess(message_id) => {
                let evidenced = self.repo.persist_accepted_message_id(
                    &claimed.terminal_delivery_key,
                    claimed.state_revision,
                    lease_token,
                    &message_id,
                )?;
                self.repo.transition_with_lease(
                    &evidenced.terminal_delivery_key,
                    evidenced.state_revision,
                    lease_token,
                    TerminalDeliveryState::Delivered,
                    TransitionUpdate {
                        delivery_lease_token: Some(None),
                        ..Default::default()
                    },
                    "DISCORD_DELIVERED",
                )
            }
            DiscordSendResult::DefinitePermanentFailure { classification } => {
                self.repo.transition_with_lease(
                    &claimed.terminal_delivery_key,
                    claimed.state_revision,
                    lease_token,
                    TerminalDeliveryState::PermanentRejected,
                    TransitionUpdate {
                        last_failure_classification: Some(Some(classification.into())),
                        delivery_lease_token: Some(None),
                        ..Default::default()
                    },
                    "DISCORD_PERMANENT_REJECTED",
                )
            }
            DiscordSendResult::Ambiguous { classification } => self.repo.transition_with_lease(
                &claimed.terminal_delivery_key,
                claimed.state_revision,
                lease_token,
                TerminalDeliveryState::Ambiguous,
                TransitionUpdate {
                    last_failure_classification: Some(Some(classification.into())),
                    delivery_lease_token: Some(None),
                    ..Default::default()
                },
                "DISCORD_AMBIGUOUS",
            ),
            DiscordSendResult::DefiniteTransientFailure { classification }
                if attempt >= MAX_EXTERNAL_ATTEMPTS =>
            {
                let retry_scheduled = self.repo.transition_with_lease(
                    &claimed.terminal_delivery_key,
                    claimed.state_revision,
                    lease_token,
                    TerminalDeliveryState::RetryScheduled,
                    TransitionUpdate {
                        last_failure_classification: Some(Some(classification.into())),
                        delivery_lease_token: Some(None),
                        ..Default::default()
                    },
                    "DISCORD_RETRY_EXHAUSTED",
                )?;
                self.repo.transition(
                    &retry_scheduled.terminal_delivery_key,
                    retry_scheduled.state_revision,
                    TerminalDeliveryState::OperatorHold,
                    TransitionUpdate {
                        operator_hold_reason: Some(Some(
                            "RETRY_EXHAUSTED_DEFINITE_TRANSIENT".into(),
                        )),
                        ..Default::default()
                    },
                    "RETRY_EXHAUSTED_DEFINITE_TRANSIENT",
                )
            }
            DiscordSendResult::DefiniteTransientFailure { classification } => {
                let seconds = retry_delay_secs(attempt);
                self.repo.transition_with_lease(
                    &claimed.terminal_delivery_key,
                    claimed.state_revision,
                    lease_token,
                    TerminalDeliveryState::RetryScheduled,
                    TransitionUpdate {
                        next_attempt_at: Some(Some(Utc::now() + Duration::seconds(seconds as i64))),
                        last_failure_classification: Some(Some(classification.into())),
                        delivery_lease_token: Some(None),
                        ..Default::default()
                    },
                    "DISCORD_RETRY_SCHEDULED",
                )
            }
        }
    }
}

fn channel_from_record(record: &TerminalDeliveryRecordV1) -> ChannelRef {
    ChannelRef {
        platform: record.platform.clone(),
        channel_id: record.channel_id.clone(),
        thread_id: record.thread_id.clone(),
        parent_id: None,
        origin_event_id: None,
    }
}

#[async_trait]
impl TerminalDeliveryPort for TerminalDeliveryWorker {
    async fn capture_and_deliver(
        &self,
        metadata: AcpTerminalMetadata,
        channel: ChannelRef,
        inbound_message_id: &str,
        sender: &dyn DiscordTerminalDeliverySender,
    ) -> Result<TerminalDeliveryRecordV1, LookupError> {
        TerminalDeliveryWorker::capture_and_deliver(
            self,
            metadata,
            channel,
            inbound_message_id,
            sender,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Barrier;
    use std::thread;
    use tempfile::TempDir;

    #[tokio::test(start_paused = true)]
    async fn terminal_send_timeout_is_ambiguous_without_a_real_sleep() {
        let outcome = tokio::spawn(await_terminal_send_with_timeout(std::future::pending::<
            anyhow::Result<crate::adapter::MessageRef>,
        >()));
        tokio::time::advance(TERMINAL_SEND_TIMEOUT + std::time::Duration::from_secs(1)).await;
        assert!(outcome.await.unwrap().is_err());
    }

    #[test]
    fn capture_requires_canonical_matching_runtime_identity() {
        let ok = json!({"metadata":{"runtime_run_id":"runtime-1", "run_id":"runtime-1", "workflow_run_id":"workflow-never-a-fallback"}});
        assert_eq!(
            capture_terminal_metadata(&ok, 7, "s")
                .unwrap()
                .runtime_run_id,
            "runtime-1"
        );
        assert_eq!(
            capture_terminal_metadata(
                &json!({"metadata":{"runtime_run_id":"a", "run_id":"b"}}),
                7,
                "s"
            ),
            Err(CaptureError::RunIdMismatch)
        );
        assert_eq!(
            capture_terminal_metadata(
                &json!({"metadata":{"workflow_run_id":"not-runtime"}}),
                7,
                "s"
            ),
            Err(CaptureError::MissingRuntimeRunId)
        );
    }

    #[test]
    fn materialization_redacts_hostile_failure_snapshot() {
        let payload = materialize_terminal_payload(
            &json!({"terminal_status":"failed", "terminal_result":{"error":"Bearer abc /tmp/x SELECT * traceback"}}),
        );
        assert_eq!(payload, json!({"content": SAFE_FAILED_MESSAGE}));
        assert_eq!(
            materialize_terminal_payload(&json!({"terminal_status":"cancelled"})),
            json!({"content": SAFE_CANCELLED_MESSAGE})
        );
    }

    #[test]
    fn terminal_discord_content_is_the_persisted_content_without_formatting() {
        assert_eq!(
            terminal_content_for_discord(&json!({"content":"**exact**\n<@1>"})),
            "**exact**\n<@1>"
        );
        assert_eq!(
            terminal_content_for_discord(&json!({"not_content":"ignored"})),
            SAFE_FAILED_MESSAGE
        );
    }

    struct Lookup(Value);
    #[async_trait]
    impl TerminalResultLookup for Lookup {
        async fn get_terminal_result(&self, _: &str) -> Result<Value, LookupError> {
            Ok(self.0.clone())
        }
    }
    struct RecordingLookup {
        value: Value,
        calls: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl TerminalResultLookup for RecordingLookup {
        async fn get_terminal_result(&self, runtime_run_id: &str) -> Result<Value, LookupError> {
            assert_eq!(runtime_run_id, "r");
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.value.clone())
        }
    }
    struct Outcome(DiscordSendResult);
    #[async_trait]
    impl DiscordTerminalDeliverySender for Outcome {
        async fn send_terminal(&self, _: &ChannelRef, _: &str) -> DiscordSendResult {
            self.0.clone()
        }
    }

    struct CountingOutcome {
        outcome: DiscordSendResult,
        calls: Arc<AtomicUsize>,
    }

    struct RecordedOutcomes {
        calls: Arc<AtomicUsize>,
        contents: Arc<std::sync::Mutex<Vec<String>>>,
    }
    #[async_trait]
    impl DiscordTerminalDeliverySender for RecordedOutcomes {
        async fn send_terminal(&self, _: &ChannelRef, content: &str) -> DiscordSendResult {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            self.contents.lock().unwrap().push(content.to_owned());
            if call == 0 {
                DiscordSendResult::DefiniteTransientFailure {
                    classification: "TEST_TRANSIENT",
                }
            } else {
                DiscordSendResult::DefiniteSuccess("message-2".into())
            }
        }
    }
    #[async_trait]
    impl DiscordTerminalDeliverySender for CountingOutcome {
        async fn send_terminal(&self, _: &ChannelRef, _: &str) -> DiscordSendResult {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.outcome.clone()
        }
    }

    struct RecordingReader {
        expected_author_id: String,
        outcome: DiscordTerminalDeliveryReadResult,
        calls: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl DiscordTerminalDeliveryReader for RecordingReader {
        fn expected_author_id(&self) -> &str {
            &self.expected_author_id
        }

        async fn get_message(
            &self,
            channel_id: &str,
            message_id: &str,
        ) -> DiscordTerminalDeliveryReadResult {
            assert_eq!(channel_id, "1");
            assert_eq!(message_id, "discord-known");
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.outcome.clone()
        }
    }

    fn metadata() -> AcpTerminalMetadata {
        AcpTerminalMetadata {
            runtime_run_id: "r".into(),
            legacy_run_id: Some("r".into()),
            acp_request_id: "q".into(),
            acp_session_id: "s".into(),
            workflow_run_id: None,
            conversation_id: "c".into(),
        }
    }

    fn channel() -> ChannelRef {
        ChannelRef {
            platform: "discord".into(),
            channel_id: "1".into(),
            thread_id: None,
            parent_id: None,
            origin_event_id: Some("42".into()),
        }
    }

    fn delivering_record(
        repo: &TerminalDeliveryRepository,
        suffix: &str,
        started_at: Option<chrono::DateTime<Utc>>,
        lease_token: Option<&str>,
    ) -> TerminalDeliveryRecordV1 {
        let pending = repo
            .create_or_reuse(crate::terminal_delivery::NewTerminalDeliveryRecord {
                openab_inbound_turn_id: format!("discord:{suffix}"),
                platform: "discord".into(),
                channel_id: "1".into(),
                thread_id: None,
                acp_request_id: "q".into(),
                acp_session_id: "s".into(),
                runtime_run_id: format!("runtime:{suffix}"),
                workflow_run_id: None,
                conversation_id: "c".into(),
                response_sequence: 0,
                terminal_payload: json!({"content":"persisted"}),
            })
            .unwrap();
        let ready = repo
            .transition(
                &pending.terminal_delivery_key,
                pending.state_revision,
                TerminalDeliveryState::ReadyToDeliver,
                TransitionUpdate::default(),
                "TEST_READY",
            )
            .unwrap();
        repo.transition(
            &ready.terminal_delivery_key,
            ready.state_revision,
            TerminalDeliveryState::Delivering,
            TransitionUpdate {
                delivery_lease_token: Some(lease_token.map(str::to_owned)),
                delivery_started_at: Some(started_at),
                next_attempt_at: Some(Some(Utc::now() + Duration::seconds(60))),
                ..Default::default()
            },
            "TEST_DELIVERING",
        )
        .unwrap()
    }

    fn ambiguous_record_with_known_message(
        repo: &TerminalDeliveryRepository,
    ) -> TerminalDeliveryRecordV1 {
        let pending = repo
            .create_or_reuse(crate::terminal_delivery::NewTerminalDeliveryRecord {
                openab_inbound_turn_id: "discord:known-message".into(),
                platform: "discord".into(),
                channel_id: "1".into(),
                thread_id: None,
                acp_request_id: "q".into(),
                acp_session_id: "s".into(),
                runtime_run_id: "runtime:known-message".into(),
                workflow_run_id: None,
                conversation_id: "c".into(),
                response_sequence: 0,
                terminal_payload: json!({"content":"persisted"}),
            })
            .unwrap();
        let ready = repo
            .transition(
                &pending.terminal_delivery_key,
                pending.state_revision,
                TerminalDeliveryState::ReadyToDeliver,
                TransitionUpdate::default(),
                "TEST_READY",
            )
            .unwrap();
        let delivering = repo
            .transition(
                &ready.terminal_delivery_key,
                ready.state_revision,
                TerminalDeliveryState::Delivering,
                TransitionUpdate {
                    discord_message_id: Some("discord-known".into()),
                    ..Default::default()
                },
                "TEST_KNOWN_MESSAGE",
            )
            .unwrap();
        repo.transition(
            &delivering.terminal_delivery_key,
            delivering.state_revision,
            TerminalDeliveryState::Ambiguous,
            TransitionUpdate::default(),
            "TEST_AMBIGUOUS",
        )
        .unwrap()
    }

    fn found_terminal_message() -> DiscordTerminalDeliveryReadResult {
        DiscordTerminalDeliveryReadResult::Found(DiscordTerminalMessage {
            message_id: "discord-known".into(),
            channel_id: "1".into(),
            author_id: "bot-1".into(),
            content: "persisted".into(),
        })
    }

    fn pending_record(repo: &TerminalDeliveryRepository, suffix: &str) -> TerminalDeliveryRecordV1 {
        repo.create_or_reuse(crate::terminal_delivery::NewTerminalDeliveryRecord {
            openab_inbound_turn_id: format!("discord:manager:{suffix}"),
            platform: "discord".into(),
            channel_id: "1".into(),
            thread_id: None,
            acp_request_id: "q".into(),
            acp_session_id: "s".into(),
            runtime_run_id: format!("runtime:manager:{suffix}"),
            workflow_run_id: None,
            conversation_id: "c".into(),
            response_sequence: 0,
            terminal_payload: json!({"content":"persisted"}),
        })
        .unwrap()
    }

    fn reconciliation_manager(
        repo: Arc<TerminalDeliveryRepository>,
        sender: Arc<dyn DiscordTerminalDeliverySender>,
        reader: Arc<dyn DiscordTerminalDeliveryReader>,
    ) -> TerminalDeliveryReconciliationManager {
        let worker = Arc::new(TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(Lookup(json!({}))),
        ));
        TerminalDeliveryReconciliationManager::new(repo, worker, sender, reader)
    }

    #[tokio::test]
    async fn reconciliation_manager_routes_mixed_candidates_without_crossing_discord_authorities() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let pending = pending_record(&repo, "pending");
        let ready_source = pending_record(&repo, "ready");
        let ready = repo
            .transition(
                &ready_source.terminal_delivery_key,
                ready_source.state_revision,
                TerminalDeliveryState::ReadyToDeliver,
                TransitionUpdate::default(),
                "TEST_READY",
            )
            .unwrap();
        let stale = delivering_record(
            &repo,
            "stale",
            Some(Utc::now() - Duration::seconds(STALE_DELIVERING_AFTER_SECS + 1)),
            Some("lease"),
        );
        let ambiguous = ambiguous_record_with_known_message(&repo);
        let sender_calls = Arc::new(AtomicUsize::new(0));
        let reader_calls = Arc::new(AtomicUsize::new(0));
        let manager = reconciliation_manager(
            repo.clone(),
            Arc::new(CountingOutcome {
                outcome: DiscordSendResult::DefiniteSuccess("sent".into()),
                calls: sender_calls.clone(),
            }),
            Arc::new(RecordingReader {
                expected_author_id: "bot-1".into(),
                outcome: found_terminal_message(),
                calls: reader_calls.clone(),
            }),
        );

        let summary = manager.reconcile_once(Utc::now()).await.unwrap();
        assert_eq!(summary.candidates, 4);
        assert_eq!(summary.advanced, 2);
        assert_eq!(summary.delivered, 2);
        assert_eq!(
            sender_calls.load(Ordering::SeqCst),
            1,
            "only READY was sent"
        );
        assert_eq!(
            reader_calls.load(Ordering::SeqCst),
            1,
            "only AMBIGUOUS was read"
        );
        assert_eq!(
            repo.get(&pending.terminal_delivery_key)
                .unwrap()
                .unwrap()
                .state,
            TerminalDeliveryState::ReadyToDeliver
        );
        assert_eq!(
            repo.get(&ready.terminal_delivery_key)
                .unwrap()
                .unwrap()
                .state,
            TerminalDeliveryState::Delivered
        );
        assert_eq!(
            repo.get(&stale.terminal_delivery_key)
                .unwrap()
                .unwrap()
                .state,
            TerminalDeliveryState::Ambiguous
        );
        assert_eq!(
            repo.get(&ambiguous.terminal_delivery_key)
                .unwrap()
                .unwrap()
                .state,
            TerminalDeliveryState::Delivered
        );
    }

    #[tokio::test(start_paused = true)]
    async fn reconciliation_manager_runs_startup_then_periodically_and_stops_on_shutdown() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let pending = pending_record(&repo, "periodic");
        let sender_calls = Arc::new(AtomicUsize::new(0));
        let manager = Arc::new(reconciliation_manager(
            repo.clone(),
            Arc::new(CountingOutcome {
                outcome: DiscordSendResult::DefiniteSuccess("sent".into()),
                calls: sender_calls.clone(),
            }),
            Arc::new(RecordingReader {
                expected_author_id: "bot-1".into(),
                outcome: found_terminal_message(),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
        ));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = manager.spawn(shutdown_rx);
        tokio::task::yield_now().await;
        assert_eq!(
            repo.get(&pending.terminal_delivery_key)
                .unwrap()
                .unwrap()
                .state,
            TerminalDeliveryState::ReadyToDeliver
        );
        assert_eq!(sender_calls.load(Ordering::SeqCst), 0);
        tokio::time::advance(TERMINAL_RECONCILIATION_INTERVAL).await;
        tokio::task::yield_now().await;
        assert_eq!(sender_calls.load(Ordering::SeqCst), 1);
        shutdown_tx.send(true).unwrap();
        handle.await.unwrap();
        tokio::time::advance(TERMINAL_RECONCILIATION_INTERVAL * 2).await;
        assert_eq!(sender_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reconciliation_manager_limits_each_scan_and_later_scans_progress_remaining_records() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        for i in 0..=TERMINAL_RECONCILIATION_BATCH_LIMIT {
            let pending = pending_record(&repo, &format!("batch-{i}"));
            repo.transition(
                &pending.terminal_delivery_key,
                pending.state_revision,
                TerminalDeliveryState::ReadyToDeliver,
                TransitionUpdate::default(),
                "TEST_READY",
            )
            .unwrap();
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let manager = reconciliation_manager(
            repo.clone(),
            Arc::new(CountingOutcome {
                outcome: DiscordSendResult::DefiniteSuccess("sent".into()),
                calls: calls.clone(),
            }),
            Arc::new(RecordingReader {
                expected_author_id: "bot-1".into(),
                outcome: found_terminal_message(),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
        );
        let first = manager.reconcile_once(Utc::now()).await.unwrap();
        assert_eq!(first.candidates, TERMINAL_RECONCILIATION_BATCH_LIMIT);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            TERMINAL_RECONCILIATION_BATCH_LIMIT
        );
        let second = manager.reconcile_once(Utc::now()).await.unwrap();
        assert_eq!(second.candidates, 1);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            TERMINAL_RECONCILIATION_BATCH_LIMIT + 1
        );
    }

    #[tokio::test]
    async fn reconciliation_manager_isolates_record_failure_and_concurrent_managers_claim_one_send()
    {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        delivering_record(&repo, "missing-lease", None, None);
        let ready = pending_record(&repo, "concurrent-ready");
        let ready = repo
            .transition(
                &ready.terminal_delivery_key,
                ready.state_revision,
                TerminalDeliveryState::ReadyToDeliver,
                TransitionUpdate::default(),
                "TEST_READY",
            )
            .unwrap();
        let ambiguous = ambiguous_record_with_known_message(&repo);
        let sends = Arc::new(AtomicUsize::new(0));
        let reads = Arc::new(AtomicUsize::new(0));
        let make_manager = || {
            reconciliation_manager(
                repo.clone(),
                Arc::new(CountingOutcome {
                    outcome: DiscordSendResult::DefiniteSuccess("sent".into()),
                    calls: sends.clone(),
                }),
                Arc::new(RecordingReader {
                    expected_author_id: "bot-1".into(),
                    outcome: found_terminal_message(),
                    calls: reads.clone(),
                }),
            )
        };
        let one = Arc::new(make_manager());
        let two = Arc::new(make_manager());
        let (a, b) = tokio::join!(
            one.reconcile_once(Utc::now()),
            two.reconcile_once(Utc::now())
        );
        assert_eq!(
            a.unwrap().failed + b.unwrap().failed,
            2,
            "unfenced record is reported by both scans"
        );
        assert_eq!(
            sends.load(Ordering::SeqCst),
            1,
            "CAS permits one READY sender"
        );
        assert!(
            reads.load(Ordering::SeqCst) <= 2,
            "durable read budget remains bounded"
        );
        assert_eq!(
            repo.get(&ready.terminal_delivery_key)
                .unwrap()
                .unwrap()
                .state,
            TerminalDeliveryState::Delivered
        );
        assert_eq!(
            repo.get(&ambiguous.terminal_delivery_key)
                .unwrap()
                .unwrap()
                .state,
            TerminalDeliveryState::Delivered
        );
    }

    #[tokio::test]
    async fn known_message_positive_proof_delivers_without_sender_or_runtime_lookup() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let lookup_calls = Arc::new(AtomicUsize::new(0));
        let worker = TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(RecordingLookup {
                value: json!({}),
                calls: lookup_calls.clone(),
            }),
        );
        let ambiguous = ambiguous_record_with_known_message(&repo);
        let reader_calls = Arc::new(AtomicUsize::new(0));
        let reader = RecordingReader {
            expected_author_id: "bot-1".into(),
            outcome: found_terminal_message(),
            calls: reader_calls.clone(),
        };

        assert_eq!(
            worker
                .reconcile_ambiguous_with_known_message(ambiguous.clone(), &reader)
                .await
                .unwrap(),
            KnownMessageVerificationOutcome::Delivered
        );
        let delivered = repo.get(&ambiguous.terminal_delivery_key).unwrap().unwrap();
        assert_eq!(delivered.state, TerminalDeliveryState::Delivered);
        assert_eq!(
            delivered.discord_message_id.as_deref(),
            Some("discord-known")
        );
        assert_eq!(lookup_calls.load(Ordering::SeqCst), 0);
        assert_eq!(reader_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            repo.events(&ambiguous.terminal_delivery_key)
                .unwrap()
                .last()
                .unwrap()
                .safe_reason_code,
            "DELIVERY_VERIFIED_BY_MESSAGE_ID"
        );
    }

    #[tokio::test]
    async fn known_message_nonproof_outcomes_keep_ambiguous_without_sender_or_runtime_lookup() {
        let scenarios = vec![
            (
                DiscordTerminalDeliveryReadResult::NotFound,
                KnownMessageVerificationOutcome::NotFound,
            ),
            (
                DiscordTerminalDeliveryReadResult::Forbidden,
                KnownMessageVerificationOutcome::Forbidden,
            ),
            (
                DiscordTerminalDeliveryReadResult::TransientFailure {
                    classification: "DISCORD_MESSAGE_VERIFICATION_RATE_LIMITED",
                },
                KnownMessageVerificationOutcome::TransientFailure,
            ),
            (
                DiscordTerminalDeliveryReadResult::Inconclusive {
                    classification: "DISCORD_MESSAGE_VERIFICATION_TRANSPORT",
                },
                KnownMessageVerificationOutcome::Inconclusive,
            ),
        ];
        for (outcome, expected) in scenarios {
            let tmp = TempDir::new().unwrap();
            let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
            let lookup_calls = Arc::new(AtomicUsize::new(0));
            let worker = TerminalDeliveryWorker::new(
                repo.clone(),
                Arc::new(RecordingLookup {
                    value: json!({}),
                    calls: lookup_calls.clone(),
                }),
            );
            let ambiguous = ambiguous_record_with_known_message(&repo);
            let reader = RecordingReader {
                expected_author_id: "bot-1".into(),
                outcome,
                calls: Arc::new(AtomicUsize::new(0)),
            };
            assert_eq!(
                worker
                    .reconcile_ambiguous_with_known_message(ambiguous.clone(), &reader)
                    .await
                    .unwrap(),
                expected
            );
            assert_eq!(
                repo.get(&ambiguous.terminal_delivery_key)
                    .unwrap()
                    .unwrap()
                    .state,
                TerminalDeliveryState::Ambiguous
            );
            assert_eq!(lookup_calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn known_message_identity_mismatches_and_missing_id_are_safe_noops() {
        for mismatch in [
            DiscordTerminalMessage {
                message_id: "wrong".into(),
                channel_id: "1".into(),
                author_id: "bot-1".into(),
                content: "persisted".into(),
            },
            DiscordTerminalMessage {
                message_id: "discord-known".into(),
                channel_id: "wrong".into(),
                author_id: "bot-1".into(),
                content: "persisted".into(),
            },
            DiscordTerminalMessage {
                message_id: "discord-known".into(),
                channel_id: "1".into(),
                author_id: "wrong".into(),
                content: "persisted".into(),
            },
            DiscordTerminalMessage {
                message_id: "discord-known".into(),
                channel_id: "1".into(),
                author_id: "bot-1".into(),
                content: "wrong".into(),
            },
        ] {
            let tmp = TempDir::new().unwrap();
            let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
            let worker = TerminalDeliveryWorker::new(repo.clone(), Arc::new(Lookup(json!({}))));
            let ambiguous = ambiguous_record_with_known_message(&repo);
            let reader = RecordingReader {
                expected_author_id: "bot-1".into(),
                outcome: DiscordTerminalDeliveryReadResult::Found(mismatch),
                calls: Arc::new(AtomicUsize::new(0)),
            };
            assert_eq!(
                worker
                    .reconcile_ambiguous_with_known_message(ambiguous.clone(), &reader)
                    .await
                    .unwrap(),
                KnownMessageVerificationOutcome::IdentityMismatch
            );
            assert_eq!(
                repo.get(&ambiguous.terminal_delivery_key)
                    .unwrap()
                    .unwrap()
                    .state,
                TerminalDeliveryState::Ambiguous
            );
        }

        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = TerminalDeliveryWorker::new(repo.clone(), Arc::new(Lookup(json!({}))));
        let mut no_id = ambiguous_record_with_known_message(&repo);
        no_id.discord_message_id = None;
        let reader_calls = Arc::new(AtomicUsize::new(0));
        let reader = RecordingReader {
            expected_author_id: "bot-1".into(),
            outcome: found_terminal_message(),
            calls: reader_calls.clone(),
        };
        assert_eq!(
            worker
                .reconcile_ambiguous_with_known_message(no_id, &reader)
                .await
                .unwrap(),
            KnownMessageVerificationOutcome::Noop
        );
        assert_eq!(reader_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn concurrent_known_message_verifiers_have_one_transition_winner_and_one_safe_success() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = Arc::new(TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(Lookup(json!({}))),
        ));
        let ambiguous = ambiguous_record_with_known_message(&repo);
        let key = ambiguous.terminal_delivery_key.clone();
        let reader_calls = Arc::new(AtomicUsize::new(0));
        let reader = Arc::new(RecordingReader {
            expected_author_id: "bot-1".into(),
            outcome: found_terminal_message(),
            calls: reader_calls.clone(),
        });
        let first = {
            let worker = worker.clone();
            let reader = reader.clone();
            let record = ambiguous.clone();
            tokio::spawn(async move {
                worker
                    .reconcile_ambiguous_with_known_message(record, reader.as_ref())
                    .await
            })
        };
        let second = {
            let worker = worker.clone();
            let reader = reader.clone();
            tokio::spawn(async move {
                worker
                    .reconcile_ambiguous_with_known_message(ambiguous, reader.as_ref())
                    .await
            })
        };
        assert_eq!(
            first.await.unwrap().unwrap(),
            KnownMessageVerificationOutcome::Delivered
        );
        assert_eq!(
            second.await.unwrap().unwrap(),
            KnownMessageVerificationOutcome::Delivered
        );
        assert_eq!(reader_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            repo.events(&key)
                .unwrap()
                .iter()
                .filter(|event| event.safe_reason_code == "DELIVERY_VERIFIED_BY_MESSAGE_ID")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn bounded_ambiguous_reconciliation_persists_attempts_and_holds_on_exhaustion() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let lookup_calls = Arc::new(AtomicUsize::new(0));
        let worker = TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(RecordingLookup {
                value: json!({}),
                calls: lookup_calls.clone(),
            }),
        );
        let original = ambiguous_record_with_known_message(&repo);
        let key = original.terminal_delivery_key.clone();
        let payload = original.terminal_payload.clone();
        let digest = original.terminal_payload_digest.clone();
        let delivery_attempts = original.attempt_count;
        let reader_calls = Arc::new(AtomicUsize::new(0));

        for expected_attempt in 1..MAX_AMBIGUOUS_RECONCILIATION_ATTEMPTS {
            let current = repo.get(&key).unwrap().unwrap();
            assert_eq!(
                worker
                    .reconcile_ambiguous(
                        current,
                        &RecordingReader {
                            expected_author_id: "bot-1".into(),
                            outcome: DiscordTerminalDeliveryReadResult::TransientFailure {
                                classification: "DISCORD_MESSAGE_VERIFICATION_TRANSPORT",
                            },
                            calls: reader_calls.clone(),
                        },
                    )
                    .await
                    .unwrap(),
                AmbiguousReconciliationOutcome::StillAmbiguous
            );
            assert_eq!(
                repo.ambiguous_reconciliation_read_attempts(&key).unwrap(),
                expected_attempt
            );
        }

        // Reopen the durable repository to prove the next worker observes the
        // prior budget rather than resetting a process-local counter.
        let reopened = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let next_worker =
            TerminalDeliveryWorker::new(reopened.clone(), Arc::new(Lookup(json!({}))));
        let current = reopened.get(&key).unwrap().unwrap();
        assert_eq!(
            next_worker
                .reconcile_ambiguous(
                    current,
                    &RecordingReader {
                        expected_author_id: "bot-1".into(),
                        outcome: DiscordTerminalDeliveryReadResult::TransientFailure {
                            classification: "DISCORD_MESSAGE_VERIFICATION_TRANSPORT",
                        },
                        calls: reader_calls.clone(),
                    },
                )
                .await
                .unwrap(),
            AmbiguousReconciliationOutcome::Held
        );
        let held = reopened.get(&key).unwrap().unwrap();
        assert_eq!(held.state, TerminalDeliveryState::OperatorHold);
        assert_eq!(
            held.operator_hold_reason.as_deref(),
            Some("RECONCILIATION_EXHAUSTED")
        );
        assert_eq!(
            reopened
                .ambiguous_reconciliation_read_attempts(&key)
                .unwrap(),
            MAX_AMBIGUOUS_RECONCILIATION_ATTEMPTS
        );
        assert_eq!(
            reader_calls.load(Ordering::SeqCst),
            MAX_AMBIGUOUS_RECONCILIATION_ATTEMPTS
        );
        assert_eq!(lookup_calls.load(Ordering::SeqCst), 0);
        assert_eq!(held.terminal_payload, payload);
        assert_eq!(held.terminal_payload_digest, digest);
        assert_eq!(held.discord_message_id.as_deref(), Some("discord-known"));
        assert_eq!(held.attempt_count, delivery_attempts);
    }

    #[tokio::test]
    async fn bounded_ambiguous_reconciliation_accepts_positive_proof_before_exhaustion() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = TerminalDeliveryWorker::new(repo.clone(), Arc::new(Lookup(json!({}))));
        let original = ambiguous_record_with_known_message(&repo);
        let key = original.terminal_delivery_key.clone();
        for outcome in [
            DiscordTerminalDeliveryReadResult::TransientFailure {
                classification: "DISCORD_MESSAGE_VERIFICATION_TRANSPORT",
            },
            DiscordTerminalDeliveryReadResult::NotFound,
        ] {
            let current = repo.get(&key).unwrap().unwrap();
            assert_eq!(
                worker
                    .reconcile_ambiguous(
                        current,
                        &RecordingReader {
                            expected_author_id: "bot-1".into(),
                            outcome,
                            calls: Arc::new(AtomicUsize::new(0)),
                        },
                    )
                    .await
                    .unwrap(),
                AmbiguousReconciliationOutcome::StillAmbiguous
            );
        }
        let current = repo.get(&key).unwrap().unwrap();
        assert_eq!(
            worker
                .reconcile_ambiguous(
                    current,
                    &RecordingReader {
                        expected_author_id: "bot-1".into(),
                        outcome: found_terminal_message(),
                        calls: Arc::new(AtomicUsize::new(0)),
                    },
                )
                .await
                .unwrap(),
            AmbiguousReconciliationOutcome::Delivered
        );
        assert_eq!(
            repo.get(&key).unwrap().unwrap().state,
            TerminalDeliveryState::Delivered
        );
    }

    #[tokio::test]
    async fn ambiguous_reconciliation_holds_immediately_for_missing_id_forbidden_and_mismatch() {
        let scenarios = [
            (
                DiscordTerminalDeliveryReadResult::Forbidden,
                "MESSAGE_VERIFICATION_FORBIDDEN",
            ),
            (
                DiscordTerminalDeliveryReadResult::Found(DiscordTerminalMessage {
                    message_id: "discord-known".into(),
                    channel_id: "1".into(),
                    author_id: "wrong".into(),
                    content: "persisted".into(),
                }),
                "MESSAGE_IDENTITY_MISMATCH",
            ),
        ];
        for (outcome, reason) in scenarios {
            let tmp = TempDir::new().unwrap();
            let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
            let worker = TerminalDeliveryWorker::new(repo.clone(), Arc::new(Lookup(json!({}))));
            let record = ambiguous_record_with_known_message(&repo);
            assert_eq!(
                worker
                    .reconcile_ambiguous(
                        record.clone(),
                        &RecordingReader {
                            expected_author_id: "bot-1".into(),
                            outcome,
                            calls: Arc::new(AtomicUsize::new(0)),
                        },
                    )
                    .await
                    .unwrap(),
                AmbiguousReconciliationOutcome::Held
            );
            let held = repo.get(&record.terminal_delivery_key).unwrap().unwrap();
            assert_eq!(held.operator_hold_reason.as_deref(), Some(reason));
            assert_eq!(held.attempt_count, record.attempt_count);
        }

        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = TerminalDeliveryWorker::new(repo.clone(), Arc::new(Lookup(json!({}))));
        let delivering = delivering_record(&repo, "no-message-id", Some(Utc::now()), Some("lease"));
        let ambiguous = repo
            .transition(
                &delivering.terminal_delivery_key,
                delivering.state_revision,
                TerminalDeliveryState::Ambiguous,
                TransitionUpdate::default(),
                "TEST_AMBIGUOUS",
            )
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        assert_eq!(
            worker
                .reconcile_ambiguous(
                    ambiguous.clone(),
                    &RecordingReader {
                        expected_author_id: "bot-1".into(),
                        outcome: found_terminal_message(),
                        calls: calls.clone(),
                    },
                )
                .await
                .unwrap(),
            AmbiguousReconciliationOutcome::Held
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let held = repo.get(&ambiguous.terminal_delivery_key).unwrap().unwrap();
        assert_eq!(
            held.operator_hold_reason.as_deref(),
            Some("AMBIGUOUS_UNRESOLVED")
        );
    }

    #[tokio::test]
    async fn concurrent_bounded_reconcilers_cannot_hold_a_record_verified_delivered() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = Arc::new(TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(Lookup(json!({}))),
        ));
        let ambiguous = ambiguous_record_with_known_message(&repo);
        let key = ambiguous.terminal_delivery_key.clone();
        let reader = Arc::new(RecordingReader {
            expected_author_id: "bot-1".into(),
            outcome: found_terminal_message(),
            calls: Arc::new(AtomicUsize::new(0)),
        });
        let first = {
            let worker = worker.clone();
            let reader = reader.clone();
            let record = ambiguous.clone();
            tokio::spawn(async move { worker.reconcile_ambiguous(record, reader.as_ref()).await })
        };
        let second = {
            let worker = worker.clone();
            let reader = reader.clone();
            tokio::spawn(
                async move { worker.reconcile_ambiguous(ambiguous, reader.as_ref()).await },
            )
        };
        for result in [
            first.await.unwrap().unwrap(),
            second.await.unwrap().unwrap(),
        ] {
            assert!(matches!(
                result,
                AmbiguousReconciliationOutcome::Delivered | AmbiguousReconciliationOutcome::Noop
            ));
        }
        let delivered = repo.get(&key).unwrap().unwrap();
        assert_eq!(delivered.state, TerminalDeliveryState::Delivered);
        assert_ne!(
            delivered.operator_hold_reason.as_deref(),
            Some("RECONCILIATION_EXHAUSTED")
        );
    }

    async fn deliver_outcome(outcome: DiscordSendResult) -> TerminalDeliveryRecordV1 {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = TerminalDeliveryWorker::new(
            repo,
            Arc::new(Lookup(
                json!({"terminal_status":"completed", "terminal_result":{"final_answer":"done"}}),
            )),
        );
        worker
            .capture_and_deliver(metadata(), channel(), "42", &Outcome(outcome))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn durable_outcomes_use_safe_terminal_states_and_bounded_attempts() {
        assert_eq!(
            deliver_outcome(DiscordSendResult::DefiniteSuccess("m".into()))
                .await
                .state,
            TerminalDeliveryState::Delivered
        );
        assert_eq!(
            deliver_outcome(DiscordSendResult::DefinitePermanentFailure {
                classification: "403"
            })
            .await
            .state,
            TerminalDeliveryState::PermanentRejected
        );
        assert_eq!(
            deliver_outcome(DiscordSendResult::Ambiguous {
                classification: "TIMEOUT"
            })
            .await
            .state,
            TerminalDeliveryState::Ambiguous
        );
        let retry = deliver_outcome(DiscordSendResult::DefiniteTransientFailure {
            classification: "429",
        })
        .await;
        assert_eq!(retry.state, TerminalDeliveryState::RetryScheduled);
        assert_eq!(retry.attempt_count, 1);
        assert!(retry.next_attempt_at.is_some());
    }

    #[tokio::test]
    async fn definite_transient_exhaustion_holds_after_five_sends() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = TerminalDeliveryWorker::new(
            repo,
            Arc::new(Lookup(
                json!({"terminal_status":"completed", "terminal_result":{"final_answer":"done"}}),
            )),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let sender = CountingOutcome {
            outcome: DiscordSendResult::DefiniteTransientFailure {
                classification: "DISCORD_HTTP_5XX_NO_ACCEPTANCE",
            },
            calls: calls.clone(),
        };
        let mut record = worker
            .capture_and_deliver(metadata(), channel(), "42", &sender)
            .await
            .unwrap();
        for _ in 1..MAX_EXTERNAL_ATTEMPTS {
            record = worker
                .deliver_ready(record, channel(), &sender)
                .await
                .unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), MAX_EXTERNAL_ATTEMPTS as usize);
        assert_eq!(record.state, TerminalDeliveryState::OperatorHold);
        assert_eq!(
            record.operator_hold_reason.as_deref(),
            Some("RETRY_EXHAUSTED_DEFINITE_TRANSIENT")
        );
        let states: Vec<_> = worker
            .repo
            .events(&record.terminal_delivery_key)
            .unwrap()
            .into_iter()
            .map(|event| event.new_state)
            .collect();
        assert_eq!(
            &states[states.len() - 3..],
            [
                TerminalDeliveryState::Delivering,
                TerminalDeliveryState::RetryScheduled,
                TerminalDeliveryState::OperatorHold,
            ]
        );
    }

    #[tokio::test]
    async fn retry_reuses_persisted_payload_without_a_second_runtime_lookup() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let worker = TerminalDeliveryWorker::new(
            repo,
            Arc::new(RecordingLookup {
                value: json!({"terminal_status":"completed", "terminal_result":{"final_answer":"stored"}}),
                calls: calls.clone(),
            }),
        );
        let sender = RecordedOutcomes {
            calls: Arc::new(AtomicUsize::new(0)),
            contents: Arc::new(std::sync::Mutex::new(Vec::new())),
        };
        let retry = worker
            .capture_and_deliver(metadata(), channel(), "42", &sender)
            .await
            .unwrap();
        let digest = retry.terminal_payload_digest.clone();
        let delivered = worker
            .recover_ready_or_retry(retry, channel(), &sender)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(delivered.terminal_payload_digest, digest);
        assert_eq!(*sender.contents.lock().unwrap(), vec!["stored", "stored"]);
    }

    #[tokio::test]
    async fn pending_recovery_uses_persisted_payload_without_runtime_lookup() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let worker = TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(RecordingLookup {
                value: json!({"terminal_status":"completed", "terminal_result":{"final_answer":"must-not-read"}}),
                calls: calls.clone(),
            }),
        );
        let pending = repo
            .create_or_reuse(crate::terminal_delivery::NewTerminalDeliveryRecord {
                openab_inbound_turn_id: "discord:pending".into(),
                platform: "discord".into(),
                channel_id: "1".into(),
                thread_id: None,
                acp_request_id: "q".into(),
                acp_session_id: "s".into(),
                runtime_run_id: "runtime-pending".into(),
                workflow_run_id: Some("workflow-pending".into()),
                conversation_id: "conversation-pending".into(),
                response_sequence: 0,
                terminal_payload: json!({"content":"persisted"}),
            })
            .unwrap();
        let ready = worker.recover_pending_result(pending.clone()).unwrap();
        assert_eq!(ready.state, TerminalDeliveryState::ReadyToDeliver);
        assert_eq!(ready.terminal_payload, pending.terminal_payload);
        assert_eq!(
            ready.terminal_payload_digest,
            pending.terminal_payload_digest
        );
        assert_eq!(ready.runtime_run_id, pending.runtime_run_id);
        assert_eq!(ready.attempt_count, pending.attempt_count);
        assert_eq!(ready.state_revision, pending.state_revision + 1);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn pending_recovery_rejects_corrupt_payload_without_transition() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(Lookup(json!({"terminal_status":"completed"}))),
        );
        let pending = repo
            .create_or_reuse(crate::terminal_delivery::NewTerminalDeliveryRecord {
                openab_inbound_turn_id: "discord:corrupt".into(),
                platform: "discord".into(),
                channel_id: "1".into(),
                thread_id: None,
                acp_request_id: "q".into(),
                acp_session_id: "s".into(),
                runtime_run_id: "runtime-corrupt".into(),
                workflow_run_id: None,
                conversation_id: "conversation-corrupt".into(),
                response_sequence: 0,
                terminal_payload: json!({"content":"persisted"}),
            })
            .unwrap();
        let mut corrupt = pending.clone();
        corrupt.terminal_payload_digest = "not-the-persisted-digest".into();
        assert!(matches!(
            worker.recover_pending_result(corrupt),
            Err(TerminalDeliveryError::MalformedRecord(_))
        ));
        assert_eq!(
            repo.get(&pending.terminal_delivery_key)
                .unwrap()
                .unwrap()
                .state,
            TerminalDeliveryState::PendingResult
        );
    }

    #[tokio::test]
    async fn timeout_outcome_is_persisted_as_ambiguous_without_resend() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = TerminalDeliveryWorker::new(
            repo,
            Arc::new(Lookup(
                json!({"terminal_status":"completed","terminal_result":{"final_answer":"done"}}),
            )),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let sender = CountingOutcome {
            outcome: DiscordSendResult::Ambiguous {
                classification: "DISCORD_TERMINAL_SEND_TIMEOUT",
            },
            calls: calls.clone(),
        };
        let record = worker
            .capture_and_deliver(metadata(), channel(), "42", &sender)
            .await
            .unwrap();
        assert_eq!(record.state, TerminalDeliveryState::Ambiguous);
        assert_eq!(
            record.last_failure_classification.as_deref(),
            Some("DISCORD_TERMINAL_SEND_TIMEOUT")
        );
        assert_eq!(record.next_attempt_at, None);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let replay = worker
            .deliver_ready(record, channel(), &sender)
            .await
            .unwrap();
        assert_eq!(replay.state, TerminalDeliveryState::Ambiguous);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_workers_claim_once_and_send_once() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(Lookup(
                json!({"terminal_status":"completed","terminal_result":{"final_answer":"done"}}),
            )),
        );
        let pending = repo
            .create_or_reuse(crate::terminal_delivery::NewTerminalDeliveryRecord {
                openab_inbound_turn_id: "discord:42".into(),
                platform: "discord".into(),
                channel_id: "1".into(),
                thread_id: None,
                acp_request_id: "q".into(),
                acp_session_id: "s".into(),
                runtime_run_id: "r".into(),
                workflow_run_id: None,
                conversation_id: "c".into(),
                response_sequence: 0,
                terminal_payload: json!({"content":"done"}),
            })
            .unwrap();
        let ready = repo
            .transition(
                &pending.terminal_delivery_key,
                pending.state_revision,
                TerminalDeliveryState::ReadyToDeliver,
                TransitionUpdate::default(),
                "TEST_READY",
            )
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let sender = CountingOutcome {
            outcome: DiscordSendResult::DefiniteSuccess("one".into()),
            calls: calls.clone(),
        };
        let (one, two) = tokio::join!(
            worker.deliver_ready(ready.clone(), channel(), &sender),
            worker.deliver_ready(ready, channel(), &sender),
        );
        assert!(
            one.is_ok() && two.is_ok(),
            "worker results: {one:?} / {two:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let record = repo.get(&pending.terminal_delivery_key).unwrap().unwrap();
        assert_eq!(record.state, TerminalDeliveryState::Delivered);
        let events = repo.events(&pending.terminal_delivery_key).unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|e| e.safe_reason_code == "DELIVERY_CLAIMED")
                .count(),
            1,
            "exactly one claim transition"
        );
    }

    #[tokio::test]
    async fn concurrent_workers_multi_iteration_stability() {
        for _ in 0..25 {
            let tmp = TempDir::new().unwrap();
            let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
            let worker = Arc::new(TerminalDeliveryWorker::new(
                repo.clone(),
                Arc::new(Lookup(
                    json!({"terminal_status":"completed","terminal_result":{"final_answer":"done"}}),
                )),
            ));
            let pending = repo
                .create_or_reuse(crate::terminal_delivery::NewTerminalDeliveryRecord {
                    openab_inbound_turn_id: "discord:42".into(),
                    platform: "discord".into(),
                    channel_id: "1".into(),
                    thread_id: None,
                    acp_request_id: "q".into(),
                    acp_session_id: "s".into(),
                    runtime_run_id: "r".into(),
                    workflow_run_id: None,
                    conversation_id: "c".into(),
                    response_sequence: 0,
                    terminal_payload: json!({"content":"done"}),
                })
                .unwrap();
            let ready = repo
                .transition(
                    &pending.terminal_delivery_key,
                    pending.state_revision,
                    TerminalDeliveryState::ReadyToDeliver,
                    TransitionUpdate::default(),
                    "TEST_READY",
                )
                .unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let sender = CountingOutcome {
                outcome: DiscordSendResult::DefiniteSuccess("one".into()),
                calls: calls.clone(),
            };
            let (one, two) = tokio::join!(
                worker.deliver_ready(ready.clone(), channel(), &sender),
                worker.deliver_ready(ready, channel(), &sender),
            );
            assert!(
                one.is_ok() && two.is_ok(),
                "worker results: {one:?} / {two:?}"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let record = repo.get(&pending.terminal_delivery_key).unwrap().unwrap();
            assert_eq!(record.state, TerminalDeliveryState::Delivered);
            let events = repo.events(&pending.terminal_delivery_key).unwrap();
            assert_eq!(
                events
                    .iter()
                    .filter(|e| e.safe_reason_code == "DELIVERY_CLAIMED")
                    .count(),
                1,
                "exactly one claim transition per race"
            );
        }
    }

    #[tokio::test]
    async fn permanent_rejected_replay_does_not_invoke_sender() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(Lookup(
                json!({"terminal_status":"completed","terminal_result":{"final_answer":"done"}}),
            )),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let sender = CountingOutcome {
            outcome: DiscordSendResult::DefinitePermanentFailure {
                classification: "403_FORBIDDEN",
            },
            calls: calls.clone(),
        };
        let rejected = worker
            .capture_and_deliver(metadata(), channel(), "42", &sender)
            .await
            .unwrap();
        assert_eq!(rejected.state, TerminalDeliveryState::PermanentRejected);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let replay_calls = Arc::new(AtomicUsize::new(0));
        let replay_sender = CountingOutcome {
            outcome: DiscordSendResult::DefiniteSuccess("unexpected".into()),
            calls: replay_calls.clone(),
        };
        let replay = worker
            .deliver_ready(rejected, channel(), &replay_sender)
            .await
            .unwrap();
        assert_eq!(replay.state, TerminalDeliveryState::PermanentRejected);
        assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn stale_revision_does_not_invoke_sender_and_does_not_mutate() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(Lookup(
                json!({"terminal_status":"completed","terminal_result":{"final_answer":"done"}}),
            )),
        );
        let pending = repo
            .create_or_reuse(crate::terminal_delivery::NewTerminalDeliveryRecord {
                openab_inbound_turn_id: "discord:42".into(),
                platform: "discord".into(),
                channel_id: "1".into(),
                thread_id: None,
                acp_request_id: "q".into(),
                acp_session_id: "s".into(),
                runtime_run_id: "r".into(),
                workflow_run_id: None,
                conversation_id: "c".into(),
                response_sequence: 0,
                terminal_payload: json!({"content":"done"}),
            })
            .unwrap();
        let ready = repo
            .transition(
                &pending.terminal_delivery_key,
                pending.state_revision,
                TerminalDeliveryState::ReadyToDeliver,
                TransitionUpdate::default(),
                "TEST_READY",
            )
            .unwrap();
        let stale_snapshot = ready.clone();

        let _winner = repo
            .transition(
                &ready.terminal_delivery_key,
                ready.state_revision,
                TerminalDeliveryState::Delivering,
                TransitionUpdate {
                    attempt_count: Some(1),
                    delivery_lease_token: Some(Some("winner-token".into())),
                    delivery_started_at: Some(Some(Utc::now())),
                    ..Default::default()
                },
                "DELIVERY_CLAIMED",
            )
            .unwrap();

        let loser_calls = Arc::new(AtomicUsize::new(0));
        let loser_sender = CountingOutcome {
            outcome: DiscordSendResult::DefiniteSuccess("unexpected".into()),
            calls: loser_calls.clone(),
        };
        let res = worker
            .deliver_ready(stale_snapshot, channel(), &loser_sender)
            .await
            .unwrap();
        assert_eq!(res.state, TerminalDeliveryState::Delivering);
        assert_eq!(loser_calls.load(Ordering::SeqCst), 0);
        let record = repo.get(&pending.terminal_delivery_key).unwrap().unwrap();
        assert_eq!(record.state_revision, 2);
        assert_eq!(record.delivery_lease_token.as_deref(), Some("winner-token"));
    }

    #[tokio::test]
    async fn semantic_payload_conflict_does_not_invoke_sender() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let _first = repo
            .create_or_reuse(crate::terminal_delivery::NewTerminalDeliveryRecord {
                openab_inbound_turn_id: "discord:42".into(),
                platform: "discord".into(),
                channel_id: "1".into(),
                thread_id: None,
                acp_request_id: "q".into(),
                acp_session_id: "s".into(),
                runtime_run_id: "r".into(),
                workflow_run_id: None,
                conversation_id: "c".into(),
                response_sequence: 0,
                terminal_payload: json!({"content":"payload-1"}),
            })
            .unwrap();

        let worker = TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(Lookup(
                json!({"terminal_status":"completed","terminal_result":{"final_answer":"conflicting-payload"}}),
            )),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let sender = CountingOutcome {
            outcome: DiscordSendResult::DefiniteSuccess("unexpected".into()),
            calls: calls.clone(),
        };
        let result = worker
            .capture_and_deliver(metadata(), channel(), "42", &sender)
            .await;
        assert!(matches!(result, Err(LookupError::Unavailable)));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn delivered_replay_regression_does_not_invoke_sender() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(Lookup(
                json!({"terminal_status":"completed", "terminal_result":{"final_answer":"done"}}),
            )),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let sender = CountingOutcome {
            outcome: DiscordSendResult::DefiniteSuccess("msg-1".into()),
            calls: calls.clone(),
        };
        let delivered = worker
            .capture_and_deliver(metadata(), channel(), "42", &sender)
            .await
            .unwrap();
        assert_eq!(delivered.state, TerminalDeliveryState::Delivered);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let replay = worker
            .deliver_ready(delivered, channel(), &sender)
            .await
            .unwrap();
        assert_eq!(replay.state, TerminalDeliveryState::Delivered);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn record_exists_before_send_and_replay_is_not_resent() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(Lookup(
                json!({"terminal_status":"completed", "terminal_result":{"final_answer":"done"}}),
            )),
        );
        let metadata = AcpTerminalMetadata {
            runtime_run_id: "r".into(),
            legacy_run_id: Some("r".into()),
            acp_request_id: "q".into(),
            acp_session_id: "s".into(),
            workflow_run_id: None,
            conversation_id: "c".into(),
        };
        let channel = ChannelRef {
            platform: "discord".into(),
            channel_id: "1".into(),
            thread_id: None,
            parent_id: None,
            origin_event_id: Some("42".into()),
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let sender = CountingOutcome {
            outcome: DiscordSendResult::DefiniteSuccess("discord-1".into()),
            calls: calls.clone(),
        };
        let delivered = worker
            .capture_and_deliver(metadata.clone(), channel.clone(), "42", &sender)
            .await
            .unwrap();
        assert_eq!(delivered.state, TerminalDeliveryState::Delivered);
        assert_eq!(delivered.delivery_lease_token, None);
        let replay = worker
            .capture_and_deliver(metadata, channel, "42", &sender)
            .await
            .unwrap();
        assert_eq!(replay.discord_message_id.as_deref(), Some("discord-1"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let events = repo.events(&replay.terminal_delivery_key).unwrap();
        let reasons: Vec<_> = events
            .iter()
            .map(|event| event.safe_reason_code.as_str())
            .collect();
        assert_eq!(
            reasons,
            vec![
                "CREATED",
                "RESULT_MATERIALIZED",
                "DELIVERY_CLAIMED",
                "DISCORD_ACCEPTED_MESSAGE_ID_PERSISTED",
                "DISCORD_DELIVERED",
            ]
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.new_state == TerminalDeliveryState::Delivering)
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn post_evidence_crash_reopens_and_verifies_known_message_without_resend_or_lookup() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let stale_at = Utc::now() - Duration::seconds(STALE_DELIVERING_AFTER_SECS + 1);
        let delivering = delivering_record(&repo, "post-evidence", Some(stale_at), Some("lease"));
        let evidenced = repo
            .persist_accepted_message_id(
                &delivering.terminal_delivery_key,
                delivering.state_revision,
                "lease",
                "discord-known",
            )
            .unwrap();
        let key = evidenced.terminal_delivery_key.clone();
        let digest = evidenced.terminal_payload_digest.clone();
        let path = repo.database_path().to_path_buf();
        drop(repo);

        let reopened = Arc::new(TerminalDeliveryRepository::open_path(path).unwrap());
        let lookup_calls = Arc::new(AtomicUsize::new(0));
        let worker = TerminalDeliveryWorker::new(
            reopened.clone(),
            Arc::new(RecordingLookup {
                value: json!({}),
                calls: lookup_calls.clone(),
            }),
        );
        let stale = reopened.get(&key).unwrap().unwrap();
        let ambiguous = worker
            .recover_stale_delivering(stale, stale_delivering_before(Utc::now()))
            .unwrap();
        assert_eq!(ambiguous.state, TerminalDeliveryState::Ambiguous);
        assert_eq!(
            ambiguous.discord_message_id.as_deref(),
            Some("discord-known")
        );
        let reader_calls = Arc::new(AtomicUsize::new(0));
        assert_eq!(
            worker
                .reconcile_ambiguous(
                    ambiguous,
                    &RecordingReader {
                        expected_author_id: "bot-1".into(),
                        outcome: found_terminal_message(),
                        calls: reader_calls.clone(),
                    },
                )
                .await
                .unwrap(),
            AmbiguousReconciliationOutcome::Delivered
        );
        let delivered = reopened.get(&key).unwrap().unwrap();
        assert_eq!(delivered.state, TerminalDeliveryState::Delivered);
        assert_eq!(
            delivered.discord_message_id.as_deref(),
            Some("discord-known")
        );
        assert_eq!(delivered.terminal_payload_digest, digest);
        assert_eq!(reader_calls.load(Ordering::SeqCst), 1);
        assert_eq!(lookup_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn stale_delivering_recovery_is_fenced_ambiguous_and_side_effect_free() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let lookup_calls = Arc::new(AtomicUsize::new(0));
        let worker = TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(RecordingLookup {
                value: json!({}),
                calls: lookup_calls.clone(),
            }),
        );
        let sender_calls = Arc::new(AtomicUsize::new(0));
        let _sender = CountingOutcome {
            outcome: DiscordSendResult::DefiniteSuccess("must-not-send".into()),
            calls: sender_calls.clone(),
        };
        let now = Utc::now();
        let stale = delivering_record(
            &repo,
            "stale",
            Some(now - Duration::seconds(STALE_DELIVERING_AFTER_SECS + 1)),
            Some("lease-stale"),
        );
        let recovered = worker
            .recover_stale_delivering(stale.clone(), stale_delivering_before(now))
            .unwrap();
        assert_eq!(recovered.state, TerminalDeliveryState::Ambiguous);
        assert_eq!(recovered.terminal_payload, stale.terminal_payload);
        assert_eq!(
            recovered.terminal_payload_digest,
            stale.terminal_payload_digest
        );
        assert_eq!(recovered.attempt_count, stale.attempt_count);
        assert_eq!(recovered.delivery_started_at, stale.delivery_started_at);
        assert_eq!(recovered.next_attempt_at, None);
        assert_eq!(recovered.delivery_lease_token, None);
        assert_eq!(lookup_calls.load(Ordering::SeqCst), 0);
        assert_eq!(sender_calls.load(Ordering::SeqCst), 0);
        let events = repo.events(&stale.terminal_delivery_key).unwrap();
        assert_eq!(
            events.last().unwrap().previous_state,
            Some(TerminalDeliveryState::Delivering)
        );
        assert_eq!(
            events.last().unwrap().new_state,
            TerminalDeliveryState::Ambiguous
        );
        assert_eq!(
            events.last().unwrap().safe_reason_code,
            "STALE_DELIVERING_DECLARED_AMBIGUOUS"
        );
    }

    #[test]
    fn fresh_delivering_and_non_delivering_records_are_noops() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = TerminalDeliveryWorker::new(repo.clone(), Arc::new(Lookup(json!({}))));
        let now = Utc::now();
        let fresh = delivering_record(&repo, "fresh", Some(now), Some("lease-fresh"));
        assert_eq!(
            worker
                .recover_stale_delivering(fresh.clone(), stale_delivering_before(now))
                .unwrap(),
            fresh
        );
        let ambiguous = repo
            .transition(
                &fresh.terminal_delivery_key,
                fresh.state_revision,
                TerminalDeliveryState::Ambiguous,
                TransitionUpdate::default(),
                "TEST_AMBIGUOUS",
            )
            .unwrap();
        assert_eq!(
            worker
                .recover_stale_delivering(ambiguous.clone(), stale_delivering_before(now))
                .unwrap(),
            ambiguous
        );
        let terminal = delivering_record(
            &repo,
            "terminal-noop",
            Some(now - Duration::seconds(301)),
            Some("lease-terminal"),
        );
        let delivered = repo
            .transition(
                &terminal.terminal_delivery_key,
                terminal.state_revision,
                TerminalDeliveryState::Delivered,
                TransitionUpdate::default(),
                "TEST_DELIVERED",
            )
            .unwrap();
        assert_eq!(
            worker
                .recover_stale_delivering(delivered.clone(), stale_delivering_before(now))
                .unwrap(),
            delivered
        );
        let rejected_source = delivering_record(
            &repo,
            "permanent-rejected-noop",
            Some(now - Duration::seconds(301)),
            Some("lease-permanent-rejected"),
        );
        let rejected = repo
            .transition(
                &rejected_source.terminal_delivery_key,
                rejected_source.state_revision,
                TerminalDeliveryState::PermanentRejected,
                TransitionUpdate::default(),
                "TEST_PERMANENT_REJECTED",
            )
            .unwrap();
        assert_eq!(
            worker
                .recover_stale_delivering(rejected.clone(), stale_delivering_before(now))
                .unwrap(),
            rejected
        );
    }

    #[test]
    fn stale_recovery_rejects_wrong_fence_and_missing_lease_without_mutation() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = TerminalDeliveryWorker::new(repo.clone(), Arc::new(Lookup(json!({}))));
        let now = Utc::now();
        let stale = delivering_record(
            &repo,
            "fence",
            Some(now - Duration::seconds(301)),
            Some("lease-a"),
        );
        let mut wrong_token = stale.clone();
        wrong_token.delivery_lease_token = Some("lease-b".into());
        assert!(matches!(
            worker.recover_stale_delivering(wrong_token, stale_delivering_before(now)),
            Err(TerminalDeliveryError::LeaseTokenConflict)
        ));
        let mut wrong_revision = stale.clone();
        wrong_revision.state_revision -= 1;
        assert!(matches!(
            worker.recover_stale_delivering(wrong_revision, stale_delivering_before(now)),
            Err(TerminalDeliveryError::RevisionConflict { .. })
        ));
        assert_eq!(
            repo.get(&stale.terminal_delivery_key).unwrap().unwrap(),
            stale
        );
        let missing_lease = delivering_record(
            &repo,
            "missing-lease",
            Some(now - Duration::seconds(301)),
            None,
        );
        assert!(matches!(
            worker.recover_stale_delivering(missing_lease.clone(), stale_delivering_before(now)),
            Err(TerminalDeliveryError::MissingDeliveryLeaseToken)
        ));
        assert_eq!(
            repo.get(&missing_lease.terminal_delivery_key)
                .unwrap()
                .unwrap(),
            missing_lease
        );
    }

    #[test]
    fn missing_start_time_is_ambiguous_when_a_lease_exists() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = TerminalDeliveryWorker::new(repo.clone(), Arc::new(Lookup(json!({}))));
        let record = delivering_record(&repo, "missing-start", None, Some("lease-missing-start"));
        let recovered = worker
            .recover_stale_delivering(record.clone(), Utc::now())
            .unwrap();
        assert_eq!(recovered.state, TerminalDeliveryState::Ambiguous);
        assert_eq!(
            repo.events(&record.terminal_delivery_key)
                .unwrap()
                .last()
                .unwrap()
                .safe_reason_code,
            "DELIVERING_MISSING_START_TIME_AMBIGUOUS"
        );
    }

    #[test]
    fn concurrent_stale_recovery_has_one_transition_winner_and_one_safe_observer() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let now = Utc::now();
        let stale = delivering_record(
            &repo,
            "concurrent",
            Some(now - Duration::seconds(301)),
            Some("lease-concurrent"),
        );
        let worker = Arc::new(TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(Lookup(json!({}))),
        ));
        let barrier = Arc::new(Barrier::new(2));
        let mut joins = Vec::new();
        for _ in 0..2 {
            let worker = worker.clone();
            let barrier = barrier.clone();
            let snapshot = stale.clone();
            joins.push(thread::spawn(move || {
                barrier.wait();
                worker.recover_stale_delivering(snapshot, stale_delivering_before(now))
            }));
        }
        let results: Vec<_> = joins.into_iter().map(|join| join.join().unwrap()).collect();
        assert!(results.iter().all(|result| matches!(result, Ok(record) if record.state == TerminalDeliveryState::Ambiguous)));
        let events = repo.events(&stale.terminal_delivery_key).unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.new_state == TerminalDeliveryState::Ambiguous)
                .count(),
            1
        );
        assert_eq!(
            repo.get(&stale.terminal_delivery_key)
                .unwrap()
                .unwrap()
                .state,
            TerminalDeliveryState::Ambiguous
        );
    }

    #[tokio::test]
    async fn restart_reconciliation_advances_pending_then_delivers_on_the_next_run_without_lookup()
    {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let lookup_calls = Arc::new(AtomicUsize::new(0));
        let worker = TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(RecordingLookup {
                value: json!({}),
                calls: lookup_calls.clone(),
            }),
        );
        let pending = repo
            .create_or_reuse(crate::terminal_delivery::NewTerminalDeliveryRecord {
                openab_inbound_turn_id: "discord:restart-pending".into(),
                platform: "discord".into(),
                channel_id: "1".into(),
                thread_id: None,
                acp_request_id: "q".into(),
                acp_session_id: "s".into(),
                runtime_run_id: "r".into(),
                workflow_run_id: None,
                conversation_id: "c".into(),
                response_sequence: 0,
                terminal_payload: json!({"content":"persisted"}),
            })
            .unwrap();
        let send_calls = Arc::new(AtomicUsize::new(0));
        let sender = CountingOutcome {
            outcome: DiscordSendResult::DefiniteSuccess("delivered-next-run".into()),
            calls: send_calls.clone(),
        };
        let now = Utc::now();
        assert_eq!(
            worker
                .reconcile_record(pending.clone(), now, stale_delivering_before(now), &sender)
                .await
                .unwrap(),
            ReconciliationOutcome::Advanced
        );
        assert_eq!(send_calls.load(Ordering::SeqCst), 0);
        assert_eq!(lookup_calls.load(Ordering::SeqCst), 0);
        let ready = repo.get(&pending.terminal_delivery_key).unwrap().unwrap();
        assert_eq!(ready.state, TerminalDeliveryState::ReadyToDeliver);
        assert_eq!(ready.attempt_count, pending.attempt_count);
        assert_eq!(
            worker
                .reconcile_record(ready, now, stale_delivering_before(now), &sender)
                .await
                .unwrap(),
            ReconciliationOutcome::Delivered
        );
        assert_eq!(send_calls.load(Ordering::SeqCst), 1);
        assert_eq!(lookup_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn restart_reconciliation_enforces_due_retry_and_preserves_attempt_budget() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let lookup_calls = Arc::new(AtomicUsize::new(0));
        let worker = TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(RecordingLookup {
                value: json!({}),
                calls: lookup_calls.clone(),
            }),
        );
        let pending = repo
            .create_or_reuse(crate::terminal_delivery::NewTerminalDeliveryRecord {
                openab_inbound_turn_id: "discord:due-retry".into(),
                platform: "discord".into(),
                channel_id: "1".into(),
                thread_id: None,
                acp_request_id: "q".into(),
                acp_session_id: "s".into(),
                runtime_run_id: "r".into(),
                workflow_run_id: None,
                conversation_id: "c".into(),
                response_sequence: 0,
                terminal_payload: json!({"content":"persisted"}),
            })
            .unwrap();
        let ready = repo
            .transition(
                &pending.terminal_delivery_key,
                pending.state_revision,
                TerminalDeliveryState::ReadyToDeliver,
                TransitionUpdate::default(),
                "TEST_READY",
            )
            .unwrap();
        let delivering = repo
            .transition(
                &ready.terminal_delivery_key,
                ready.state_revision,
                TerminalDeliveryState::Delivering,
                TransitionUpdate {
                    attempt_count: Some(1),
                    delivery_lease_token: Some(Some("retry-lease".into())),
                    delivery_started_at: Some(Some(Utc::now())),
                    ..Default::default()
                },
                "TEST_DELIVERING",
            )
            .unwrap();
        let now = Utc::now();
        let due_retry = repo
            .transition_with_lease(
                &delivering.terminal_delivery_key,
                delivering.state_revision,
                "retry-lease",
                TerminalDeliveryState::RetryScheduled,
                TransitionUpdate {
                    next_attempt_at: Some(Some(now)),
                    delivery_lease_token: Some(None),
                    ..Default::default()
                },
                "TEST_RETRY_DUE",
            )
            .unwrap();
        let send_calls = Arc::new(AtomicUsize::new(0));
        let sender = CountingOutcome {
            outcome: DiscordSendResult::DefiniteSuccess("due-retry-delivered".into()),
            calls: send_calls.clone(),
        };
        assert_eq!(
            worker
                .reconcile_record(due_retry, now, stale_delivering_before(now), &sender)
                .await
                .unwrap(),
            ReconciliationOutcome::Delivered
        );
        let delivered = repo.get(&pending.terminal_delivery_key).unwrap().unwrap();
        assert_eq!(delivered.attempt_count, 2);
        assert_eq!(lookup_calls.load(Ordering::SeqCst), 0);
        assert_eq!(send_calls.load(Ordering::SeqCst), 1);

        let future = repo
            .create_or_reuse(crate::terminal_delivery::NewTerminalDeliveryRecord {
                openab_inbound_turn_id: "discord:future-retry".into(),
                platform: "discord".into(),
                channel_id: "1".into(),
                thread_id: None,
                acp_request_id: "q2".into(),
                acp_session_id: "s".into(),
                runtime_run_id: "r2".into(),
                workflow_run_id: None,
                conversation_id: "c".into(),
                response_sequence: 0,
                terminal_payload: json!({"content":"persisted"}),
            })
            .unwrap();
        let future_ready = repo
            .transition(
                &future.terminal_delivery_key,
                future.state_revision,
                TerminalDeliveryState::ReadyToDeliver,
                TransitionUpdate::default(),
                "TEST_READY",
            )
            .unwrap();
        let future_delivering = repo
            .transition(
                &future_ready.terminal_delivery_key,
                future_ready.state_revision,
                TerminalDeliveryState::Delivering,
                TransitionUpdate {
                    delivery_lease_token: Some(Some("future-lease".into())),
                    delivery_started_at: Some(Some(now)),
                    ..Default::default()
                },
                "TEST_DELIVERING",
            )
            .unwrap();
        let future_retry = repo
            .transition_with_lease(
                &future_delivering.terminal_delivery_key,
                future_delivering.state_revision,
                "future-lease",
                TerminalDeliveryState::RetryScheduled,
                TransitionUpdate {
                    next_attempt_at: Some(Some(now + Duration::seconds(60))),
                    delivery_lease_token: Some(None),
                    ..Default::default()
                },
                "TEST_RETRY_FUTURE",
            )
            .unwrap();
        assert_eq!(
            worker
                .reconcile_record(
                    future_retry.clone(),
                    now,
                    stale_delivering_before(now),
                    &sender
                )
                .await
                .unwrap(),
            ReconciliationOutcome::Noop
        );
        assert_eq!(send_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            repo.get(&future_retry.terminal_delivery_key)
                .unwrap()
                .unwrap()
                .attempt_count,
            future_retry.attempt_count
        );
        assert_eq!(lookup_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn restart_reconciliation_batch_is_bounded_and_isolates_record_errors() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let worker = TerminalDeliveryWorker::new(repo.clone(), Arc::new(Lookup(json!({}))));
        let now = Utc::now();
        let failing = delivering_record(
            &repo,
            "batch-missing-lease",
            Some(now - Duration::seconds(STALE_DELIVERING_AFTER_SECS + 1)),
            None,
        );
        let ready_source = repo
            .create_or_reuse(crate::terminal_delivery::NewTerminalDeliveryRecord {
                openab_inbound_turn_id: "discord:batch-ready".into(),
                platform: "discord".into(),
                channel_id: "1".into(),
                thread_id: None,
                acp_request_id: "q".into(),
                acp_session_id: "s".into(),
                runtime_run_id: "r".into(),
                workflow_run_id: None,
                conversation_id: "c".into(),
                response_sequence: 0,
                terminal_payload: json!({"content":"persisted"}),
            })
            .unwrap();
        let ready = repo
            .transition(
                &ready_source.terminal_delivery_key,
                ready_source.state_revision,
                TerminalDeliveryState::ReadyToDeliver,
                TransitionUpdate::default(),
                "TEST_READY",
            )
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let sender = CountingOutcome {
            outcome: DiscordSendResult::DefiniteSuccess("batch-delivered".into()),
            calls: calls.clone(),
        };
        let summary = worker
            .reconcile_candidates(now, stale_delivering_before(now), 2, &sender)
            .await
            .unwrap();
        assert_eq!(summary.processed, 2);
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.delivered, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(summary.results.iter().any(|result| {
            result.terminal_delivery_key == failing.terminal_delivery_key
                && matches!(
                    &result.outcome,
                    Err(TerminalDeliveryError::MissingDeliveryLeaseToken)
                )
        }));
        assert!(summary.results.iter().any(|result| {
            result.terminal_delivery_key == ready.terminal_delivery_key
                && matches!(&result.outcome, Ok(ReconciliationOutcome::Delivered))
        }));
    }

    #[tokio::test]
    async fn restart_reconciliation_marks_only_stale_delivering_ambiguous_without_send_or_lookup() {
        let tmp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(tmp.path()).unwrap());
        let lookup_calls = Arc::new(AtomicUsize::new(0));
        let worker = TerminalDeliveryWorker::new(
            repo.clone(),
            Arc::new(RecordingLookup {
                value: json!({}),
                calls: lookup_calls.clone(),
            }),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let sender = CountingOutcome {
            outcome: DiscordSendResult::DefiniteSuccess("must-not-send".into()),
            calls: calls.clone(),
        };
        let now = Utc::now();
        let stale = delivering_record(
            &repo,
            "entry-stale",
            Some(now - Duration::seconds(STALE_DELIVERING_AFTER_SECS + 1)),
            Some("stale-lease"),
        );
        let fresh = delivering_record(&repo, "entry-fresh", Some(now), Some("fresh-lease"));
        assert_eq!(
            worker
                .reconcile_record(stale, now, stale_delivering_before(now), &sender)
                .await
                .unwrap(),
            ReconciliationOutcome::Advanced
        );
        assert_eq!(
            worker
                .reconcile_record(fresh, now, stale_delivering_before(now), &sender)
                .await
                .unwrap(),
            ReconciliationOutcome::Noop
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(lookup_calls.load(Ordering::SeqCst), 0);
    }
}
