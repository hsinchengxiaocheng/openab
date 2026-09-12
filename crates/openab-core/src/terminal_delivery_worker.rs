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

use crate::adapter::{ChannelRef, ChatAdapter};
use crate::terminal_delivery::{
    validate_record_integrity, NewTerminalDeliveryRecord, TerminalDeliveryError,
    TerminalDeliveryRecordV1, TerminalDeliveryRepository, TerminalDeliveryState, TransitionUpdate,
};

pub const SAFE_FAILED_MESSAGE: &str =
    "⚠️ The requested work failed. Please contact the operator if you need assistance.";
pub const SAFE_CANCELLED_MESSAGE: &str = "⚠️ The requested work was cancelled.";
pub const MAX_EXTERNAL_ATTEMPTS: u64 = 5;
/// A delivery claim older than this is no longer treated as live.  Its
/// outbound outcome is unknown, so recovery may only declare it ambiguous.
pub const STALE_DELIVERING_AFTER_SECS: i64 = 300;
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
impl TerminalDeliveryWorker {
    pub fn new(
        repo: Arc<TerminalDeliveryRepository>,
        lookup: Arc<dyn TerminalResultLookup>,
    ) -> Self {
        Self { repo, lookup }
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
        let content = claimed
            .terminal_payload
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or(SAFE_FAILED_MESSAGE);
        let attempt = claimed.attempt_count;
        let outcome = sender.send_terminal(&channel, content).await;
        let lease_token = claimed
            .delivery_lease_token
            .as_deref()
            .ok_or(TerminalDeliveryError::InvalidInput("delivery_lease_token"))?;
        match outcome {
            DiscordSendResult::DefiniteSuccess(message_id) => self.repo.transition_with_lease(
                &claimed.terminal_delivery_key,
                claimed.state_revision,
                lease_token,
                TerminalDeliveryState::Delivered,
                TransitionUpdate {
                    discord_message_id: Some(message_id),
                    delivery_lease_token: Some(None),
                    ..Default::default()
                },
                "DISCORD_DELIVERED",
            ),
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
    struct Success;
    #[async_trait]
    impl DiscordTerminalDeliverySender for Success {
        async fn send_terminal(&self, _: &ChannelRef, _: &str) -> DiscordSendResult {
            DiscordSendResult::DefiniteSuccess("discord-1".into())
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
                .filter(|e| e.new_state == TerminalDeliveryState::Delivering)
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
                    .filter(|e| e.new_state == TerminalDeliveryState::Delivering)
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
        let delivered = worker
            .capture_and_deliver(metadata.clone(), channel.clone(), "42", &Success)
            .await
            .unwrap();
        assert_eq!(delivered.state, TerminalDeliveryState::Delivered);
        let replay = worker
            .capture_and_deliver(metadata, channel, "42", &Success)
            .await
            .unwrap();
        assert_eq!(replay.discord_message_id.as_deref(), Some("discord-1"));
        assert_eq!(
            repo.events(&replay.terminal_delivery_key)
                .unwrap()
                .iter()
                .filter(|event| event.new_state == TerminalDeliveryState::Delivering)
                .count(),
            1
        );
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
}
