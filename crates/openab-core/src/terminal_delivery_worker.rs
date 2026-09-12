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
    NewTerminalDeliveryRecord, TerminalDeliveryError, TerminalDeliveryRecordV1,
    TerminalDeliveryRepository, TerminalDeliveryState, TransitionUpdate,
};

pub const SAFE_FAILED_MESSAGE: &str =
    "⚠️ The requested work failed. Please contact the operator if you need assistance.";
pub const SAFE_CANCELLED_MESSAGE: &str = "⚠️ The requested work was cancelled.";
pub const MAX_EXTERNAL_ATTEMPTS: u64 = 5;
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
        match outcome {
            DiscordSendResult::DefiniteSuccess(message_id) => self.repo.transition(
                &claimed.terminal_delivery_key,
                claimed.state_revision,
                TerminalDeliveryState::Delivered,
                TransitionUpdate {
                    discord_message_id: Some(message_id),
                    delivery_lease_token: Some(None),
                    ..Default::default()
                },
                "DISCORD_DELIVERED",
            ),
            DiscordSendResult::DefinitePermanentFailure { classification } => self.repo.transition(
                &claimed.terminal_delivery_key,
                claimed.state_revision,
                TerminalDeliveryState::PermanentRejected,
                TransitionUpdate {
                    last_failure_classification: Some(Some(classification.into())),
                    delivery_lease_token: Some(None),
                    ..Default::default()
                },
                "DISCORD_PERMANENT_REJECTED",
            ),
            DiscordSendResult::Ambiguous { classification } => self.repo.transition(
                &claimed.terminal_delivery_key,
                claimed.state_revision,
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
                let retry_scheduled = self.repo.transition(
                    &claimed.terminal_delivery_key,
                    claimed.state_revision,
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
                self.repo.transition(
                    &claimed.terminal_delivery_key,
                    claimed.state_revision,
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
            .deliver_ready(retry, channel(), &sender)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(delivered.terminal_payload_digest, digest);
        assert_eq!(*sender.contents.lock().unwrap(), vec!["stored", "stored"]);
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
}
