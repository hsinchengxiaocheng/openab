//! Discord Tech Lead -> AAP terminal-work reopen control-plane client.
//!
//! This module is deliberately separate from autonomous ingress:
//!
//! * autonomous ingress creates/continues ordinary workflow work;
//! * workflow reopen is an explicit mutation authority;
//! * only an already-authorized Tech Lead Discord turn may reach it;
//! * the Runtime remains authoritative for WorkflowRun state/revision;
//! * failures never fall through to ordinary ACP.
//!
//! ## Two sibling actions, one transport
//!
//! Both `Canonical action: reopen-work` (terminal-work reopen) and
//! `Canonical action: reopen-primary` (bounded-defect correction) flow
//! through the same `HttpWorkflowReopenClient`. The bounded-defect
//! correction reuses AAP Runtime's existing intervention capability:
//!
//! ```text
//! VERIFIER_ACTIVE + defect_loop_count == 1
//!        |
//!        | POST /v1/workflows/{workflow_run_id}/intervention/reopen-primary
//!        v
//! PRIMARY_ACTIVE (defect_loop_count remains 1)
//! ```
//!
//! Reusing the Runtime endpoint is deliberate: AAP already enforces
//! the hold-identity precondition, the bounded-defect counter
//! preservation, and the durable audit sink. OpenAB only mirrors the
//! defense-in-depth eligibility checks (state / defect_loop_count /
//! snapshot identity) before invoking the mutation endpoint; AAP
//! remains the final authority.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

use crate::config::WorkflowReopenConfig;

const TECH_LEAD_WAIT_STATE: &str = "TECH_LEAD_WAIT";
const TECH_LEAD_REOPEN_REASON: &str = "TECH_LEAD_POST_REVIEW_REOPEN";

// Phase 8.x — bounded-defect intervention mirrors Runtime's existing
// canonical reasoning at `runtime.application.tech_lead_intervention`.
const VERIFIER_ACTIVE_STATE: &str = "VERIFIER_ACTIVE";
const TECH_LEAD_INTERVENTION_REASON: &str = "BOUNDED_DEFECT_LOOP_TECH_LEAD_CORRECTION";
const EXPECTED_DEFECT_LOOP_COUNT: u64 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowRunSnapshot {
    pub workflow_run_id: String,
    pub state: String,
    pub revision: u64,
    pub defect_loop_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowReopenRequest {
    pub expected_revision: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowReopenResponse {
    pub predecessor_workflow_run_id: String,
    pub predecessor_state: String,
    pub predecessor_revision: u64,
    pub successor_workflow_run_id: String,
    pub successor_state: String,
    pub successor_revision: u64,
    pub successor_defect_loop_count: u64,
    pub actor_client_id: String,
    pub reason: String,
}

/// Phase 8.x — bounded-defect intervention request body.
///
/// Mirrors `WorkflowTechLeadInterventionRequestModel` in the AAP
/// Runtime (`runtime/api/models.py`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowInterventionRequest {
    pub expected_revision: u64,
    pub reason: String,
}

/// Phase 8.x — bounded-defect intervention response body.
///
/// Mirrors `WorkflowTechLeadInterventionResponseModel` in the AAP
/// Runtime (`runtime/api/models.py`). ``workflow_run_id`` MUST equal
/// the requested run id (the intervention mutates the SAME run; it
/// does not create a successor).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowInterventionResponse {
    pub workflow_run_id: String,
    pub previous_state: String,
    pub state: String,
    pub previous_revision: u64,
    pub revision: u64,
    pub previous_defect_loop_count: u64,
    pub defect_loop_count: u64,
    pub previous_hold_error_code: Option<String>,
    pub cleared_hold_rows: u64,
    pub scheduler_woken: bool,
    pub actor_client_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowReopenError {
    Unreachable(String),
    Timeout,
    AuthMissing,
    Http {
        status: u16,
        body_snippet: String,
    },
    Malformed(String),
    WrongState {
        workflow_run_id: String,
        state: String,
    },
    /// Bounded-defect intervention preconditions failed. The snapshot
    /// was either not in ``VERIFIER_ACTIVE`` or its
    /// ``defect_loop_count`` was not the canonical value ``1``.
    WrongDefectLoopCount {
        workflow_run_id: String,
        defect_loop_count: u64,
    },
}

impl std::fmt::Display for WorkflowReopenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreachable(message) => {
                write!(f, "AAP unreachable: {message}")
            }
            Self::Timeout => write!(f, "AAP timeout"),
            Self::AuthMissing => {
                write!(f, "AAP auth credential missing")
            }
            Self::Http {
                status,
                body_snippet,
            } => {
                write!(f, "AAP HTTP {status}: {body_snippet}")
            }
            Self::Malformed(message) => {
                write!(f, "AAP response malformed: {message}")
            }
            Self::WrongState {
                workflow_run_id,
                state,
            } => {
                write!(
                    f,
                    "WorkflowRun '{workflow_run_id}' is not eligible for \
                     terminal-work reopen: state={state}"
                )
            }
            Self::WrongDefectLoopCount {
                workflow_run_id,
                defect_loop_count,
            } => {
                write!(
                    f,
                    "WorkflowRun '{workflow_run_id}' is not eligible for \
                     bounded-defect intervention: defect_loop_count={defect_loop_count}"
                )
            }
        }
    }
}

impl std::error::Error for WorkflowReopenError {}

#[async_trait::async_trait]
pub trait WorkflowReopenTransport: Send + Sync {
    async fn get_json(
        &self,
        url: String,
        bearer_token: String,
        timeout: Duration,
    ) -> Result<(u16, String), WorkflowReopenError>;

    async fn post_json(
        &self,
        url: String,
        bearer_token: String,
        timeout: Duration,
        body: String,
    ) -> Result<(u16, String), WorkflowReopenError>;
}

#[async_trait::async_trait]
pub trait WorkflowReopenClient: Send + Sync {
    /// ``Canonical action: reopen-work`` — start a fresh successor
    /// WorkflowRun after ``TECH_LEAD_WAIT``.
    async fn reopen_terminal_work(
        &self,
        workflow_run_id: &str,
    ) -> Result<WorkflowReopenResponse, WorkflowReopenError>;

    /// ``Canonical action: reopen-primary`` — bounded-defect correction
    /// that flips ``VERIFIER_ACTIVE`` with ``defect_loop_count == 1``
    /// back to ``PRIMARY_ACTIVE`` on the SAME WorkflowRun.
    async fn reopen_primary(
        &self,
        workflow_run_id: &str,
    ) -> Result<WorkflowInterventionResponse, WorkflowReopenError>;
}

pub struct HttpWorkflowReopenClient {
    base_url: String,
    credential: String,
    timeout: Duration,
    http: Arc<dyn WorkflowReopenTransport>,
}

impl HttpWorkflowReopenClient {
    pub fn new(
        config: &WorkflowReopenConfig,
        http: Arc<dyn WorkflowReopenTransport>,
    ) -> Result<Self, WorkflowReopenError> {
        let credential = config
            .resolve_credential()
            .ok_or(WorkflowReopenError::AuthMissing)?;

        Ok(Self {
            base_url: config.aap_runtime_url.trim_end_matches('/').to_string(),
            credential,
            timeout: Duration::from_secs(config.request_timeout_seconds),
            http,
        })
    }

    pub async fn reopen_terminal_work(
        &self,
        workflow_run_id: &str,
    ) -> Result<WorkflowReopenResponse, WorkflowReopenError> {
        let snapshot_url = format!("{}/v1/workflows/{}", self.base_url, workflow_run_id,);

        let (status, response_body) = self
            .http
            .get_json(snapshot_url, self.credential.clone(), self.timeout)
            .await?;

        if !(200..300).contains(&status) {
            return Err(WorkflowReopenError::Http {
                status,
                body_snippet: response_body.chars().take(200).collect(),
            });
        }

        let snapshot: WorkflowRunSnapshot =
            serde_json::from_str(&response_body).map_err(|error| {
                WorkflowReopenError::Malformed(format!("decode workflow snapshot: {error}"))
            })?;

        // The requested path identity and body identity must agree.
        // Never borrow revision authority from a different WorkflowRun.
        if snapshot.workflow_run_id != workflow_run_id {
            return Err(WorkflowReopenError::Malformed(format!(
                "workflow snapshot identity mismatch: requested '{}' but Runtime returned '{}'",
                workflow_run_id, snapshot.workflow_run_id,
            )));
        }

        // OpenAB performs a defense-in-depth eligibility check before
        // invoking the mutation endpoint. AAP remains the final authority.
        if snapshot.state != TECH_LEAD_WAIT_STATE {
            return Err(WorkflowReopenError::WrongState {
                workflow_run_id: snapshot.workflow_run_id,
                state: snapshot.state,
            });
        }

        let request = WorkflowReopenRequest {
            expected_revision: snapshot.revision,
            reason: TECH_LEAD_REOPEN_REASON.to_string(),
        };

        let request_body = serde_json::to_string(&request).map_err(|error| {
            WorkflowReopenError::Malformed(format!("encode reopen request: {error}"))
        })?;

        let reopen_url = format!(
            "{}/v1/workflows/{}/tech-lead/reopen-work",
            self.base_url, workflow_run_id,
        );

        let (status, response_body) = self
            .http
            .post_json(
                reopen_url,
                self.credential.clone(),
                self.timeout,
                request_body,
            )
            .await?;

        if !(200..300).contains(&status) {
            return Err(WorkflowReopenError::Http {
                status,
                body_snippet: response_body.chars().take(200).collect(),
            });
        }

        let response: WorkflowReopenResponse =
            serde_json::from_str(&response_body).map_err(|error| {
                WorkflowReopenError::Malformed(format!("decode reopen response: {error}"))
            })?;

        // Response identity must remain bound to the WorkflowRun that
        // supplied the authoritative CAS revision.
        if response.predecessor_workflow_run_id != workflow_run_id {
            return Err(WorkflowReopenError::Malformed(format!(
                "reopen predecessor identity mismatch: requested '{}' but Runtime returned '{}'",
                workflow_run_id, response.predecessor_workflow_run_id,
            )));
        }

        Ok(response)
    }

    /// Phase 8.x — bounded-defect correction. Reuses the Runtime
    /// endpoint
    /// ``POST /v1/workflows/{workflow_run_id}/intervention/reopen-primary``.
    ///
    /// Defense-in-depth eligibility checks performed here:
    ///
    /// * GET snapshot, then reject any state other than
    ///   ``VERIFIER_ACTIVE``;
    /// * reject any snapshot whose ``defect_loop_count`` is not the
    ///   canonical value ``1``;
    /// * reject any snapshot whose identity disagrees with the
    ///   requested run id;
    /// * forward the snapshot's authoritative revision as
    ///   ``expected_revision``;
    /// * confirm the Runtime response preserves the SAME
    ///   ``workflow_run_id`` (the intervention does NOT create a
    ///   successor run).
    ///
    /// AAP remains the final authority — these checks only avoid a
    /// wasted round trip on inputs that are obviously ineligible.
    pub async fn reopen_primary(
        &self,
        workflow_run_id: &str,
    ) -> Result<WorkflowInterventionResponse, WorkflowReopenError> {
        let snapshot_url = format!("{}/v1/workflows/{}", self.base_url, workflow_run_id,);

        let (status, response_body) = self
            .http
            .get_json(snapshot_url, self.credential.clone(), self.timeout)
            .await?;

        if !(200..300).contains(&status) {
            return Err(WorkflowReopenError::Http {
                status,
                body_snippet: response_body.chars().take(200).collect(),
            });
        }

        let snapshot: WorkflowRunSnapshot =
            serde_json::from_str(&response_body).map_err(|error| {
                WorkflowReopenError::Malformed(format!("decode workflow snapshot: {error}"))
            })?;

        // The requested path identity and body identity must agree.
        // Never borrow revision authority from a different WorkflowRun.
        if snapshot.workflow_run_id != workflow_run_id {
            return Err(WorkflowReopenError::Malformed(format!(
                "workflow snapshot identity mismatch: requested '{}' but Runtime returned '{}'",
                workflow_run_id, snapshot.workflow_run_id,
            )));
        }

        // Defense-in-depth: state must be exactly ``VERIFIER_ACTIVE``.
        if snapshot.state != VERIFIER_ACTIVE_STATE {
            return Err(WorkflowReopenError::WrongState {
                workflow_run_id: snapshot.workflow_run_id,
                state: snapshot.state,
            });
        }

        // Defense-in-depth: defect_loop_count must be exactly 1.
        if snapshot.defect_loop_count != EXPECTED_DEFECT_LOOP_COUNT {
            return Err(WorkflowReopenError::WrongDefectLoopCount {
                workflow_run_id: snapshot.workflow_run_id,
                defect_loop_count: snapshot.defect_loop_count,
            });
        }

        let request = WorkflowInterventionRequest {
            expected_revision: snapshot.revision,
            reason: TECH_LEAD_INTERVENTION_REASON.to_string(),
        };

        let request_body = serde_json::to_string(&request).map_err(|error| {
            WorkflowReopenError::Malformed(format!("encode intervention request: {error}"))
        })?;

        let intervention_url = format!(
            "{}/v1/workflows/{}/intervention/reopen-primary",
            self.base_url, workflow_run_id,
        );

        let (status, response_body) = self
            .http
            .post_json(
                intervention_url,
                self.credential.clone(),
                self.timeout,
                request_body,
            )
            .await?;

        if !(200..300).contains(&status) {
            return Err(WorkflowReopenError::Http {
                status,
                body_snippet: response_body.chars().take(200).collect(),
            });
        }

        let response: WorkflowInterventionResponse =
            serde_json::from_str(&response_body).map_err(|error| {
                WorkflowReopenError::Malformed(format!("decode intervention response: {error}"))
            })?;

        // Successful intervention MUST preserve the SAME workflow_run_id
        // — the bounded-defect correction mutates the held run, it does
        // NOT create a successor. A mismatched response identity
        // indicates a malformed Runtime response and must fail closed.
        if response.workflow_run_id != workflow_run_id {
            return Err(WorkflowReopenError::Malformed(format!(
                "intervention identity mismatch: requested '{}' but Runtime returned '{}'",
                workflow_run_id, response.workflow_run_id,
            )));
        }

        // The intervention MUST preserve ``defect_loop_count == 1`` so
        // the bounded defect loop is not silently re-armed. A response
        // that flips the counter to 0 indicates a Runtime bug and
        // must fail closed.
        if response.defect_loop_count != EXPECTED_DEFECT_LOOP_COUNT {
            return Err(WorkflowReopenError::Malformed(format!(
                "intervention response defect_loop_count={} violates bounded-loop preservation (must remain 1)",
                response.defect_loop_count
            )));
        }

        Ok(response)
    }
}

#[async_trait::async_trait]
impl WorkflowReopenClient for HttpWorkflowReopenClient {
    async fn reopen_terminal_work(
        &self,
        workflow_run_id: &str,
    ) -> Result<WorkflowReopenResponse, WorkflowReopenError> {
        HttpWorkflowReopenClient::reopen_terminal_work(self, workflow_run_id).await
    }

    async fn reopen_primary(
        &self,
        workflow_run_id: &str,
    ) -> Result<WorkflowInterventionResponse, WorkflowReopenError> {
        HttpWorkflowReopenClient::reopen_primary(self, workflow_run_id).await
    }
}

pub fn build_production_client(
    config: &WorkflowReopenConfig,
) -> Result<HttpWorkflowReopenClient, WorkflowReopenError> {
    HttpWorkflowReopenClient::new(config, Arc::new(ReqwestWorkflowReopenTransport::new()))
}

pub struct ReqwestWorkflowReopenTransport {
    client: reqwest::Client,
}

impl ReqwestWorkflowReopenTransport {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl Default for ReqwestWorkflowReopenTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl WorkflowReopenTransport for ReqwestWorkflowReopenTransport {
    async fn get_json(
        &self,
        url: String,
        bearer_token: String,
        timeout: Duration,
    ) -> Result<(u16, String), WorkflowReopenError> {
        let response = self
            .client
            .get(&url)
            .bearer_auth(bearer_token)
            .timeout(timeout)
            .send()
            .await
            .map_err(map_reqwest_error)?;

        let status = response.status().as_u16();
        let body = response.text().await.map_err(|error| {
            WorkflowReopenError::Malformed(format!("read GET response body: {error}"))
        })?;

        Ok((status, body))
    }

    async fn post_json(
        &self,
        url: String,
        bearer_token: String,
        timeout: Duration,
        body: String,
    ) -> Result<(u16, String), WorkflowReopenError> {
        let response = self
            .client
            .post(&url)
            .bearer_auth(bearer_token)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .timeout(timeout)
            .body(body)
            .send()
            .await
            .map_err(map_reqwest_error)?;

        let status = response.status().as_u16();
        let body = response.text().await.map_err(|error| {
            WorkflowReopenError::Malformed(format!("read POST response body: {error}"))
        })?;

        Ok((status, body))
    }
}

fn map_reqwest_error(error: reqwest::Error) -> WorkflowReopenError {
    if error.is_timeout() {
        WorkflowReopenError::Timeout
    } else {
        WorkflowReopenError::Unreachable(error.to_string())
    }
}

#[cfg(test)]
#[derive(Clone)]
struct FakeWorkflowReopenTransport {
    get_outcome: Result<WorkflowRunSnapshot, WorkflowReopenError>,
    post_outcome_reopen: Result<WorkflowReopenResponse, WorkflowReopenError>,
    post_outcome_intervention: Result<WorkflowInterventionResponse, WorkflowReopenError>,
    post_requests_reopen: Arc<std::sync::Mutex<Vec<WorkflowReopenRequest>>>,
    post_requests_intervention: Arc<std::sync::Mutex<Vec<WorkflowInterventionRequest>>>,
    /// Captures every POST URL the client issues, in order. Tests
    /// inspect the recorded URLs to assert the production surface
    /// (`/v1/workflows/{id}/tech-lead/reopen-work` vs
    /// `/v1/workflows/{id}/intervention/reopen-primary`).
    post_urls: Arc<std::sync::Mutex<Vec<String>>>,
}

#[cfg(test)]
impl FakeWorkflowReopenTransport {
    fn new(snapshot: WorkflowRunSnapshot, response: WorkflowReopenResponse) -> Self {
        Self {
            get_outcome: Ok(snapshot),
            post_outcome_reopen: Ok(response),
            post_outcome_intervention: Err(WorkflowReopenError::Malformed(
                "intervention POST must not be reached in reopen-work test".into(),
            )),
            post_requests_reopen: Arc::new(std::sync::Mutex::new(Vec::new())),
            post_requests_intervention: Arc::new(std::sync::Mutex::new(Vec::new())),
            post_urls: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn new_intervention(
        snapshot: WorkflowRunSnapshot,
        response: WorkflowInterventionResponse,
    ) -> Self {
        Self {
            get_outcome: Ok(snapshot),
            post_outcome_reopen: Err(WorkflowReopenError::Malformed(
                "reopen-work POST must not be reached in intervention test".into(),
            )),
            post_outcome_intervention: Ok(response),
            post_requests_reopen: Arc::new(std::sync::Mutex::new(Vec::new())),
            post_requests_intervention: Arc::new(std::sync::Mutex::new(Vec::new())),
            post_urls: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn get_error(error: WorkflowReopenError) -> Self {
        Self {
            get_outcome: Err(error),
            post_outcome_reopen: Err(WorkflowReopenError::Malformed(
                "POST must not be reached".into(),
            )),
            post_outcome_intervention: Err(WorkflowReopenError::Malformed(
                "POST must not be reached".into(),
            )),
            post_requests_reopen: Arc::new(std::sync::Mutex::new(Vec::new())),
            post_requests_intervention: Arc::new(std::sync::Mutex::new(Vec::new())),
            post_urls: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn post_error(snapshot: WorkflowRunSnapshot, error: WorkflowReopenError) -> Self {
        Self {
            get_outcome: Ok(snapshot),
            post_outcome_reopen: Err(error.clone()),
            post_outcome_intervention: Err(error),
            post_requests_reopen: Arc::new(std::sync::Mutex::new(Vec::new())),
            post_requests_intervention: Arc::new(std::sync::Mutex::new(Vec::new())),
            post_urls: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn reopen_post_call_count(&self) -> usize {
        self.post_requests_reopen.lock().unwrap().len()
    }

    fn intervention_post_call_count(&self) -> usize {
        self.post_requests_intervention.lock().unwrap().len()
    }

    fn recorded_post_urls(&self) -> Vec<String> {
        self.post_urls.lock().unwrap().clone()
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl WorkflowReopenTransport for FakeWorkflowReopenTransport {
    async fn get_json(
        &self,
        _url: String,
        _bearer_token: String,
        _timeout: Duration,
    ) -> Result<(u16, String), WorkflowReopenError> {
        match &self.get_outcome {
            Ok(snapshot) => Ok((
                200,
                serde_json::to_string(snapshot).map_err(|error| {
                    WorkflowReopenError::Malformed(format!("encode fake GET response: {error}"))
                })?,
            )),
            Err(error) => Err(error.clone()),
        }
    }

    async fn post_json(
        &self,
        url: String,
        _bearer_token: String,
        _timeout: Duration,
        body: String,
    ) -> Result<(u16, String), WorkflowReopenError> {
        self.post_urls.lock().unwrap().push(url.clone());

        if url.contains("/intervention/reopen-primary") {
            let request: WorkflowInterventionRequest =
                serde_json::from_str(&body).map_err(|error| {
                    WorkflowReopenError::Malformed(format!(
                        "decode fake intervention POST request: {error}"
                    ))
                })?;

            self.post_requests_intervention
                .lock()
                .unwrap()
                .push(request);

            return match &self.post_outcome_intervention {
                Ok(response) => Ok((
                    200,
                    serde_json::to_string(response).map_err(|error| {
                        WorkflowReopenError::Malformed(format!(
                            "encode fake intervention POST response: {error}"
                        ))
                    })?,
                )),
                Err(error) => Err(error.clone()),
            };
        }

        let request: WorkflowReopenRequest = serde_json::from_str(&body).map_err(|error| {
            WorkflowReopenError::Malformed(format!("decode fake reopen-work POST request: {error}"))
        })?;

        self.post_requests_reopen.lock().unwrap().push(request);

        match &self.post_outcome_reopen {
            Ok(response) => Ok((
                200,
                serde_json::to_string(response).map_err(|error| {
                    WorkflowReopenError::Malformed(format!("encode fake POST response: {error}"))
                })?,
            )),
            Err(error) => Err(error.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn cfg() -> crate::config::WorkflowReopenConfig {
        crate::config::WorkflowReopenConfig {
            aap_runtime_url: "http://127.0.0.1:8000".into(),
            aap_credential_env: "TEST_WORKFLOW_REOPEN_TOKEN".into(),
            request_timeout_seconds: 5,
        }
    }

    fn success_response(predecessor_revision: u64) -> WorkflowReopenResponse {
        WorkflowReopenResponse {
            predecessor_workflow_run_id: "wfr-old".into(),
            predecessor_state: "TECH_LEAD_WAIT".into(),
            predecessor_revision,
            successor_workflow_run_id: "wfr-new".into(),
            successor_state: "PRIMARY_ACTIVE".into(),
            successor_revision: 1,
            successor_defect_loop_count: 0,
            actor_client_id: "openab".into(),
            reason: "TECH_LEAD_POST_REVIEW_REOPEN".into(),
        }
    }

    #[tokio::test]
    async fn tech_lead_wait_snapshot_posts_authoritative_revision() {
        let _env_guard = ENV_LOCK.lock().await;
        let transport = Arc::new(FakeWorkflowReopenTransport::new(
            WorkflowRunSnapshot {
                workflow_run_id: "wfr-old".into(),
                state: "TECH_LEAD_WAIT".into(),
                revision: 6,
                defect_loop_count: 0,
            },
            success_response(6),
        ));

        std::env::set_var("TEST_WORKFLOW_REOPEN_TOKEN", "test-token-not-real");

        let client =
            HttpWorkflowReopenClient::new(&cfg(), transport.clone()).expect("client must build");

        let result = client
            .reopen_terminal_work("wfr-old")
            .await
            .expect("reopen must succeed");

        assert_eq!(result.successor_workflow_run_id, "wfr-new");

        assert_eq!(
            transport.reopen_post_call_count(),
            1,
            "eligible TECH_LEAD_WAIT workflow must POST exactly once"
        );

        let urls = transport.recorded_post_urls();
        assert_eq!(
            urls,
            vec!["http://127.0.0.1:8000/v1/workflows/wfr-old/tech-lead/reopen-work"],
            "reopen-work must POST to the canonical terminal-work endpoint",
        );

        let request = transport.post_requests_reopen.lock().unwrap()[0].clone();

        assert_eq!(
            request.expected_revision, 6,
            "POST CAS must use revision returned by authoritative GET",
        );
        assert_eq!(request.reason, "TECH_LEAD_POST_REVIEW_REOPEN");
    }

    #[tokio::test]
    async fn non_tech_lead_wait_snapshot_never_posts() {
        let _env_guard = ENV_LOCK.lock().await;
        let transport = Arc::new(FakeWorkflowReopenTransport::new(
            WorkflowRunSnapshot {
                workflow_run_id: "wfr-old".into(),
                state: "PRIMARY_ACTIVE".into(),
                revision: 9,
                defect_loop_count: 0,
            },
            success_response(9),
        ));

        std::env::set_var("TEST_WORKFLOW_REOPEN_TOKEN", "test-token-not-real");

        let client =
            HttpWorkflowReopenClient::new(&cfg(), transport.clone()).expect("client must build");

        let error = client
            .reopen_terminal_work("wfr-old")
            .await
            .expect_err("non TECH_LEAD_WAIT must fail closed");

        assert!(matches!(error, WorkflowReopenError::WrongState { .. }));

        assert_eq!(
            transport.reopen_post_call_count(),
            0,
            "wrong-state workflow must never reach mutation POST"
        );
    }

    #[tokio::test]
    async fn get_failure_never_posts() {
        let _env_guard = ENV_LOCK.lock().await;
        let transport = Arc::new(FakeWorkflowReopenTransport::get_error(
            WorkflowReopenError::Timeout,
        ));

        std::env::set_var("TEST_WORKFLOW_REOPEN_TOKEN", "test-token-not-real");

        let client =
            HttpWorkflowReopenClient::new(&cfg(), transport.clone()).expect("client must build");

        let error = client
            .reopen_terminal_work("wfr-old")
            .await
            .expect_err("GET failure must fail closed");

        assert!(matches!(error, WorkflowReopenError::Timeout));

        assert_eq!(
            transport.reopen_post_call_count(),
            0,
            "GET failure must never reach mutation POST"
        );
    }

    #[tokio::test]
    async fn post_failure_is_returned_and_not_hidden() {
        let _env_guard = ENV_LOCK.lock().await;
        let transport = Arc::new(FakeWorkflowReopenTransport::post_error(
            WorkflowRunSnapshot {
                workflow_run_id: "wfr-old".into(),
                state: "TECH_LEAD_WAIT".into(),
                revision: 12,
                defect_loop_count: 0,
            },
            WorkflowReopenError::Http {
                status: 409,
                body_snippet: "stale revision".into(),
            },
        ));

        std::env::set_var("TEST_WORKFLOW_REOPEN_TOKEN", "test-token-not-real");

        let client =
            HttpWorkflowReopenClient::new(&cfg(), transport.clone()).expect("client must build");

        let error = client
            .reopen_terminal_work("wfr-old")
            .await
            .expect_err("POST conflict must surface");

        assert!(matches!(
            error,
            WorkflowReopenError::Http { status: 409, .. }
        ));

        assert_eq!(
            transport.reopen_post_call_count(),
            1,
            "eligible snapshot reaches POST exactly once"
        );

        let request = transport.post_requests_reopen.lock().unwrap()[0].clone();

        assert_eq!(
            request.expected_revision, 12,
            "POST CAS must use revision returned by authoritative GET"
        );
    }

    #[tokio::test]
    async fn snapshot_identity_mismatch_never_posts() {
        let _env_guard = ENV_LOCK.lock().await;
        let transport = Arc::new(FakeWorkflowReopenTransport::new(
            WorkflowRunSnapshot {
                workflow_run_id: "wfr-other".into(),
                state: "TECH_LEAD_WAIT".into(),
                revision: 6,
                defect_loop_count: 0,
            },
            success_response(6),
        ));

        std::env::set_var("TEST_WORKFLOW_REOPEN_TOKEN", "test-token-not-real");

        let client =
            HttpWorkflowReopenClient::new(&cfg(), transport.clone()).expect("client must build");

        let error = client
            .reopen_terminal_work("wfr-old")
            .await
            .expect_err("identity mismatch must fail closed");

        assert!(matches!(error, WorkflowReopenError::Malformed(_)));

        assert_eq!(
            transport.reopen_post_call_count(),
            0,
            "mismatched GET identity must never reach POST"
        );
    }

    #[tokio::test]
    async fn missing_credential_fails_closed_at_construction() {
        let _env_guard = ENV_LOCK.lock().await;
        std::env::remove_var("TEST_WORKFLOW_REOPEN_TOKEN");

        let transport = Arc::new(FakeWorkflowReopenTransport::new(
            WorkflowRunSnapshot {
                workflow_run_id: "wfr-old".into(),
                state: "TECH_LEAD_WAIT".into(),
                revision: 1,
                defect_loop_count: 0,
            },
            success_response(1),
        ));

        let result = HttpWorkflowReopenClient::new(&cfg(), transport);

        assert!(matches!(result, Err(WorkflowReopenError::AuthMissing)));
    }

    // ============================================================
    // Phase 8.x — bounded-defect intervention (`reopen-primary`) tests
    // ============================================================

    fn intervention_success_response(
        workflow_run_id: &str,
        revision: u64,
    ) -> WorkflowInterventionResponse {
        WorkflowInterventionResponse {
            workflow_run_id: workflow_run_id.into(),
            previous_state: "VERIFIER_ACTIVE".into(),
            state: "PRIMARY_ACTIVE".into(),
            previous_revision: revision,
            revision,
            previous_defect_loop_count: 1,
            defect_loop_count: 1,
            previous_hold_error_code: Some("BOUNDED_DEFECT_LOOP_EXHAUSTED".into()),
            cleared_hold_rows: 1,
            scheduler_woken: true,
            actor_client_id: "openab".into(),
            reason: "BOUNDED_DEFECT_LOOP_TECH_LEAD_CORRECTION".into(),
        }
    }

    /// Test 6: `VERIFIER_ACTIVE`, revision N, defect_loop_count=1 succeeds.
    #[tokio::test]
    async fn verifier_active_with_defect_loop_count_one_posts_intervention() {
        let _env_guard = ENV_LOCK.lock().await;
        let transport = Arc::new(FakeWorkflowReopenTransport::new_intervention(
            WorkflowRunSnapshot {
                workflow_run_id: "wfr938bda206fa7d21d".into(),
                state: "VERIFIER_ACTIVE".into(),
                revision: 7,
                defect_loop_count: 1,
            },
            intervention_success_response("wfr938bda206fa7d21d", 7),
        ));

        std::env::set_var("TEST_WORKFLOW_REOPEN_TOKEN", "test-token-not-real");

        let client =
            HttpWorkflowReopenClient::new(&cfg(), transport.clone()).expect("client must build");

        let response = client
            .reopen_primary("wfr938bda206fa7d21d")
            .await
            .expect("reopen-primary must succeed for eligible snapshot");

        // Test 9: response preserves same workflow_run_id.
        assert_eq!(response.workflow_run_id, "wfr938bda206fa7d21d");
        assert_eq!(response.state, "PRIMARY_ACTIVE");
        // The bounded defect loop is preserved — NOT silently re-armed.
        assert_eq!(response.defect_loop_count, 1);
        assert!(response.scheduler_woken);

        // The intervention POST was issued exactly once.
        assert_eq!(
            transport.intervention_post_call_count(),
            1,
            "eligible VERIFIER_ACTIVE workflow must POST exactly once",
        );

        // Test 8: Runtime endpoint path is
        // `/v1/workflows/{id}/intervention/reopen-primary`.
        assert_eq!(
            transport.recorded_post_urls(),
            vec!["http://127.0.0.1:8000/v1/workflows/wfr938bda206fa7d21d/intervention/reopen-primary"],
        );

        // Test 7: expected_revision is forwarded from the authoritative
        // GET snapshot — NOT caller-supplied.
        let request = transport.post_requests_intervention.lock().unwrap()[0].clone();
        assert_eq!(request.expected_revision, 7);
        assert_eq!(request.reason, "BOUNDED_DEFECT_LOOP_TECH_LEAD_CORRECTION");

        // Reopen-work is not called when reopen-primary is invoked.
        assert_eq!(
            transport.reopen_post_call_count(),
            0,
            "reopen-work POST must never be reached when reopen-primary is invoked",
        );
    }

    /// Test 10: wrong state rejected (defense-in-depth).
    #[tokio::test]
    async fn non_verifier_active_snapshot_never_posts_reopen_primary() {
        let _env_guard = ENV_LOCK.lock().await;
        for state in ["PRIMARY_ACTIVE", "TECH_LEAD_WAIT", "FINAL_REVIEWER_ACTIVE"] {
            let transport = Arc::new(FakeWorkflowReopenTransport::new_intervention(
                WorkflowRunSnapshot {
                    workflow_run_id: "wfr938bda206fa7d21d".into(),
                    state: state.into(),
                    revision: 4,
                    defect_loop_count: 1,
                },
                intervention_success_response("wfr938bda206fa7d21d", 4),
            ));

            std::env::set_var("TEST_WORKFLOW_REOPEN_TOKEN", "test-token-not-real");

            let client = HttpWorkflowReopenClient::new(&cfg(), transport.clone())
                .expect("client must build");

            let error = client
                .reopen_primary("wfr938bda206fa7d21d")
                .await
                .expect_err("non VERIFIER_ACTIVE must fail closed");

            assert!(
                matches!(error, WorkflowReopenError::WrongState { .. }),
                "state={state} expected WrongState, got {error:?}",
            );

            assert_eq!(
                transport.intervention_post_call_count(),
                0,
                "wrong-state workflow (state={state}) must never reach intervention POST",
            );
        }
    }

    /// Test 11: defect_loop_count != 1 rejected.
    #[tokio::test]
    async fn defect_loop_count_not_one_never_posts_reopen_primary() {
        let _env_guard = ENV_LOCK.lock().await;
        for defect_loop_count in [0u64, 2u64] {
            let transport = Arc::new(FakeWorkflowReopenTransport::new_intervention(
                WorkflowRunSnapshot {
                    workflow_run_id: "wfr938bda206fa7d21d".into(),
                    state: "VERIFIER_ACTIVE".into(),
                    revision: 4,
                    defect_loop_count,
                },
                intervention_success_response("wfr938bda206fa7d21d", 4),
            ));

            std::env::set_var("TEST_WORKFLOW_REOPEN_TOKEN", "test-token-not-real");

            let client = HttpWorkflowReopenClient::new(&cfg(), transport.clone())
                .expect("client must build");

            let error = client
                .reopen_primary("wfr938bda206fa7d21d")
                .await
                .expect_err("defect_loop_count != 1 must fail closed");

            assert!(
                matches!(error, WorkflowReopenError::WrongDefectLoopCount { .. }),
                "defect_loop_count={defect_loop_count} expected WrongDefectLoopCount, got {error:?}",
            );

            assert_eq!(
                transport.intervention_post_call_count(),
                0,
                "defect_loop_count={defect_loop_count} must never reach intervention POST",
            );
        }
    }

    /// Test 12: stale revision rejected — Runtime 409 surfaces as
    /// `Http { status: 409 }` and the request is recorded with the
    /// authoritative snapshot revision.
    #[tokio::test]
    async fn stale_revision_runtime_409_surfaces_for_reopen_primary() {
        let _env_guard = ENV_LOCK.lock().await;
        let transport = Arc::new(FakeWorkflowReopenTransport::post_error(
            WorkflowRunSnapshot {
                workflow_run_id: "wfr938bda206fa7d21d".into(),
                state: "VERIFIER_ACTIVE".into(),
                revision: 4,
                defect_loop_count: 1,
            },
            WorkflowReopenError::Http {
                status: 409,
                body_snippet: "stale revision".into(),
            },
        ));

        std::env::set_var("TEST_WORKFLOW_REOPEN_TOKEN", "test-token-not-real");

        let client =
            HttpWorkflowReopenClient::new(&cfg(), transport.clone()).expect("client must build");

        let error = client
            .reopen_primary("wfr938bda206fa7d21d")
            .await
            .expect_err("stale revision must surface");

        assert!(
            matches!(error, WorkflowReopenError::Http { status: 409, .. }),
            "expected Http {{ status: 409 }}, got {error:?}",
        );

        // The request reached Runtime with the authoritative snapshot
        // revision, NOT a stale or caller-supplied one.
        assert_eq!(
            transport.post_requests_intervention.lock().unwrap()[0].expected_revision,
            4,
        );
    }

    /// Test: snapshot identity mismatch is rejected before the POST.
    #[tokio::test]
    async fn snapshot_identity_mismatch_never_posts_reopen_primary() {
        let _env_guard = ENV_LOCK.lock().await;
        let transport = Arc::new(FakeWorkflowReopenTransport::new_intervention(
            WorkflowRunSnapshot {
                workflow_run_id: "wfr-other".into(),
                state: "VERIFIER_ACTIVE".into(),
                revision: 4,
                defect_loop_count: 1,
            },
            intervention_success_response("wfr938bda206fa7d21d", 4),
        ));

        std::env::set_var("TEST_WORKFLOW_REOPEN_TOKEN", "test-token-not-real");

        let client =
            HttpWorkflowReopenClient::new(&cfg(), transport.clone()).expect("client must build");

        let error = client
            .reopen_primary("wfr938bda206fa7d21d")
            .await
            .expect_err("identity mismatch must fail closed");

        assert!(matches!(error, WorkflowReopenError::Malformed(_)));

        assert_eq!(
            transport.intervention_post_call_count(),
            0,
            "mismatched GET identity must never reach intervention POST",
        );
    }

    /// Test: GET failure never reaches the intervention POST.
    #[tokio::test]
    async fn get_failure_never_posts_reopen_primary() {
        let _env_guard = ENV_LOCK.lock().await;
        let transport = Arc::new(FakeWorkflowReopenTransport::get_error(
            WorkflowReopenError::Timeout,
        ));

        std::env::set_var("TEST_WORKFLOW_REOPEN_TOKEN", "test-token-not-real");

        let client =
            HttpWorkflowReopenClient::new(&cfg(), transport.clone()).expect("client must build");

        let error = client
            .reopen_primary("wfr938bda206fa7d21d")
            .await
            .expect_err("GET failure must fail closed");

        assert!(matches!(error, WorkflowReopenError::Timeout));

        assert_eq!(
            transport.intervention_post_call_count(),
            0,
            "GET failure must never reach intervention POST",
        );
    }

    /// Test: a Runtime response that flips `defect_loop_count` to 0
    /// indicates a Runtime bug — must fail closed, NOT silently
    /// re-arm the bounded defect loop.
    #[tokio::test]
    async fn response_defect_loop_count_must_remain_one() {
        let _env_guard = ENV_LOCK.lock().await;
        let mut response = intervention_success_response("wfr938bda206fa7d21d", 4);
        response.defect_loop_count = 0;
        response.previous_defect_loop_count = 1;

        let transport = Arc::new(FakeWorkflowReopenTransport::new_intervention(
            WorkflowRunSnapshot {
                workflow_run_id: "wfr938bda206fa7d21d".into(),
                state: "VERIFIER_ACTIVE".into(),
                revision: 4,
                defect_loop_count: 1,
            },
            response,
        ));

        std::env::set_var("TEST_WORKFLOW_REOPEN_TOKEN", "test-token-not-real");

        let client =
            HttpWorkflowReopenClient::new(&cfg(), transport.clone()).expect("client must build");

        let error = client
            .reopen_primary("wfr938bda206fa7d21d")
            .await
            .expect_err("response that breaks bounded-loop preservation must fail closed");

        assert!(
            matches!(error, WorkflowReopenError::Malformed(_)),
            "expected Malformed, got {error:?}",
        );
    }

    /// Test: a Runtime response whose `workflow_run_id` differs from
    /// the requested run id must fail closed (the bounded-defect
    /// intervention mutates the SAME run; a different id is malformed).
    #[tokio::test]
    async fn response_identity_must_match_requested_run() {
        let _env_guard = ENV_LOCK.lock().await;
        let mut response = intervention_success_response("wfr-other", 4);
        response.workflow_run_id = "wfr-other".into();

        let transport = Arc::new(FakeWorkflowReopenTransport::new_intervention(
            WorkflowRunSnapshot {
                workflow_run_id: "wfr938bda206fa7d21d".into(),
                state: "VERIFIER_ACTIVE".into(),
                revision: 4,
                defect_loop_count: 1,
            },
            response,
        ));

        std::env::set_var("TEST_WORKFLOW_REOPEN_TOKEN", "test-token-not-real");

        let client =
            HttpWorkflowReopenClient::new(&cfg(), transport.clone()).expect("client must build");

        let error = client
            .reopen_primary("wfr938bda206fa7d21d")
            .await
            .expect_err("identity mismatch in response must fail closed");

        assert!(
            matches!(error, WorkflowReopenError::Malformed(_)),
            "expected Malformed, got {error:?}",
        );
    }
}
