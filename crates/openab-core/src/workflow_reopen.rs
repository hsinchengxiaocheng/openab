//! Discord Tech Lead -> AAP terminal-work reopen control-plane client.
//!
//! This module is deliberately separate from autonomous ingress:
//!
//! * autonomous ingress creates/continues ordinary workflow work;
//! * workflow reopen is an explicit mutation authority;
//! * only an already-authorized Tech Lead Discord turn may reach it;
//! * the Runtime remains authoritative for WorkflowRun state/revision;
//! * failures never fall through to ordinary ACP.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

use crate::config::WorkflowReopenConfig;

const TECH_LEAD_WAIT_STATE: &str = "TECH_LEAD_WAIT";
const TECH_LEAD_REOPEN_REASON: &str = "TECH_LEAD_POST_REVIEW_REOPEN";

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
    async fn reopen_terminal_work(
        &self,
        workflow_run_id: &str,
    ) -> Result<WorkflowReopenResponse, WorkflowReopenError>;
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
}

#[async_trait::async_trait]
impl WorkflowReopenClient for HttpWorkflowReopenClient {
    async fn reopen_terminal_work(
        &self,
        workflow_run_id: &str,
    ) -> Result<WorkflowReopenResponse, WorkflowReopenError> {
        HttpWorkflowReopenClient::reopen_terminal_work(self, workflow_run_id).await
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
    post_outcome: Result<WorkflowReopenResponse, WorkflowReopenError>,
    post_requests: Arc<std::sync::Mutex<Vec<WorkflowReopenRequest>>>,
}

#[cfg(test)]
impl FakeWorkflowReopenTransport {
    fn new(snapshot: WorkflowRunSnapshot, response: WorkflowReopenResponse) -> Self {
        Self {
            get_outcome: Ok(snapshot),
            post_outcome: Ok(response),
            post_requests: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn get_error(error: WorkflowReopenError) -> Self {
        Self {
            get_outcome: Err(error),
            post_outcome: Err(WorkflowReopenError::Malformed(
                "POST must not be reached".into(),
            )),
            post_requests: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn post_error(snapshot: WorkflowRunSnapshot, error: WorkflowReopenError) -> Self {
        Self {
            get_outcome: Ok(snapshot),
            post_outcome: Err(error),
            post_requests: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn post_call_count(&self) -> usize {
        self.post_requests.lock().unwrap().len()
    }

    fn last_post_request(&self) -> Option<WorkflowReopenRequest> {
        self.post_requests.lock().unwrap().last().cloned()
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
        _url: String,
        _bearer_token: String,
        _timeout: Duration,
        body: String,
    ) -> Result<(u16, String), WorkflowReopenError> {
        let request: WorkflowReopenRequest = serde_json::from_str(&body).map_err(|error| {
            WorkflowReopenError::Malformed(format!("decode fake POST request: {error}"))
        })?;

        self.post_requests.lock().unwrap().push(request);

        match &self.post_outcome {
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
            transport.post_call_count(),
            1,
            "eligible TECH_LEAD_WAIT workflow must POST exactly once"
        );

        let request = transport
            .last_post_request()
            .expect("POST request must be recorded");

        assert_eq!(request.expected_revision, 6);
        assert_eq!(request.reason, "TECH_LEAD_POST_REVIEW_REOPEN");
    }

    #[tokio::test]
    async fn non_tech_lead_wait_snapshot_never_posts() {
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
            transport.post_call_count(),
            0,
            "wrong-state workflow must never reach mutation POST"
        );
    }

    #[tokio::test]
    async fn get_failure_never_posts() {
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
            transport.post_call_count(),
            0,
            "GET failure must never reach mutation POST"
        );
    }

    #[tokio::test]
    async fn post_failure_is_returned_and_not_hidden() {
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
            transport.post_call_count(),
            1,
            "eligible snapshot reaches POST exactly once"
        );

        let request = transport
            .last_post_request()
            .expect("POST request must be recorded");

        assert_eq!(
            request.expected_revision, 12,
            "POST CAS must use revision returned by authoritative GET"
        );
    }

    #[tokio::test]
    async fn snapshot_identity_mismatch_never_posts() {
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
            transport.post_call_count(),
            0,
            "mismatched GET identity must never reach POST"
        );
    }

    #[test]
    fn missing_credential_fails_closed_at_construction() {
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
}
