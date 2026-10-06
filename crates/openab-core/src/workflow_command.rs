//! M24 Phase 1.7.1 — Discord native ``/workflow`` slash-command adapter.
//!
//! This module is the **thin** OpenAB-side adapter for the canonical
//! Runtime ``/workflow`` slash-command surface. It MUST NOT
//! reimplement authorization, expected-revision CAS, reason
//! validation, binding checks, self-verification rules, or lifecycle
//! mutation logic — those invariants live in AAP Runtime's
//! ``WorkflowCommandRouter`` (Phase 1.7) and the B1-B4 services it
//! delegates to. This adapter only:
//!
//! 1. Translates a Discord ``Interaction::Command`` for the
//!    ``/workflow`` top-level application command into the textual
//!    ``/workflow <sub> <workflow_run_id> --flag value`` form that the
//!    canonical Runtime OpenClaw bridge already recognises.
//! 2. Forwards the synthesized message to Runtime via the existing
//!    ``POST /v1/integrations/openclaw/turn`` endpoint so the
//!    canonical ``WorkflowCommandRouter`` stays the single mutation
//!    authority for the bounded workflow-control surface.
//! 3. Projects the structured Runtime response (delivered outcome
//!    or typed rejection) onto a flat ``WorkflowCommandAdapterResult``
//!    so the Discord handler can render an ephemeral reply without
//!    re-parsing the OpenClaw envelope.
//!
//! ## Architecture invariant
//!
//! ```text
//! Discord ``Interaction::Command`` (cmd.data.name == "workflow")
//!      |
//!      v
//! Handler::handle_workflow_command  (defer + followup pattern)
//!      |
//!      v
//! WorkflowCommandAdapter::dispatch   (this module)
//!      |
//!      |   build  /workflow <sub> <id> --flag value text
//!      |   POST   {aap_runtime_url}/v1/integrations/openclaw/turn
//!      |           (user_id = discord_user_id)
//!      v
//! AAP Runtime  OpenClawBridgeService.execute
//!      |
//!      |   intercepts "/workflow" prefix
//!      v
//! WorkflowCommandRouter.handle(discord_sender_id)
//!      |
//!      v
//! B1 WorkflowControlReadService  (status, agents)
//! B2 WorkflowReopenPrimaryService
//! B3 WorkflowReopenWorkService
//! B4 WorkflowTopologyReconfigureCapability
//! ```
//!
//! The Tech Lead identity gate, the expected-revision CAS, the
//! canonical reason vocabulary, the conversation binding check, and
//! the self-verification refusal for ``/workflow reconfigure`` all
//! live in Runtime; this adapter preserves each by forwarding the
//! synthesized message verbatim and surfacing the typed rejection
//! verbatim.
//!
//! ## Fail-closed contract
//!
//! Every failure mode surfaces a typed ``WorkflowCommandAdapterResult``:
//!
//! * **Parse failure** (missing/invalid Discord option) —
//!   ``TypedRejection { reason: WorkflowCommandRejectionReason::UnknownCommand / ... }``.
//! * **Auth missing** — ``TypedRejection { reason: AuthMissing }``.
//! * **Transport failure** (unreachable / timeout / 5xx / 4xx) —
//!   ``TypedRejection { reason: RuntimeUnreachable / RuntimeHttp { ... } / RuntimeMalformed }``.
//! * **Runtime rejection** (e.g. ``UNAUTHORIZED_SENDER``,
//!   ``MISSING_EXPECTED_REVISION``, ``REOPEN_REJECTED``) — projected
//!   verbatim from the OpenClaw metadata envelope so the Discord
//!   reply carries the canonical reason token.
//!
//! The adapter MUST NEVER fall through to the ordinary ACP / chat
//! path on failure: the bounded surface refuses on every error.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::config::AapControlPlaneConfig;

/// Dedicated credential for trusted OpenAB -> AAP native Discord
/// Interaction::Command ingress.
///
/// This MUST remain distinct from the generic AAP control-plane bearer.
/// Possession of ARTHUR_AGENT_KEY_OPENAB must not confer native Discord
/// Tech Lead authority.
pub const DISCORD_NATIVE_INTERACTION_KEY_ENV: &str = "ARTHUR_OPENAB_DISCORD_NATIVE_INTERACTION_KEY";

// ---------------------------------------------------------------------------
// Canonical subcommand vocabulary — locked to Runtime's closed set.
// ---------------------------------------------------------------------------

/// Closed set of subcommand names the native ``/workflow`` Application
/// Command registers with Discord.
///
/// Mirrors :class:`runtime.integrations.workflow_command_router.WorkflowCommandName`
/// in AAP Runtime. The string form is the canonical Discord subcommand
/// name (``status`` / ``agents`` / ``reopen-primary`` / ``reopen-work``
/// / ``reconfigure``).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowSubcommand {
    Status,
    Agents,
    ReopenPrimary,
    ReopenWork,
    Reconfigure,
}

impl WorkflowSubcommand {
    /// Canonical Discord subcommand name. Lowercase, hyphen-separated.
    pub fn as_str(&self) -> &'static str {
        match self {
            WorkflowSubcommand::Status => "status",
            WorkflowSubcommand::Agents => "agents",
            WorkflowSubcommand::ReopenPrimary => "reopen-primary",
            WorkflowSubcommand::ReopenWork => "reopen-work",
            WorkflowSubcommand::Reconfigure => "reconfigure",
        }
    }

    /// Parse a Discord subcommand name into the canonical enum.
    /// ``None`` means the name is not one of the closed subcommands the
    /// native ``/workflow`` Application Command registered.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "status" => Some(WorkflowSubcommand::Status),
            "agents" => Some(WorkflowSubcommand::Agents),
            "reopen-primary" => Some(WorkflowSubcommand::ReopenPrimary),
            "reopen-work" => Some(WorkflowSubcommand::ReopenWork),
            "reconfigure" => Some(WorkflowSubcommand::Reconfigure),
            _ => None,
        }
    }

    /// ``true`` when the subcommand is a mutation command. Reads
    /// (``status`` / ``agents``) are NOT gated by the Tech Lead
    /// identity check in Runtime; mutations are.
    pub fn is_mutation(&self) -> bool {
        matches!(
            self,
            WorkflowSubcommand::ReopenPrimary
                | WorkflowSubcommand::ReopenWork
                | WorkflowSubcommand::Reconfigure
        )
    }
}

// ---------------------------------------------------------------------------
// Required Discord option name — locked vocabulary for the bounded
// Application Command schema. Option order on Discord is irrelevant;
// the adapter validates each option by ``name``.
// ---------------------------------------------------------------------------

/// Closed set of option names the native ``/workflow`` Application
/// Command declares. Each option is a ``String`` (Discord command
/// option type). Options are forwarded into the textual
/// ``--flag value`` form Runtime parses.
pub const OPTION_WORKFLOW_RUN_ID: &str = "workflow_run_id";
pub const OPTION_EXPECTED_REVISION: &str = "expected_revision";
pub const OPTION_REASON: &str = "reason";
pub const OPTION_CORRECTION_SPEC: &str = "correction_spec";
pub const OPTION_PRIMARY: &str = "primary";
pub const OPTION_VERIFIER: &str = "verifier";
pub const OPTION_FINAL_REVIEWER: &str = "final_reviewer";
pub const OPTION_BINDING: &str = "binding";

// ---------------------------------------------------------------------------
// Runtime-side slash command flag vocabulary.
//
// Discord option names use snake_case (per the API style guide); the
// canonical Runtime parser (parse_workflow_slash_command) accepts the
// hyphenated flag form for ``expected-revision``,
// ``correction-spec``, and ``final-reviewer``. The adapter MUST
// translate the Discord-side name to the Runtime-side flag so the
// bridge sees the canonical token and forwards it to the B-slice
// services unchanged.
// ---------------------------------------------------------------------------

pub const RUNTIME_FLAG_WORKFLOW_RUN_ID: &str = "workflow_run_id";
pub const RUNTIME_FLAG_EXPECTED_REVISION: &str = "expected-revision";
pub const RUNTIME_FLAG_REASON: &str = "reason";
pub const RUNTIME_FLAG_CORRECTION_SPEC: &str = "correction-spec";
pub const RUNTIME_FLAG_PRIMARY: &str = "primary";
pub const RUNTIME_FLAG_VERIFIER: &str = "verifier";
pub const RUNTIME_FLAG_FINAL_REVIEWER: &str = "final-reviewer";
pub const RUNTIME_FLAG_BINDING: &str = "binding";

// ---------------------------------------------------------------------------
// Adapter result — flat projection of Runtime's OpenClaw envelope.
// ---------------------------------------------------------------------------

/// Stable rejection tokens the adapter surfaces when the bounded
/// surface fails BEFORE the Runtime router can classify the inbound.
///
/// These tokens are intentionally distinct from Runtime's
/// ``WorkflowCommandRejectionReason`` so a Discord observer can
/// distinguish adapter-side errors (missing option, auth missing,
/// transport failure) from Runtime-side rejections (which the adapter
/// passes through verbatim).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdapterRejectionReason {
    /// Discord subcommand name is not in the closed set the
    /// Application Command registered.
    UnknownSubcommand,
    /// ``workflow_run_id`` option was missing or empty.
    MissingWorkflowRunId,
    /// A required option was missing or empty.
    MissingRequiredOption(&'static str),
    /// ``expected_revision`` was present but not a valid non-negative
    /// integer.
    InvalidExpectedRevision,
    /// Adapter is not wired (config missing) — fail closed.
    AdapterUnwired,
    /// Bearer credential env var is missing or empty — fail closed.
    AuthMissing,
    /// HTTP transport failed (unreachable, DNS, etc.).
    RuntimeUnreachable(String),
    /// AAP Runtime returned a non-2xx response.
    RuntimeHttp { status: u16, body_snippet: String },
    /// AAP Runtime returned a 2xx response that could not be decoded
    /// into the expected OpenClaw envelope.
    RuntimeMalformed(String),
}

impl AdapterRejectionReason {
    pub fn token(&self) -> &'static str {
        match self {
            AdapterRejectionReason::UnknownSubcommand => "unknown_subcommand",
            AdapterRejectionReason::MissingWorkflowRunId => "missing_workflow_run_id",
            AdapterRejectionReason::MissingRequiredOption(_) => "missing_required_option",
            AdapterRejectionReason::InvalidExpectedRevision => "invalid_expected_revision",
            AdapterRejectionReason::AdapterUnwired => "adapter_unwired",
            AdapterRejectionReason::AuthMissing => "auth_missing",
            AdapterRejectionReason::RuntimeUnreachable(_) => "runtime_unreachable",
            AdapterRejectionReason::RuntimeHttp { .. } => "runtime_http",
            AdapterRejectionReason::RuntimeMalformed(_) => "runtime_malformed",
        }
    }
}

/// Structured outcome of a native ``/workflow`` dispatch.
#[derive(Debug, Clone, PartialEq)]
pub enum WorkflowCommandAdapterResult {
    /// Runtime accepted the command. ``message`` is the canonical
    /// ``OpenClawTurnResponse.message`` (deterministic human-readable
    /// summary); ``runtime_response`` is the optional
    /// ``metadata.workflow_command_runtime_response`` payload so the
    /// adapter can surface it on the Discord reply.
    Delivered {
        message: String,
        runtime_response: Option<serde_json::Value>,
    },
    /// Runtime rejected the command. ``reason`` is the canonical
    /// ``WorkflowCommandRejectionReason`` token projected from the
    /// Runtime envelope; ``message`` preserves Runtime's canonical
    /// human-readable operator guidance; ``detail`` is the optional
    /// ``metadata.workflow_command_detail`` payload.
    RuntimeRejection {
        reason: String,
        message: Option<String>,
        detail: Option<String>,
    },
    /// Adapter-side failure BEFORE Runtime could classify the
    /// inbound.
    AdapterRejection {
        reason: AdapterRejectionReason,
        detail: Option<String>,
    },
}

impl WorkflowCommandAdapterResult {
    /// Convenience predicate — ``true`` only when the canonical Runtime
    /// control service accepted the dispatch.
    pub fn is_delivered(&self) -> bool {
        matches!(self, WorkflowCommandAdapterResult::Delivered { .. })
    }

    /// Stable short token identifying the outcome family.
    pub fn outcome_token(&self) -> &'static str {
        match self {
            WorkflowCommandAdapterResult::Delivered { .. } => "delivered",
            WorkflowCommandAdapterResult::RuntimeRejection { .. } => "runtime_rejection",
            WorkflowCommandAdapterResult::AdapterRejection { .. } => "adapter_rejection",
        }
    }
}

// ---------------------------------------------------------------------------
// OpenClaw envelope — minimal mirror of Runtime's
// ``OpenClawTurnRequestModel`` / ``OpenClawTurnResponseModel`` shapes.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
struct OpenClawTurnRequestBody<'a> {
    message: String,
    channel: &'a str,
    channel_session_id: String,
    user_id: Option<String>,
    request_id: Option<String>,
    metadata: OpenClawTurnMetadata<'a>,
}

#[derive(Debug, Clone, Serialize)]
struct OpenClawTurnMetadata<'a> {
    /// Native Discord ``/workflow`` adapter surface — the canonical
    /// Tech Lead control path. Pinned to ``"discord_native"`` so the
    /// Runtime audit sink can distinguish a native ``/workflow``
    /// dispatch from a plain-text OpenClaw bridge interception.
    workflow_command_dispatch_source: &'static str,
    /// Closed set of subcommand names the Runtime bridge must accept.
    /// The bridge ignores unknown source labels, so the metadata is
    /// observational only.
    workflow_command_dispatch_subcommand: &'a str,
    workflow_command_dispatch_is_mutation: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct OpenClawTurnResponseBody {
    status: String,
    message: String,
    #[serde(default)]
    metadata: serde_json::Value,
}

/// Response envelope returned by
/// POST /v1/integrations/openclaw/discord/workflow-interaction.
///
/// This is intentionally separate from OpenClawTurnResponseBody: the
/// native Discord ingress has its own bounded response contract and
/// must never be interpreted as a generic chat turn.
#[derive(Debug, Clone, Deserialize)]
struct DiscordWorkflowInteractionResponseBody {
    delivered: bool,
    reason: String,
    #[serde(default)]
    ephemeral: bool,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    metadata: serde_json::Value,
    #[serde(default)]
    detail: Option<String>,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    workflow_run_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Transport abstraction — mirrors `WorkflowReopenTransport` so the
// adapter is unit-testable without a real HTTP server.
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
pub trait WorkflowCommandAdapterTransport: Send + Sync {
    async fn post_json(
        &self,
        url: String,
        bearer_token: String,
        timeout: Duration,
        body: String,
    ) -> Result<(u16, String), WorkflowCommandAdapterError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowCommandAdapterError {
    Unreachable(String),
    Malformed(String),
}

impl std::fmt::Display for WorkflowCommandAdapterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkflowCommandAdapterError::Unreachable(msg) => {
                write!(f, "workflow_command_adapter unreachable: {msg}")
            }
            WorkflowCommandAdapterError::Malformed(msg) => {
                write!(f, "workflow_command_adapter malformed: {msg}")
            }
        }
    }
}

impl std::error::Error for WorkflowCommandAdapterError {}

/// ``reqwest``-backed transport. Constructed once per adapter
/// instance; the underlying connection pool is reused across
/// dispatches.
pub struct ReqwestWorkflowCommandAdapterTransport {
    client: reqwest::Client,
}

impl ReqwestWorkflowCommandAdapterTransport {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl Default for ReqwestWorkflowCommandAdapterTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl WorkflowCommandAdapterTransport for ReqwestWorkflowCommandAdapterTransport {
    async fn post_json(
        &self,
        url: String,
        bearer_token: String,
        timeout: Duration,
        body: String,
    ) -> Result<(u16, String), WorkflowCommandAdapterError> {
        let response = self
            .client
            .post(&url)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {bearer_token}"),
            )
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .timeout(timeout)
            .body(body)
            .send()
            .await
            .map_err(|e| WorkflowCommandAdapterError::Unreachable(e.to_string()))?;
        let status = response.status().as_u16();
        let text = response
            .text()
            .await
            .map_err(|e| WorkflowCommandAdapterError::Malformed(e.to_string()))?;
        Ok((status, text))
    }
}

// ---------------------------------------------------------------------------
// Adapter
// ---------------------------------------------------------------------------

/// Thin Discord ``/workflow`` → AAP Runtime adapter.
///
/// The adapter is constructed at composition time and reused for
/// every Discord ``Interaction::Command`` for the ``workflow`` top-
/// level Application Command. It is fully driven by the Discord
/// subcommand + options the user supplied; it never inspects
/// arbitrary message text.
pub struct WorkflowCommandAdapter {
    base_url: String,
    credential: String,
    timeout: Duration,
    http: Arc<dyn WorkflowCommandAdapterTransport>,
}

impl WorkflowCommandAdapter {
    /// Build an adapter from the shared AAP control-plane config.
    /// Returns ``AuthMissing`` when the credential env var is unset
    /// or empty so callers can fail closed at composition time.
    pub fn from_aap_control_plane(
        config: &AapControlPlaneConfig,
        http: Arc<dyn WorkflowCommandAdapterTransport>,
    ) -> Result<Self, AdapterRejectionReason> {
        let credential = config
            .resolve_credential()
            .ok_or(AdapterRejectionReason::AuthMissing)?;
        Ok(Self {
            base_url: config.aap_runtime_url.trim_end_matches('/').to_string(),
            credential,
            timeout: Duration::from_secs(30),
            http,
        })
    }

    /// Build the trusted native Discord adapter.
    ///
    /// The Runtime URL may still come from the shared AAP control-plane
    /// configuration, but authority comes ONLY from
    /// ARTHUR_OPENAB_DISCORD_NATIVE_INTERACTION_KEY. The generic AAP
    /// control-plane credential is deliberately not consulted here.
    pub fn from_native_discord_env(
        aap_runtime_url: &str,
        http: Arc<dyn WorkflowCommandAdapterTransport>,
    ) -> Result<Self, AdapterRejectionReason> {
        let credential = std::env::var(DISCORD_NATIVE_INTERACTION_KEY_ENV)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .ok_or(AdapterRejectionReason::AuthMissing)?;

        Ok(Self {
            base_url: aap_runtime_url.trim_end_matches('/').to_string(),
            credential,
            timeout: Duration::from_secs(30),
            http,
        })
    }

    /// Build an adapter with an explicit credential (test-only escape
    /// hatch — production wiring goes through ``from_aap_control_plane``).
    #[cfg(test)]
    pub fn for_tests(
        aap_runtime_url: &str,
        credential: String,
        http: Arc<dyn WorkflowCommandAdapterTransport>,
    ) -> Self {
        Self {
            base_url: aap_runtime_url.trim_end_matches('/').to_string(),
            credential,
            timeout: Duration::from_secs(5),
            http,
        }
    }

    /// Dispatch a native Discord ``/workflow`` command to AAP Runtime.
    ///
    /// ``subcommand`` and ``options`` come straight from the Discord
    /// ``CommandInteraction`` payload. The adapter MUST be
    /// non-fallthrough: any unrecognized subcommand, missing
    /// required option, transport failure, or 4xx/5xx response
    /// returns a typed ``WorkflowCommandAdapterResult`` so the
    /// Discord handler can render an ephemeral reply without
    /// re-routing the inbound to ACP / chat.
    pub async fn dispatch(
        &self,
        subcommand: WorkflowSubcommand,
        options: &WorkflowCommandOptions,
        discord_user_id: &str,
        request_id: Option<&str>,
        channel_session_id: &str,
    ) -> WorkflowCommandAdapterResult {
        // Build the textual /workflow <sub> <id> [flags] form that
        // the canonical Runtime OpenClaw bridge already parses. This
        // is the ONLY place where the bounded command surface is
        // assembled — Runtime stays the single mutation authority.
        let message = match build_slash_command_message(subcommand, options) {
            Ok(message) => message,
            Err(reason) => {
                return WorkflowCommandAdapterResult::AdapterRejection {
                    reason,
                    detail: None,
                };
            }
        };

        let body = OpenClawTurnRequestBody {
            message: message.clone(),
            channel: "discord_native",
            channel_session_id: channel_session_id.to_string(),
            user_id: Some(discord_user_id.to_string()),
            request_id: request_id.map(|s| s.to_string()),
            metadata: OpenClawTurnMetadata {
                workflow_command_dispatch_source: "discord_native",
                workflow_command_dispatch_subcommand: subcommand.as_str(),
                workflow_command_dispatch_is_mutation: subcommand.is_mutation(),
            },
        };

        let serialized = match serde_json::to_string(&body) {
            Ok(s) => s,
            Err(e) => {
                return WorkflowCommandAdapterResult::AdapterRejection {
                    reason: AdapterRejectionReason::RuntimeMalformed(e.to_string()),
                    detail: None,
                };
            }
        };

        let url = format!("{}/v1/integrations/openclaw/turn", self.base_url);

        let (status, body_text) = match self
            .http
            .post_json(
                url.clone(),
                self.credential.clone(),
                self.timeout,
                serialized,
            )
            .await
        {
            Ok(t) => t,
            Err(WorkflowCommandAdapterError::Unreachable(msg)) => {
                return WorkflowCommandAdapterResult::AdapterRejection {
                    reason: AdapterRejectionReason::RuntimeUnreachable(msg),
                    detail: Some(url),
                };
            }
            Err(WorkflowCommandAdapterError::Malformed(msg)) => {
                return WorkflowCommandAdapterResult::AdapterRejection {
                    reason: AdapterRejectionReason::RuntimeMalformed(msg),
                    detail: Some(url),
                };
            }
        };

        if !(200..300).contains(&status) {
            return WorkflowCommandAdapterResult::AdapterRejection {
                reason: AdapterRejectionReason::RuntimeHttp {
                    status,
                    body_snippet: body_text.chars().take(200).collect(),
                },
                detail: Some(url),
            };
        }

        let parsed: OpenClawTurnResponseBody = match serde_json::from_str(&body_text) {
            Ok(p) => p,
            Err(e) => {
                return WorkflowCommandAdapterResult::AdapterRejection {
                    reason: AdapterRejectionReason::RuntimeMalformed(e.to_string()),
                    detail: Some(body_text.chars().take(200).collect()),
                };
            }
        };

        // Runtime surfaces a successful dispatch as ``status == "completed"``
        // and a typed rejection as ``status == "failed"``. The canonical
        // rejection token lives on ``metadata.workflow_command_error``
        // (mirrors ``runtime.integrations.openclaw._project_workflow_*``).
        if parsed.status == "completed" {
            let runtime_response = parsed
                .metadata
                .get("workflow_command_runtime_response")
                .cloned();
            return WorkflowCommandAdapterResult::Delivered {
                message: parsed.message,
                runtime_response,
            };
        }

        let reason = parsed
            .metadata
            .get("workflow_command_error")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "unknown_runtime_rejection".to_string());
        let detail = parsed
            .metadata
            .get("workflow_command_detail")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        WorkflowCommandAdapterResult::RuntimeRejection {
            reason,
            message: Some(parsed.message),
            detail,
        }
    }

    /// Forward an already-serialized Discord CommandInteraction payload
    /// to AAP Runtime's dedicated native interaction ingress.
    ///
    /// The caller owns Serenity serialization. This method does not
    /// synthesize slash-command text and never falls through to the
    /// ordinary /openclaw/turn path.
    pub async fn dispatch_native_interaction(
        &self,
        serialized_interaction: String,
    ) -> WorkflowCommandAdapterResult {
        let url = format!(
            "{}/v1/integrations/openclaw/discord/workflow-interaction",
            self.base_url
        );

        let (status, body_text) = match self
            .http
            .post_json(
                url.clone(),
                self.credential.clone(),
                self.timeout,
                serialized_interaction,
            )
            .await
        {
            Ok(result) => result,
            Err(WorkflowCommandAdapterError::Unreachable(message)) => {
                return WorkflowCommandAdapterResult::AdapterRejection {
                    reason: AdapterRejectionReason::RuntimeUnreachable(message),
                    detail: Some(url),
                };
            }
            Err(WorkflowCommandAdapterError::Malformed(message)) => {
                return WorkflowCommandAdapterResult::AdapterRejection {
                    reason: AdapterRejectionReason::RuntimeMalformed(message),
                    detail: Some(url),
                };
            }
        };

        if !(200..300).contains(&status) {
            return WorkflowCommandAdapterResult::AdapterRejection {
                reason: AdapterRejectionReason::RuntimeHttp {
                    status,
                    body_snippet: body_text.chars().take(200).collect(),
                },
                detail: Some(url),
            };
        }

        let parsed: DiscordWorkflowInteractionResponseBody = match serde_json::from_str(&body_text)
        {
            Ok(parsed) => parsed,
            Err(error) => {
                return WorkflowCommandAdapterResult::AdapterRejection {
                    reason: AdapterRejectionReason::RuntimeMalformed(error.to_string()),
                    detail: Some(body_text.chars().take(200).collect()),
                };
            }
        };

        if parsed.delivered {
            let mut runtime_response = match parsed.metadata {
                serde_json::Value::Object(map) => map,
                _ => serde_json::Map::new(),
            };

            if let Some(workflow_run_id) = parsed.workflow_run_id {
                runtime_response
                    .entry("workflow_run_id".to_string())
                    .or_insert(serde_json::Value::String(workflow_run_id));
            }

            if let Some(command) = parsed.command {
                runtime_response
                    .entry("command".to_string())
                    .or_insert(serde_json::Value::String(command));
            }

            let message = parsed
                .message
                .unwrap_or_else(|| "Workflow command delivered.".to_string());

            return WorkflowCommandAdapterResult::Delivered {
                message,
                runtime_response: Some(serde_json::Value::Object(runtime_response)),
            };
        }

        let _ = parsed.ephemeral;

        WorkflowCommandAdapterResult::RuntimeRejection {
            reason: parsed.reason,
            message: parsed.message,
            detail: parsed.detail,
        }
    }
}

// ---------------------------------------------------------------------------
// Options shape — the bounded set of Discord ``/workflow`` options.
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
pub struct WorkflowCommandOptions {
    pub workflow_run_id: Option<String>,
    pub expected_revision: Option<i64>,
    pub reason: Option<String>,
    pub correction_spec: Option<String>,
    pub primary: Option<String>,
    pub verifier: Option<String>,
    pub final_reviewer: Option<String>,
    pub binding: Option<String>,
}

impl WorkflowCommandOptions {
    /// Construct an empty options struct (used by tests).
    pub fn new() -> Self {
        Self::default()
    }

    /// Pull a named option value from a Discord subcommand option
    /// slice. The serializer argument receives the typed accessor
    /// (``as_i64`` / ``as_str``) so callers do not have to repeat
    /// the lookup pattern.
    pub fn from_discord_options<F>(
        subcommand: WorkflowSubcommand,
        discord_options: &[(String, DiscordOptionValue)],
        mut read: F,
    ) -> Self
    where
        F: FnMut(&str, &[(String, DiscordOptionValue)]) -> Option<DiscordOptionValue>,
    {
        let mut opts = WorkflowCommandOptions::new();
        if let Some(value) = read(OPTION_WORKFLOW_RUN_ID, discord_options) {
            if let Some(s) = value.as_str() {
                opts.workflow_run_id = Some(s.to_string());
            }
        }
        if let Some(value) = read(OPTION_EXPECTED_REVISION, discord_options) {
            opts.expected_revision = value.as_i64();
        }
        if let Some(value) = read(OPTION_REASON, discord_options) {
            if let Some(s) = value.as_str() {
                opts.reason = Some(s.to_string());
            }
        }
        if let Some(value) = read(OPTION_CORRECTION_SPEC, discord_options) {
            if let Some(s) = value.as_str() {
                opts.correction_spec = Some(s.to_string());
            }
        }
        if let Some(value) = read(OPTION_PRIMARY, discord_options) {
            if let Some(s) = value.as_str() {
                opts.primary = Some(s.to_string());
            }
        }
        if let Some(value) = read(OPTION_VERIFIER, discord_options) {
            if let Some(s) = value.as_str() {
                opts.verifier = Some(s.to_string());
            }
        }
        if let Some(value) = read(OPTION_FINAL_REVIEWER, discord_options) {
            if let Some(s) = value.as_str() {
                opts.final_reviewer = Some(s.to_string());
            }
        }
        if let Some(value) = read(OPTION_BINDING, discord_options) {
            if let Some(s) = value.as_str() {
                opts.binding = Some(s.to_string());
            }
        }
        let _ = subcommand; // reserved for subcommand-specific option gating
        opts
    }
}

/// Snapshot of a Discord command option value. The ``Discord``
/// crate's ``CommandDataOption.value`` enum is private to the
/// crate's API surface; the adapter takes a flattened view so it
/// can be unit-tested without a live ``CommandInteraction``.
#[derive(Debug, Clone)]
pub enum DiscordOptionValue {
    Str(String),
    Int(i64),
    Other,
}

impl DiscordOptionValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            DiscordOptionValue::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            DiscordOptionValue::Int(v) => Some(*v),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Slash command text builder — the bounded surface parser.
// ---------------------------------------------------------------------------

/// Build the canonical ``/workflow <sub> <workflow_run_id> [--flag value]``
/// text the Runtime OpenClaw bridge parses.
///
/// This builder is intentionally narrow — it never re-orders options,
/// never injects defaults, never coalesces flags. Every field the
/// Discord user supplied is forwarded verbatim so the canonical
/// Runtime parser (``parse_workflow_slash_command``) is the single
/// source of truth for validation.
pub fn build_slash_command_message(
    subcommand: WorkflowSubcommand,
    options: &WorkflowCommandOptions,
) -> Result<String, AdapterRejectionReason> {
    let workflow_run_id = options
        .workflow_run_id
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or(AdapterRejectionReason::MissingWorkflowRunId)?;

    let mut parts: Vec<String> = Vec::new();
    parts.push("/workflow".to_string());
    parts.push(subcommand.as_str().to_string());
    parts.push(workflow_run_id);

    // The Runtime parser treats ``--flag value`` as a key/value pair
    // where ``value`` is "every remaining token until the next
    // ``--flag``". We forward exactly the canonical Runtime flag
    // names (which use hyphenated form for ``expected-revision`` /
    // ``correction-spec`` / ``final-reviewer``); unknown options are
    // rejected at the Runtime seam.
    if let Some(rev) = options.expected_revision {
        if rev < 0 {
            return Err(AdapterRejectionReason::InvalidExpectedRevision);
        }
        parts.push(format!("--{}", RUNTIME_FLAG_EXPECTED_REVISION));
        parts.push(rev.to_string());
    }
    push_string_flag(
        &mut parts,
        "--",
        RUNTIME_FLAG_REASON,
        options.reason.as_deref(),
    )?;
    push_string_flag(
        &mut parts,
        "--",
        RUNTIME_FLAG_CORRECTION_SPEC,
        options.correction_spec.as_deref(),
    )?;
    push_string_flag(
        &mut parts,
        "--",
        RUNTIME_FLAG_PRIMARY,
        options.primary.as_deref(),
    )?;
    push_string_flag(
        &mut parts,
        "--",
        RUNTIME_FLAG_VERIFIER,
        options.verifier.as_deref(),
    )?;
    push_string_flag(
        &mut parts,
        "--",
        RUNTIME_FLAG_FINAL_REVIEWER,
        options.final_reviewer.as_deref(),
    )?;
    push_string_flag(
        &mut parts,
        "--",
        RUNTIME_FLAG_BINDING,
        options.binding.as_deref(),
    )?;

    Ok(parts.join(" "))
}

fn push_string_flag(
    parts: &mut Vec<String>,
    prefix: &str,
    name: &str,
    value: Option<&str>,
) -> Result<(), AdapterRejectionReason> {
    if let Some(raw) = value {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            // Empty string on a non-required option is silently dropped
            // (Runtime parser strips whitespace before validation); a
            // missing-required-key case is the caller's responsibility
            // (we surface it as ``MissingRequiredOption`` before
            // building the message).
            return Ok(());
        }
        parts.push(format!("{prefix}{name}"));
        parts.push(trimmed.to_string());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests — focused unit coverage for registration shape, option parsing,
// routing boundary, authorization boundary, typed rejection, and
// no-fallthrough behavior.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // ---- Subcommand vocabulary ----

    #[test]
    fn subcommand_round_trip() {
        for sc in [
            WorkflowSubcommand::Status,
            WorkflowSubcommand::Agents,
            WorkflowSubcommand::ReopenPrimary,
            WorkflowSubcommand::ReopenWork,
            WorkflowSubcommand::Reconfigure,
        ] {
            assert_eq!(WorkflowSubcommand::parse(sc.as_str()), Some(sc));
        }
    }

    #[test]
    fn unknown_subcommand_returns_none() {
        assert_eq!(WorkflowSubcommand::parse(""), None);
        assert_eq!(WorkflowSubcommand::parse("approve"), None);
        assert_eq!(WorkflowSubcommand::parse("Reopen-Primary"), None); // case-sensitive
        assert_eq!(WorkflowSubcommand::parse("/workflow"), None);
    }

    #[test]
    fn mutation_classification_matches_runtime() {
        // Runtime gates only ``reopen-*`` and ``reconfigure`` as
        // mutation commands; ``status`` and ``agents`` are read-only.
        assert!(!WorkflowSubcommand::Status.is_mutation());
        assert!(!WorkflowSubcommand::Agents.is_mutation());
        assert!(WorkflowSubcommand::ReopenPrimary.is_mutation());
        assert!(WorkflowSubcommand::ReopenWork.is_mutation());
        assert!(WorkflowSubcommand::Reconfigure.is_mutation());
    }

    // ---- Slash command text builder ----

    #[test]
    fn build_status_without_expected_revision() {
        let opts = WorkflowCommandOptions {
            workflow_run_id: Some("wfr-123".to_string()),
            binding: Some("binding-abc".to_string()),
            ..WorkflowCommandOptions::default()
        };
        let text = build_slash_command_message(WorkflowSubcommand::Status, &opts).unwrap();
        assert_eq!(text, "/workflow status wfr-123 --binding binding-abc");
    }

    #[test]
    fn build_agents_without_expected_revision() {
        let opts = WorkflowCommandOptions {
            workflow_run_id: Some("wfr-456".to_string()),
            ..WorkflowCommandOptions::default()
        };
        let text = build_slash_command_message(WorkflowSubcommand::Agents, &opts).unwrap();
        assert_eq!(text, "/workflow agents wfr-456");
    }

    #[test]
    fn build_reopen_primary_full_options() {
        let opts = WorkflowCommandOptions {
            workflow_run_id: Some("wfr-789".to_string()),
            expected_revision: Some(9),
            reason: Some("TECH_LEAD_POST_REVIEW_REOPEN".to_string()),
            correction_spec: Some("revert auth fix and re-derive".to_string()),
            ..WorkflowCommandOptions::default()
        };
        let text = build_slash_command_message(WorkflowSubcommand::ReopenPrimary, &opts).unwrap();
        assert_eq!(
            text,
            "/workflow reopen-primary wfr-789 --expected-revision 9 \
             --reason TECH_LEAD_POST_REVIEW_REOPEN \
             --correction-spec revert auth fix and re-derive"
        );
    }

    #[test]
    fn build_reopen_work_minimal_options() {
        let opts = WorkflowCommandOptions {
            workflow_run_id: Some("wfr-321".to_string()),
            expected_revision: Some(11),
            reason: Some("TECH_LEAD_POST_REVIEW_REOPEN".to_string()),
            ..WorkflowCommandOptions::default()
        };
        let text = build_slash_command_message(WorkflowSubcommand::ReopenWork, &opts).unwrap();
        assert_eq!(
            text,
            "/workflow reopen-work wfr-321 --expected-revision 11 --reason TECH_LEAD_POST_REVIEW_REOPEN"
        );
    }

    #[test]
    fn build_reconfigure_three_agent_options() {
        let opts = WorkflowCommandOptions {
            workflow_run_id: Some("wfr-555".to_string()),
            expected_revision: Some(2),
            reason: Some("TECH_LEAD_TOPOLOGY_RECONFIGURE".to_string()),
            primary: Some("ArthurCodex".to_string()),
            verifier: Some("ArthurGemini".to_string()),
            final_reviewer: Some("ArthurGemini".to_string()),
            ..WorkflowCommandOptions::default()
        };
        let text = build_slash_command_message(WorkflowSubcommand::Reconfigure, &opts).unwrap();
        assert_eq!(
            text,
            "/workflow reconfigure wfr-555 --expected-revision 2 \
             --reason TECH_LEAD_TOPOLOGY_RECONFIGURE \
             --primary ArthurCodex --verifier ArthurGemini --final-reviewer ArthurGemini"
        );
    }

    #[test]
    fn build_missing_workflow_run_id_returns_typed_rejection() {
        let opts = WorkflowCommandOptions {
            expected_revision: Some(1),
            ..WorkflowCommandOptions::default()
        };
        let err = build_slash_command_message(WorkflowSubcommand::Status, &opts).unwrap_err();
        assert_eq!(err, AdapterRejectionReason::MissingWorkflowRunId);
        assert_eq!(err.token(), "missing_workflow_run_id");
    }

    #[test]
    fn build_blank_workflow_run_id_rejected() {
        let opts = WorkflowCommandOptions {
            workflow_run_id: Some("   ".to_string()),
            ..WorkflowCommandOptions::default()
        };
        let err = build_slash_command_message(WorkflowSubcommand::Reconfigure, &opts).unwrap_err();
        assert_eq!(err, AdapterRejectionReason::MissingWorkflowRunId);
    }

    #[test]
    fn build_negative_expected_revision_rejected() {
        let opts = WorkflowCommandOptions {
            workflow_run_id: Some("wfr-x".to_string()),
            expected_revision: Some(-1),
            ..WorkflowCommandOptions::default()
        };
        let err = build_slash_command_message(WorkflowSubcommand::Status, &opts).unwrap_err();
        assert_eq!(err, AdapterRejectionReason::InvalidExpectedRevision);
    }

    #[test]
    fn build_drops_empty_string_optional_flags() {
        let opts = WorkflowCommandOptions {
            workflow_run_id: Some("wfr-y".to_string()),
            expected_revision: Some(0),
            reason: Some("   ".to_string()), // whitespace-only → dropped
            binding: Some(String::new()),    // empty → dropped
            ..WorkflowCommandOptions::default()
        };
        let text = build_slash_command_message(WorkflowSubcommand::Status, &opts).unwrap();
        assert_eq!(text, "/workflow status wfr-y --expected-revision 0");
    }

    #[test]
    fn build_preserves_multi_word_values_verbatim() {
        // Runtime's parser joins remaining tokens with single spaces
        // when the value is the last flag, so multi-word values are
        // accepted. The adapter must NOT quote them.
        let opts = WorkflowCommandOptions {
            workflow_run_id: Some("wfr-mw".to_string()),
            expected_revision: Some(1),
            reason: Some("Tech Lead token exhausted mid-run".to_string()),
            ..WorkflowCommandOptions::default()
        };
        let text = build_slash_command_message(WorkflowSubcommand::ReopenWork, &opts).unwrap();
        assert!(text.ends_with("--reason Tech Lead token exhausted mid-run"));
    }

    // ---- Adapter dispatch (transport stub) ----

    /// Stub transport that captures the last request body and returns
    /// a canned response. Used to exercise the dispatch path without
    /// a live Runtime.
    #[derive(Default)]
    struct StubTransport {
        last: Mutex<Option<StubCaptured>>,
        response_status: u16,
        response_body: String,
        response_override: Option<(u16, String)>,
    }

    #[derive(Debug, Clone)]
    struct StubCaptured {
        url: String,
        bearer_token: String,
        body: String,
    }

    impl StubTransport {
        fn new(status: u16, body: &str) -> Self {
            Self {
                last: Mutex::new(None),
                response_status: status,
                response_body: body.to_string(),
                response_override: None,
            }
        }
    }

    #[async_trait::async_trait]
    impl WorkflowCommandAdapterTransport for StubTransport {
        async fn post_json(
            &self,
            url: String,
            bearer_token: String,
            _timeout: Duration,
            body: String,
        ) -> Result<(u16, String), WorkflowCommandAdapterError> {
            *self.last.lock().unwrap() = Some(StubCaptured {
                url,
                bearer_token,
                body,
            });
            let (status, body) = self
                .response_override
                .clone()
                .unwrap_or_else(|| (self.response_status, self.response_body.clone()));
            Ok((status, body))
        }
    }

    fn tokio_block<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(future)
    }

    #[test]
    fn dispatch_status_delivered_forwards_user_id_and_message() {
        // Build the canonical Runtime success envelope.
        let body = serde_json::json!({
            "status": "completed",
            "message": "/workflow status delivered: workflow_run_id='wfr-1' revision=5 state='PRIMARY_ACTIVE'",
            "metadata": {
                "workflow_command": true,
                "workflow_command_name": "status",
                "workflow_command_workflow_run_id": "wfr-1",
                "workflow_command_runtime_response": {
                    "workflow_run_id": "wfr-1",
                    "revision": 5,
                    "state": "PRIMARY_ACTIVE",
                },
            },
        })
        .to_string();
        let transport = Arc::new(StubTransport::new(200, &body));
        let adapter = WorkflowCommandAdapter::for_tests(
            "http://runtime.test",
            "test-credential".to_string(),
            transport.clone(),
        );

        let opts = WorkflowCommandOptions {
            workflow_run_id: Some("wfr-1".to_string()),
            expected_revision: Some(5),
            ..WorkflowCommandOptions::default()
        };
        let result = tokio_block(adapter.dispatch(
            WorkflowSubcommand::Status,
            &opts,
            "discord-user-42",
            Some("req-7"),
            "discord:12345",
        ));

        assert!(result.is_delivered());
        match result {
            WorkflowCommandAdapterResult::Delivered {
                message,
                runtime_response,
            } => {
                assert!(message.contains("wfr-1"));
                let payload = runtime_response.expect("runtime_response must be Some");
                assert_eq!(payload["workflow_run_id"], "wfr-1");
                assert_eq!(payload["revision"], 5);
            }
            other => panic!("expected Delivered, got {other:?}"),
        }

        // Verify the transport saw the canonical textual slash command.
        let captured = transport.last.lock().unwrap().clone().expect("captured");
        assert_eq!(
            captured.url,
            "http://runtime.test/v1/integrations/openclaw/turn"
        );
        assert_eq!(captured.bearer_token, "test-credential");
        let req: serde_json::Value = serde_json::from_str(&captured.body).unwrap();
        assert_eq!(
            req["message"],
            "/workflow status wfr-1 --expected-revision 5"
        );
        assert_eq!(req["user_id"], "discord-user-42");
        assert_eq!(req["request_id"], "req-7");
        assert_eq!(req["channel"], "discord_native");
        assert_eq!(
            req["metadata"]["workflow_command_dispatch_subcommand"],
            "status"
        );
        assert_eq!(
            req["metadata"]["workflow_command_dispatch_is_mutation"],
            false
        );
    }

    #[test]
    fn dispatch_reopen_primary_mutation_metadata() {
        let body = serde_json::json!({
            "status": "completed",
            "message": "/workflow reopen-primary delivered",
            "metadata": {
                "workflow_command": true,
                "workflow_command_name": "reopen-primary",
                "workflow_command_workflow_run_id": "wfr-9",
                "workflow_command_runtime_response": {
                    "workflow_run_id": "wfr-9",
                    "new_revision": 10,
                },
            },
        })
        .to_string();
        let transport = Arc::new(StubTransport::new(200, &body));
        let adapter = WorkflowCommandAdapter::for_tests(
            "http://runtime.test",
            "test-credential".to_string(),
            transport.clone(),
        );
        let opts = WorkflowCommandOptions {
            workflow_run_id: Some("wfr-9".to_string()),
            expected_revision: Some(9),
            reason: Some("TECH_LEAD_POST_REVIEW_REOPEN".to_string()),
            correction_spec: Some("fix the audit sink regression".to_string()),
            ..WorkflowCommandOptions::default()
        };
        let result = tokio_block(adapter.dispatch(
            WorkflowSubcommand::ReopenPrimary,
            &opts,
            "discord-user-99",
            None,
            "discord:67890",
        ));
        assert!(result.is_delivered());

        let captured = transport.last.lock().unwrap().clone().unwrap();
        let req: serde_json::Value = serde_json::from_str(&captured.body).unwrap();
        assert_eq!(
            req["metadata"]["workflow_command_dispatch_is_mutation"],
            true
        );
        assert_eq!(
            req["metadata"]["workflow_command_dispatch_subcommand"],
            "reopen-primary"
        );
        assert!(req["message"].as_str().unwrap().contains("reopen-primary"));
    }

    #[test]
    fn dispatch_runtime_unauthorized_surfaces_typed_rejection_without_fallthrough() {
        // Runtime returns status=failed with the canonical rejection
        // token. The adapter MUST surface it verbatim — no
        // reconstruction, no fallback to chat.
        let body = serde_json::json!({
            "status": "failed",
            "message": "Workflow command rejected: 'unauthorized_sender'",
            "metadata": {
                "workflow_command": true,
                "workflow_command_name": "reopen-primary",
                "workflow_command_error": "unauthorized_sender",
                "workflow_command_detail": "sender_id='discord-user-99' is not authorised for mutation command 'reopen-primary'",
            },
        })
        .to_string();
        let transport = Arc::new(StubTransport::new(200, &body));
        let adapter = WorkflowCommandAdapter::for_tests(
            "http://runtime.test",
            "test-credential".to_string(),
            transport,
        );
        let opts = WorkflowCommandOptions {
            workflow_run_id: Some("wfr-9".to_string()),
            expected_revision: Some(9),
            reason: Some("TECH_LEAD_POST_REVIEW_REOPEN".to_string()),
            correction_spec: Some("x".to_string()),
            ..WorkflowCommandOptions::default()
        };
        let result = tokio_block(adapter.dispatch(
            WorkflowSubcommand::ReopenPrimary,
            &opts,
            "discord-user-99",
            None,
            "discord:1",
        ));
        match result {
            WorkflowCommandAdapterResult::RuntimeRejection {
                reason,
                message,
                detail,
            } => {
                assert_eq!(reason, "unauthorized_sender");
                assert_eq!(
                    message.as_deref(),
                    Some("Workflow command rejected: 'unauthorized_sender'")
                );
                assert!(detail.unwrap().contains("discord-user-99"));
            }
            other => panic!("expected RuntimeRejection, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_transport_unreachable_returns_adapter_rejection() {
        struct AlwaysUnreachable;
        #[async_trait::async_trait]
        impl WorkflowCommandAdapterTransport for AlwaysUnreachable {
            async fn post_json(
                &self,
                _url: String,
                _bearer_token: String,
                _timeout: Duration,
                _body: String,
            ) -> Result<(u16, String), WorkflowCommandAdapterError> {
                Err(WorkflowCommandAdapterError::Unreachable(
                    "connection refused".to_string(),
                ))
            }
        }
        let transport: Arc<dyn WorkflowCommandAdapterTransport> = Arc::new(AlwaysUnreachable);
        let adapter = WorkflowCommandAdapter::for_tests(
            "http://runtime.test",
            "test-credential".to_string(),
            transport,
        );
        let opts = WorkflowCommandOptions {
            workflow_run_id: Some("wfr-9".to_string()),
            expected_revision: Some(9),
            ..WorkflowCommandOptions::default()
        };
        let result = tokio_block(adapter.dispatch(
            WorkflowSubcommand::Status,
            &opts,
            "discord-user-1",
            None,
            "discord:1",
        ));
        match result {
            WorkflowCommandAdapterResult::AdapterRejection { reason, .. } => {
                assert_eq!(reason.token(), "runtime_unreachable");
            }
            other => panic!("expected AdapterRejection, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_runtime_5xx_returns_typed_http_rejection() {
        let transport = Arc::new(StubTransport::new(500, r#"{"detail":"upstream error"}"#));
        let adapter = WorkflowCommandAdapter::for_tests(
            "http://runtime.test",
            "test-credential".to_string(),
            transport,
        );
        let opts = WorkflowCommandOptions {
            workflow_run_id: Some("wfr-9".to_string()),
            expected_revision: Some(9),
            ..WorkflowCommandOptions::default()
        };
        let result = tokio_block(adapter.dispatch(
            WorkflowSubcommand::Status,
            &opts,
            "discord-user-1",
            None,
            "discord:1",
        ));
        match result {
            WorkflowCommandAdapterResult::AdapterRejection { reason, .. } => match reason {
                AdapterRejectionReason::RuntimeHttp { status, .. } => {
                    assert_eq!(status, 500);
                }
                other => panic!("expected RuntimeHttp, got {other:?}"),
            },
            other => panic!("expected AdapterRejection, got {other:?}"),
        }
    }

    #[test]
    fn dispatch_malformed_response_returns_malformed_rejection() {
        let transport = Arc::new(StubTransport::new(200, "not-json-at-all"));
        let adapter = WorkflowCommandAdapter::for_tests(
            "http://runtime.test",
            "test-credential".to_string(),
            transport,
        );
        let opts = WorkflowCommandOptions {
            workflow_run_id: Some("wfr-9".to_string()),
            expected_revision: Some(9),
            ..WorkflowCommandOptions::default()
        };
        let result = tokio_block(adapter.dispatch(
            WorkflowSubcommand::Status,
            &opts,
            "discord-user-1",
            None,
            "discord:1",
        ));
        match result {
            WorkflowCommandAdapterResult::AdapterRejection { reason, .. } => {
                assert!(matches!(
                    reason,
                    AdapterRejectionReason::RuntimeMalformed(_)
                ));
            }
            other => panic!("expected AdapterRejection, got {other:?}"),
        }
    }

    #[test]
    fn from_aap_control_plane_fails_closed_when_credential_missing() {
        // Empty credential env name → resolve_credential returns None.
        let cfg = AapControlPlaneConfig {
            aap_runtime_url: "http://runtime.test".to_string(),
            aap_credential_env: "OPENAB_TEST_NONEXISTENT_CREDENTIAL_xyz".to_string(),
            enabled: true,
        };
        let transport: Arc<dyn WorkflowCommandAdapterTransport> =
            Arc::new(StubTransport::new(200, "{}"));
        let result = WorkflowCommandAdapter::from_aap_control_plane(&cfg, transport);
        assert!(matches!(
            result.err(),
            Some(AdapterRejectionReason::AuthMissing)
        ));
    }

    #[test]
    fn from_aap_control_plane_succeeds_when_credential_set() {
        let env_name = "OPENAB_TEST_CREDENTIAL_present";
        std::env::set_var(env_name, "secret-value");
        let cfg = AapControlPlaneConfig {
            aap_runtime_url: "http://runtime.test/".to_string(),
            aap_credential_env: env_name.to_string(),
            enabled: true,
        };
        let transport: Arc<dyn WorkflowCommandAdapterTransport> =
            Arc::new(StubTransport::new(200, "{}"));
        let result = WorkflowCommandAdapter::from_aap_control_plane(&cfg, transport);
        std::env::remove_var(env_name);
        let adapter = result.expect("adapter must be constructed");
        assert_eq!(adapter.base_url, "http://runtime.test");
    }
}

#[cfg(test)]
mod native_discord_transport_tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    // Environment variables are process-global and Rust tests run in
    // parallel by default. Serialize only the tests that mutate the
    // dedicated native Discord credential.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[derive(Default)]
    struct NativeStubTransport {
        captured: Mutex<Option<(String, String, String)>>,
        status: u16,
        response: String,
    }

    impl NativeStubTransport {
        fn new(status: u16, response: &str) -> Self {
            Self {
                captured: Mutex::new(None),
                status,
                response: response.to_string(),
            }
        }
    }

    #[async_trait::async_trait]
    impl WorkflowCommandAdapterTransport for NativeStubTransport {
        async fn post_json(
            &self,
            url: String,
            bearer_token: String,
            _timeout: Duration,
            body: String,
        ) -> Result<(u16, String), WorkflowCommandAdapterError> {
            *self.captured.lock().unwrap() = Some((url, bearer_token, body));

            Ok((self.status, self.response.clone()))
        }
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(future)
    }

    #[test]
    fn native_constructor_fails_closed_without_dedicated_key() {
        let _env_guard = ENV_LOCK.lock().unwrap();
        let previous = std::env::var(DISCORD_NATIVE_INTERACTION_KEY_ENV).ok();

        std::env::remove_var(DISCORD_NATIVE_INTERACTION_KEY_ENV);

        let transport: Arc<dyn WorkflowCommandAdapterTransport> =
            Arc::new(NativeStubTransport::new(200, "{}"));

        let result =
            WorkflowCommandAdapter::from_native_discord_env("http://runtime.test", transport);

        if let Some(value) = previous {
            std::env::set_var(DISCORD_NATIVE_INTERACTION_KEY_ENV, value);
        }

        assert!(matches!(
            result.err(),
            Some(AdapterRejectionReason::AuthMissing)
        ));
    }

    #[test]
    fn native_dispatch_uses_dedicated_endpoint_and_preserves_raw_payload() {
        let _env_guard = ENV_LOCK.lock().unwrap();
        let previous = std::env::var(DISCORD_NATIVE_INTERACTION_KEY_ENV).ok();

        std::env::set_var(DISCORD_NATIVE_INTERACTION_KEY_ENV, "native-test-key");

        let response = serde_json::json!({
            "delivered": true,
            "reason": "delivered",
            "ephemeral": true,
            "message": "Workflow status delivered.",
            "metadata": {
                "state": "TECH_LEAD_WAIT",
                "revision": 24
            },
            "detail": null,
            "command": "status",
            "workflow_run_id": "wfr-test"
        })
        .to_string();

        let transport = Arc::new(NativeStubTransport::new(200, &response));

        let adapter = WorkflowCommandAdapter::from_native_discord_env(
            "http://runtime.test/",
            transport.clone(),
        )
        .expect("native adapter must build");

        let raw_interaction = serde_json::json!({
            "id": "interaction-1",
            "application_id": "app-1",
            "type": 2,
            "data": {
                "id": "command-1",
                "name": "workflow",
                "type": 1,
                "options": [
                    {
                        "name": "status",
                        "type": 1,
                        "options": [
                            {
                                "name": "workflow_run_id",
                                "type": 3,
                                "value": "wfr-test"
                            }
                        ]
                    }
                ]
            },
            "channel_id": "channel-1",
            "user": {
                "id": "tech-lead-1"
            },
            "token": "interaction-token",
            "version": 1,
            "locale": "en-US"
        })
        .to_string();

        let result = block_on(adapter.dispatch_native_interaction(raw_interaction.clone()));

        if let Some(value) = previous {
            std::env::set_var(DISCORD_NATIVE_INTERACTION_KEY_ENV, value);
        } else {
            std::env::remove_var(DISCORD_NATIVE_INTERACTION_KEY_ENV);
        }

        assert!(result.is_delivered());

        let captured = transport
            .captured
            .lock()
            .unwrap()
            .clone()
            .expect("native request captured");

        assert_eq!(
            captured.0,
            "http://runtime.test/v1/integrations/openclaw/discord/workflow-interaction"
        );
        assert_eq!(captured.1, "native-test-key");
        assert_eq!(captured.2, raw_interaction);

        match result {
            WorkflowCommandAdapterResult::Delivered {
                message,
                runtime_response,
            } => {
                assert_eq!(message, "Workflow status delivered.");

                let payload = runtime_response.expect("runtime response");
                assert_eq!(payload["workflow_run_id"], "wfr-test");
                assert_eq!(payload["revision"], 24);
                assert_eq!(payload["state"], "TECH_LEAD_WAIT");
            }
            other => panic!("expected native Delivered, got {other:?}"),
        }
    }

    #[test]
    fn native_runtime_rejection_is_not_reported_as_delivered() {
        let transport = Arc::new(NativeStubTransport::new(
            200,
            &serde_json::json!({
                "delivered": false,
                "reason": "unauthorized_sender",
                "ephemeral": true,
                "message": "Workflow command rejected.",
                "metadata": {},
                "detail": "sender is not authorised",
                "command": "reopen-work",
                "workflow_run_id": "wfr-test"
            })
            .to_string(),
        ));

        let adapter = WorkflowCommandAdapter {
            base_url: "http://runtime.test".to_string(),
            credential: "native-test-key".to_string(),
            timeout: Duration::from_secs(5),
            http: transport,
        };

        let result = block_on(adapter.dispatch_native_interaction("{}".to_string()));

        match result {
            WorkflowCommandAdapterResult::RuntimeRejection {
                reason,
                message,
                detail,
            } => {
                assert_eq!(reason, "unauthorized_sender");
                assert_eq!(message.as_deref(), Some("Workflow command rejected."));
                assert_eq!(detail.as_deref(), Some("sender is not authorised"));
            }
            other => panic!("expected RuntimeRejection, got {other:?}"),
        }
    }
}
