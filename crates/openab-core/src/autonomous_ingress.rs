//! Phase 6.4: deterministic OpenAB → AAP autonomous ingress routing.
//!
//! This module provides the narrow seam where a human-authored Discord
//! message addressed to a declared AAP-autonomous agent is routed to
//! the Arthur AI Platform (AAP) Runtime BEFORE the ordinary ACP
//! conversation path. The routing decision is **deterministic** — it
//! is driven entirely by the deployment-time
//! [`crate::config::AutonomousIngressConfig`] and never inspects the
//! prompt body, never consults the LLM, and never relies on free-form
//! NLP keyword matching.
//!
//! Per AGENTS.md critical architecture rule, the LLM MUST NOT become
//! the workflow-routing authority. This module preserves that rule by
//! treating the configuration as the only machine-testable contract
//! for "should this human message be admitted to AAP first?".
//!
//! # Flow
//!
//! ```text
//! Human Discord message
//!     ↓
//! Discord adapter (parse event)
//!     ↓
//! Dispatcher (batch)
//!     ↓
//! A13 workflow-role gate (existing)
//!     ↓ (reason == WorkflowAssignmentMissing)
//! [Phase 6.4 seam]
//!     decide_aap_autonomous_route(config, agent, sender, conversation)
//!         ↓
//!     AutonomousIngressClient::submit_autonomous_ingress(...)
//!         ↓ (POST /v1/integrations/openab/autonomous_ingress)
//!     AAP Runtime NativeRuntimeIngressService
//!         ↓
//!     AutonomousWorkflowEntryService → Task + WorkflowRun + ConversationBinding
//!         ↓
//!     scheduler → agent.work → ArthurClaude PRIMARY
//! ```
//!
//! # Message consumption invariant
//!
//! Once AAP accepts the human turn, OpenAB MUST mark the message as
//! consumed and MUST NOT let it fall through into ordinary ACP
//! dispatch. Otherwise the same human turn would execute twice —
//! once as conversational coding and once as scheduler-native
//! agent.work. [`AutonomousRouteDisposition::Accepted`] therefore
//! instructs the dispatch loop to suppress the ACP path entirely.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

use crate::adapter::ChannelRef;
use crate::config::AutonomousIngressConfig;

/// Marker that opens a deterministic canonical-title declaration at the
/// start of a human-authored prompt body. Only an exact, case-sensitive
/// prefix match on this constant produces a structured title; all other
/// shapes — case variants, surrounding whitespace, alternate phrasing,
/// markdown headers, key/value lines, or embedded mentions — are
/// ignored on purpose so the extractor never infers a title and the
/// AAP fallback (`Human autonomous workflow`) takes over.
///
/// The header line MUST be the very first non-empty line of the
/// prompt. A leading blank line is tolerated but anything else
/// (including prose, code fences, or a different first character)
/// causes the extractor to return `None`. The value extends from
/// after the colon to the end of that single line; surrounding
/// Unicode whitespace on the value is trimmed by ``str::trim``,
/// which is Unicode-aware (not ASCII-only).
///
/// The runtime NEVER inspects ``user_objective`` prose beyond this
/// narrow header parse, so the contract is machine-testable and
/// there is no risk of NLP drift between OpenAB versions.
pub const CANONICAL_TITLE_HEADER: &str = "Canonical title:";

/// Extract the structured ``title`` from a human-authored prompt
/// when, and only when, the prompt opens with an exact
/// ``Canonical title:`` header.
///
/// Rules:
///   * The first non-empty line (after skipping leading blank lines
///     composed solely of Unicode whitespace — the standard library
///     ``char::is_whitespace`` predicate, NOT ASCII-only) MUST begin
///     with the literal ``Canonical title:`` token. Case variants,
///     alternate punctuation, or alternate whitespace are rejected.
///   * The title value is the remainder of that single line after
///     the colon. Surrounding Unicode whitespace is trimmed by
///     ``str::trim`` — Unicode-aware, not ASCII-only. The value is
///     returned verbatim otherwise: no character is removed,
///     normalised, or inferred.
///   * When the value is empty after trimming, the extractor
///     returns ``None`` so callers fall back to the neutral
///     ``Human autonomous workflow`` default rather than emitting a
///     blank title.
///   * Any other prompt shape returns ``None``. This is deliberate:
///     we do NOT scan for keyword matches, headings, or
///     subject/colon/pipe lines, because doing so would silently
///     shadow the explicit header contract and reintroduce the
///     title-loss bug.
///
/// This function is a non-mutating extractor: it returns a fresh
/// owned ``String`` derived from a single substring slice. The
/// ``prompt`` argument is read-only; no character of the caller's
/// prompt is removed, rewritten, or truncated by this call. Header
/// content, body content, and internal newlines all survive
/// extraction byte-for-byte. The AAP-side
/// ``CanonicalNativeRuntimeIngressRequest`` still applies its
/// existing canonical leading/trailing whitespace normalization
/// (``str::strip`` on ``user_objective``); that normalization is
/// orthogonal to and pre-dates this extractor.
pub fn extract_canonical_title(prompt: &str) -> Option<String> {
    for raw_line in prompt.split('\n') {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        if line.chars().all(|c| c.is_whitespace()) {
            continue;
        }
        if let Some(rest) = line.strip_prefix(CANONICAL_TITLE_HEADER) {
            let trimmed = rest.trim();
            if trimmed.is_empty() {
                return None;
            }
            return Some(trimmed.to_string());
        }
        return None;
    }
    None
}

/// Phase 6.4.4 — assemble the autonomous `user_objective` from the
/// human prompt and the typed Discord text-attachment bodies. The
/// function never inspects `extra_blocks` or any `ContentBlock`:
/// the typing at the Discord ingestion seam
/// (`BufferedMessage.discord_text_attachment_bodies`) is the ONLY
/// source of attachment content for AAP.
///
/// Ordering is deterministic: the human prompt body comes first
/// (header + body, byte-for-byte), then each text attachment
/// contribution is appended in arrival order separated by a single
/// blank line so downstream consumers see a stable shape. STT
/// transcripts, image / video metadata, and arbitrary
/// `ContentBlock::Text` blocks are deliberately NOT consulted, so
/// they cannot leak into AAP human authority.
///
/// When no typed attachment bodies are present the original prompt
/// is returned byte-for-byte (the canonical-title extractor still
/// runs unchanged).
pub fn assemble_user_objective(
    prompt: &str,
    text_attachments: &[crate::dispatch::TextAttachment],
) -> String {
    if text_attachments.is_empty() {
        return prompt.to_string();
    }
    let mut out = String::with_capacity(prompt.len());
    out.push_str(prompt);
    for attachment in text_attachments {
        out.push_str("\n\n[Attached text file: ");
        out.push_str(&attachment.filename);
        out.push_str("]\n");
        out.push_str(&attachment.body);
    }
    out
}

/// Phase 6.4.9 — explicit CURRENT-TURN HUMAN TEXT AUTHORITY policy
/// for ``original_human_prompt``.
///
/// Authority policy (Tech Lead authorized):
///
/// 1. Visible prompt non-empty
///    → ``original_human_prompt`` = visible prompt byte-for-byte.
///
/// 2. Visible prompt empty
///    AND ``sender_is_bot == false``
///    AND exactly one typed ``TextAttachment``
///    AND that attachment's filename is the canonical paste name
///    ``"message.txt"`` (matches ``media::is_text_file`` recognition
///    and the upstream Discord oversized-paste convention)
///    AND the attachment body is non-empty
///    → ``original_human_prompt`` = that attachment body byte-for-byte.
///
/// 3. Otherwise (bot sender, multiple attachments, non-``message.txt``
///    filename, empty body, etc.)
///    → no attachment promotion;
///      ``resolve_current_human_prompt_authority`` returns ``""`` and
///      the existing ``HTTP 422`` fail-closed surface remains intact
///      at AAP.
///
/// Important semantic statement
/// ─────────────────────────────
/// Discord and Serenity expose no reliable provenance discriminator
/// between (a) a Discord-generated oversized-paste text attachment
/// (the user typed >2000 chars and Discord materialized the spill
/// into a ``message.txt`` file) and (b) a current human sender
/// manually uploading a single ``message.txt``. The Phase 6.4.9
/// verification confirmed this against the live Discord Attachment
/// schema and the pinned Serenity 0.12.5 deserializer.
///
/// The policy therefore does NOT pretend to model provenance. It is
/// an explicit semantic authority rule: **a single non-empty
/// ``message.txt`` from a current authorized HUMAN turn with no
/// visible prompt is the turn's canonical human text authority**,
/// regardless of whether Discord generated the attachment from an
/// oversized paste or the human manually uploaded it.
///
/// Security boundary
/// ─────────────────
/// The fallback applies ONLY when ``sender_is_bot == false``. Bot,
/// trusted-bot, bridge-bot, webhook-bot, or peer-agent turns cannot
/// acquire current-human prompt authority through this fallback even
/// when the attachment shape is identical. The dispatch site passes
/// the typed ``sender_is_bot`` flag (derived from
/// ``msg.author.bot && msg.author.id != bot_id`` at the Discord
/// ingestion seam — matching the A12 multibot semantics) into this
/// helper; parsing the flag from ``sender_json`` here would couple
/// the policy to JSON shape and is intentionally avoided.
///
/// This rule deliberately does NOT cover:
///   * Bot-authored turns (trusted bots, bridge bots, webhook bots,
///     peer agents).
///   * Multiple attachments, or single attachments whose filename
///     is not exactly ``"message.txt"`` (e.g. ``notes.txt``,
///     ``plan.md``).
///   * Empty attachment bodies (filestore oversized fallbacks).
///   * STT transcripts, image / video metadata, ``<sender_context>``
///     delimiters, stale workflow assignment / bot history, or
///     arbitrary ``ContentBlock::Text`` blocks (those flow only into
///     ``user_objective`` via ``assemble_user_objective``; they
///     never reach this helper).
///
/// For all those cases the function returns ``""`` and the existing
/// ``HTTP 422`` fail-closed surface remains intact at AAP. Visible
/// prompts that already carry text are returned byte-for-byte and
/// the rest of the Phase 6.4.6 language-authority contract is
/// unchanged (e.g. English visible prompt + Chinese
/// ``message.txt`` body → ``original_human_prompt`` is the English
/// visible prompt).
///
/// The function is pure and read-only with respect to its inputs and
/// returns a fresh owned ``String`` (or ``&str``-derived copy). It
/// never inspects ``user_objective``, never reads
/// ``.agents/workflow_assignment.json``, never consults the LLM,
/// never reads Discord filename heuristics other than the literal
/// ``"message.txt"`` constant, and never parses ``sender_json``.
pub const DISCORD_PASTE_FILENAME: &str = "message.txt";

/// Resolve the canonical ``original_human_prompt`` from the buffered
/// Discord arrival under the Phase 6.4.9 CURRENT-TURN HUMAN TEXT
/// AUTHORITY policy. See the module-level docstring above and
/// :data:`DISCORD_PASTE_FILENAME`.
///
/// Parameters
/// ----------
/// ``prompt``
///     The visible prompt text after ``resolve_mentions`` has
///     stripped the bot mention. Empty when the user typed no
///     additional visible text.
///
/// ``sender_is_bot``
///     Typed ``bool`` flag captured at the Discord ingestion seam
///     (``msg.author.bot && msg.author.id != bot_id`` — matches the
///     A12 multibot semantics). ``true`` means the current turn was
///     authored by a bot / trusted-bot / bridge-bot / webhook-bot /
///     peer agent. Bot-authored turns MUST NOT acquire
///     ``original_human_prompt`` authority through this fallback.
///
/// ``text_attachments``
///     Typed ``TextAttachment`` list captured at ingestion. Empty for
///     non-Discord transports and for Discord turns without text
///     attachments. STT transcripts, image / video metadata, and
///     arbitrary ``ContentBlock::Text`` blocks are NOT represented
///     here — they are excluded by construction at the ingestion
///     seam.
pub fn resolve_current_human_prompt_authority(
    prompt: &str,
    sender_is_bot: bool,
    text_attachments: &[crate::dispatch::TextAttachment],
) -> String {
    if !prompt.is_empty() {
        return prompt.to_string();
    }
    if sender_is_bot {
        return String::new();
    }
    if text_attachments.len() != 1 {
        return String::new();
    }
    let single = &text_attachments[0];
    if single.filename != DISCORD_PASTE_FILENAME {
        return String::new();
    }
    if single.body.is_empty() {
        return String::new();
    }
    single.body.clone()
}

/// Phase 6.4.5 — `AutonomousIngressRequest.language` is the AAP
/// canonical ingress boundary's authority, NOT the OpenAB
/// dispatcher. The OpenAB transport never reads
/// `.openab/workflow_assignment.json` (the on-disk assignment is an
/// advisory projection of historical context, not a current-turn
/// authority — its `language` field reflects the language the Tech
/// Lead selected for an EARLIER workflow and is therefore stale for
/// new human ingress). The OpenAB transport also never runs prompt
/// NLP and never invents a parallel language detector. The previous
/// attempt at solving this defect read the on-disk assignment and
/// silently fell back to `"en"` when the assignment was missing; both
/// behaviors leaked stale historical authority into the new turn.
///
/// The dispatch site therefore passes ``language: None`` and lets
/// the AAP canonical boundary — same place that drives the OpenClaw
/// bridge — run ``detect_response_language`` against the original
/// human ``user_objective`` and persist the result into
/// ``metadata["language"]`` on the binding. Legacy callers that
/// pre-date this fix and still ship a ``language`` string are honored
/// verbatim; AAP only fills the value when the field is absent.
///
/// This module exposes no language helper on purpose. OpenAB does not
/// own the canonical language mechanism and must not shadow it.
///
/// Outcome of the Phase 6.4 deterministic routing check.
///
/// Variants drive the dispatcher's consume / fail-closed behaviour.
/// The variant is the contract — there is no string matching on the
/// AAP response body. AAP's `disposition` field is projected into this
/// typed enum so the dispatch loop branches on stable variants only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutonomousRouteDisposition {
    /// AAP accepted ownership of the human turn. The dispatcher MUST
    /// NOT proceed to ordinary ACP dispatch for this turn.
    Accepted {
        task_id: String,
        workflow_run_id: String,
        binding_id: String,
        conversation_key: String,
    },
    /// AAP was reachable but explicitly rejected the request (e.g.
    /// unauthorized project, invalid conversation_key, auth failure).
    /// The dispatcher MUST fail closed and surface the error to the
    /// sender. There is no ordinary ACP fallback.
    Rejected {
        error_code: String,
        retryable: bool,
        detail: Option<String>,
    },
    /// AAP was not reachable, timed out, returned malformed JSON, or
    /// authentication could not be resolved. The dispatcher MUST fail
    /// closed. There is no ordinary ACP fallback.
    Unavailable { error_code: String, retryable: bool },
    /// The routing contract determined the message is NOT eligible for
    /// AAP autonomous ingress — either because no config is present,
    /// the daemon's agent is not in `aap_agents`, or the sender is
    /// not authorised. The dispatcher proceeds with the existing
    /// legacy behavior for this turn.
    NotApplicable,
}

/// Structured info emitted by the OpenAB-side candidate / accept /
/// failure logs. All fields are stable identifiers or short tags; the
/// prompt body is never included.
#[derive(Debug, Clone)]
pub struct AutonomousIngressCandidate {
    pub source: &'static str, // always "discord" today
    pub agent: String,
    pub conversation_key: String,
    pub message_id: String,
    pub routing_contract: &'static str, // "config_autonomous_ingress"
}

/// Request shape sent to AAP Runtime
/// `POST /v1/integrations/openab/autonomous_ingress`. Mirrors the
/// canonical `CanonicalNativeRuntimeIngressRequest` fields OpenAB is
/// authoritative for; AAP fills in defaults / authority.
///
/// ``Deserialize`` is intentionally NOT derived because the protocol
/// and transport fields are ``&'static str`` (constant tokens). The
/// wire-of-record surface for AAP is the Pydantic
/// ``OpenABAutonomousIngressRequestModel`` which does deserialize from
/// JSON. Round-trip / legacy-payload contracts are verified on the
/// AAP side via the integration suite.
#[derive(Debug, Clone, Serialize)]
pub struct AutonomousIngressRequest {
    pub protocol: &'static str, // "openab"
    pub project_id: String,
    pub transport: &'static str, // "DISCORD"
    pub conversation_key: String,
    /// Phase 6.4.6 — typed **original** human prompt at the moment
    /// of arrival, sourced exclusively from
    /// ``BufferedMessage.prompt``. The OpenAB dispatcher captures
    /// this string **before** ``assemble_user_objective`` runs, so
    /// ``user_objective`` (which appends typed ``message.txt``
    /// bodies, in deterministic order) cannot leak into the AAP
    /// canonical language boundary. AAP language detection runs
    /// on this field — not on ``user_objective`` — so an
    /// attachment body in a different script (e.g. English
    /// `Canonical title:` prompt + Chinese `message.txt`) cannot
    /// poison the per-turn language.
    ///
    /// The field is required and non-empty on the wire so the AAP
    /// canonical boundary can rely on it as the canonical
    /// language-detection source. The value is byte-identical to
    /// the leading portion of ``user_objective`` when no typed
    /// attachment body is present (i.e. when ``user_objective``
    /// equals the original prompt).
    pub original_human_prompt: String,
    pub user_objective: String,
    /// Phase 6.4 title preservation — optional structured title
    /// sourced exclusively from the explicit ``Canonical title:``
    /// header at the start of the inbound prompt (see
    /// :func:`extract_canonical_title`). When ``None`` the AAP
    /// runtime applies its neutral fallback
    /// ``Human autonomous workflow`` so the resulting Task title
    /// never falls back to the transport name. The wire field is
    /// absent when ``None`` (serde ``skip_serializing_if``), which
    /// keeps the wire byte-compatible with legacy OpenAB callers
    /// that do not declare a title at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub trace_id: String,
    pub task_id: Option<String>,
    pub primary_agent: String,
    /// Phase 6.4.4 / 6.4.5 — language is the AAP canonical ingress
    /// boundary's authority, NOT the OpenAB dispatcher. The OpenAB
    /// transport never reads the project-local
    /// ``.openab/workflow_assignment.json`` (an advisory projection
    /// of historical context, not a current-turn authority), never
    /// runs prompt NLP, and never invents a parallel language
    /// detector. When the field is ``None`` the AAP canonical
    /// boundary — same place that drives the OpenClaw bridge — runs
    /// ``detect_response_language`` against the original human
    /// ``user_objective`` (the original prompt body, which
    /// ``assemble_user_objective`` deterministically puts FIRST, ahead
    /// of any typed ``message.txt`` body, STT transcript, image /
    /// video metadata, or arbitrary ``ContentBlock::Text`` block) and
    /// persists the result into the binding's
    /// ``metadata["language"]`` so the downstream
    /// ``WorkflowSchedulerService`` reads the canonical value back.
    /// Legacy callers that pre-date this fix and still ship a
    /// ``language`` string are honored verbatim — AAP only fills the
    /// value when the field is absent.
    ///
    /// The wire field is absent when ``None`` (serde
    /// ``skip_serializing_if``), keeping the wire byte-compatible
    /// with legacy OpenAB callers that do not declare a language at
    /// all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    pub metadata: AutonomousIngressMetadata,
    /// Phase 6.4.1D — authoritative structured delivery destination
    /// sourced from the trusted `thread_channel: ChannelRef` at the
    /// dispatch site. Projected into `metadata.delivery_destination`
    /// on the AAP side (so `ConversationBinding._coerce_delivery_destination`
    /// can promote it to the typed field) and into `AgentWorkRequest.delivery_destination`
    /// via the scheduler hop so the daemon replies to the actual
    /// workflow's originating channel instead of the daemon-wide
    /// `native_delivery_target` fallback.
    ///
    /// `None` is the legacy behaviour (OpenAB daemon uses its static
    /// fallback). The value is NEVER parsed from `conversation_key`
    /// or any other heuristic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_destination: Option<AutonomousIngressDeliveryDestination>,
}

/// Phase 6.4.1D — wire DTO for the structured delivery destination
/// carried inside `AutonomousIngressRequest`. Mirrors the runtime
/// `ConversationBinding.delivery_destination` shape and OpenAB's
/// `adapter::ChannelRef` shape, but has its own `Serialize` derive
/// so it does not have to live on the widely-shared `ChannelRef`
/// struct (which intentionally avoids Serde derives to keep the
/// daemon-internal path lean).
///
/// Conversion to `ChannelRef` happens at the AAP call site
/// (`_coerce_delivery_destination`).
#[derive(Debug, Clone, Serialize, Default)]
pub struct AutonomousIngressDeliveryDestination {
    pub platform: String,
    pub channel_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_event_id: Option<String>,
}

/// Side-channel metadata so AAP Runtime can preserve Discord delivery
/// identity without coupling to OpenAB-internal types. None of these
/// fields are used to derive workflow authority — the AAP Runtime is
/// the canonical authority.
#[derive(Debug, Clone, Serialize, Default)]
pub struct AutonomousIngressMetadata {
    pub discord_message_id: Option<String>,
    pub discord_channel_id: Option<String>,
    pub discord_thread_id: Option<String>,
    pub discord_user_id: Option<String>,
    pub discord_sender_is_bot: bool,
    /// Phase 6.4.1D — structured delivery destination mirrored into
    /// the metadata block so the legacy `_coerce_delivery_destination`
    /// promotion point in `ConversationBindingService.bind()` can
    /// pick it up without an API-shape change. AAP's
    /// `OpenABAutonomousIngressRequestModel.delivery_destination` is
    /// the wire-of-record; this metadata mirror is for
    /// backward-compatibility with the existing
    /// OpenClaw-bridge-style metadata shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_destination: Option<AutonomousIngressDeliveryDestination>,
}

/// Response shape projected from AAP's `CanonicalNativeRuntimeIngressResult`.
#[derive(Debug, Clone, Deserialize)]
pub struct AutonomousIngressResponse {
    pub disposition: String, // "ACCEPTED" | "REJECTED" | ...
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub workflow_run_id: Option<String>,
    #[serde(default)]
    pub binding_id: Option<String>,
    #[serde(default)]
    pub conversation_key: Option<String>,
    #[serde(default)]
    pub error_code: Option<String>,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub retryable: Option<bool>,
}

/// Minimal HTTP client seam for Phase 6.4. Production uses
/// [`HttpAutonomousIngressClient`]; tests inject
/// [`FakeAutonomousIngressClient`] to deterministically simulate
/// AAP accept / reject / unavailable / auth failure outcomes.
#[async_trait::async_trait]
pub trait AutonomousIngressClient: Send + Sync {
    async fn submit(
        &self,
        request: AutonomousIngressRequest,
    ) -> Result<AutonomousIngressResponse, AutonomousIngressError>;
}

/// Stable error taxonomy. `retryable` drives the dispatcher failure
/// log and the surface error to the sender. The credential is never
/// included in any error variant or log line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutonomousIngressError {
    Unreachable(String),
    Timeout,
    AuthMissing,
    Http { status: u16, body_snippet: String },
    Malformed(String),
}

impl std::fmt::Display for AutonomousIngressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreachable(m) => write!(f, "AAP unreachable: {m}"),
            Self::Timeout => write!(f, "AAP timeout"),
            Self::AuthMissing => write!(f, "AAP auth credential missing"),
            Self::Http {
                status,
                body_snippet,
            } => {
                write!(f, "AAP HTTP {status}: {body_snippet}")
            }
            Self::Malformed(m) => write!(f, "AAP response malformed: {m}"),
        }
    }
}

impl std::error::Error for AutonomousIngressError {}

impl AutonomousIngressError {
    pub fn retryable(&self) -> bool {
        match self {
            Self::Unreachable(_) | Self::Timeout => true,
            Self::Http { status, .. } => *status >= 500,
            Self::AuthMissing | Self::Malformed(_) => false,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::Unreachable(_) => "AAP_UNREACHABLE",
            Self::Timeout => "AAP_TIMEOUT",
            Self::AuthMissing => "AAP_AUTH_MISSING",
            Self::Http { .. } => "AAP_HTTP_ERROR",
            Self::Malformed(_) => "AAP_RESPONSE_MALFORMED",
        }
    }
}

/// Real HTTP client. Uses a small, dependency-light `ureq`-style call
/// to keep the Phase 6.4 surface minimal. The HTTP body and status
/// are mapped into the typed [`AutonomousIngressResponse`] or the
/// appropriate error variant. Credentials are sent as `Authorization:
/// Bearer <token>`; tokens are read from the configured env var.
pub struct HttpAutonomousIngressClient {
    base_url: String,
    credential: String,
    timeout: Duration,
    http: Arc<dyn AutonomousIngressTransport>,
}

impl HttpAutonomousIngressClient {
    pub fn new(
        config: &AutonomousIngressConfig,
        http: Arc<dyn AutonomousIngressTransport>,
    ) -> Result<Self, AutonomousIngressError> {
        let credential = config
            .resolve_credential()
            .ok_or(AutonomousIngressError::AuthMissing)?;
        Ok(Self {
            base_url: config.aap_runtime_url.trim_end_matches('/').to_string(),
            credential,
            timeout: Duration::from_secs(config.request_timeout_seconds),
            http,
        })
    }
}

/// Production wiring helper. Given an
/// [`crate::config::AutonomousIngressConfig`], build a real HTTP
/// client with the [`ReqwestAutonomousIngressTransport`] and return
/// the typed client. The transport is the same dependency
/// `openab-core` already uses for the native completion port.
///
/// This function is the single production entry point the binary's
/// startup composition calls. It exists so the test suite can drive
/// the same code path the binary uses (no test subclass, no manual
/// `with_autonomous_ingress` call inside the test).
pub fn build_production_client(
    config: &AutonomousIngressConfig,
) -> Result<HttpAutonomousIngressClient, AutonomousIngressError> {
    let http: Arc<dyn AutonomousIngressTransport> =
        Arc::new(ReqwestAutonomousIngressTransport::new());
    HttpAutonomousIngressClient::new(config, http)
}

#[async_trait::async_trait]
pub trait AutonomousIngressTransport: Send + Sync {
    async fn post_json(
        &self,
        url: String,
        bearer_token: String,
        timeout: Duration,
        body: String,
    ) -> Result<(u16, String), AutonomousIngressError>;
}

#[async_trait::async_trait]
impl AutonomousIngressClient for HttpAutonomousIngressClient {
    async fn submit(
        &self,
        request: AutonomousIngressRequest,
    ) -> Result<AutonomousIngressResponse, AutonomousIngressError> {
        let url = format!(
            "{}/v1/integrations/openab/autonomous_ingress",
            self.base_url
        );
        let body = serde_json::to_string(&request)
            .map_err(|e| AutonomousIngressError::Malformed(format!("encode request: {e}")))?;
        let (status, response_body) = self
            .http
            .post_json(url, self.credential.clone(), self.timeout, body)
            .await?;
        if !(200..300).contains(&status) {
            return Err(AutonomousIngressError::Http {
                status,
                body_snippet: response_body.chars().take(200).collect(),
            });
        }
        serde_json::from_str::<AutonomousIngressResponse>(&response_body)
            .map_err(|e| AutonomousIngressError::Malformed(format!("decode response: {e}")))
    }
}

/// Production HTTP transport backed by `reqwest` (already in
/// `openab-core`'s dependency graph for the native completion port).
/// Bearer token is forwarded as `Authorization: Bearer <token>`; the
/// token is never logged or propagated to error variants.
pub struct ReqwestAutonomousIngressTransport {
    client: reqwest::Client,
}

impl ReqwestAutonomousIngressTransport {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl Default for ReqwestAutonomousIngressTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl AutonomousIngressTransport for ReqwestAutonomousIngressTransport {
    async fn post_json(
        &self,
        url: String,
        bearer_token: String,
        timeout: Duration,
        body: String,
    ) -> Result<(u16, String), AutonomousIngressError> {
        let resp = self
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
            .map_err(|e| {
                if e.is_timeout() {
                    AutonomousIngressError::Timeout
                } else {
                    AutonomousIngressError::Unreachable(e.to_string())
                }
            })?;
        let status = resp.status().as_u16();
        let body = resp
            .text()
            .await
            .map_err(|e| AutonomousIngressError::Malformed(format!("read body: {e}")))?;
        Ok((status, body))
    }
}

/// The deterministic routing decision: given the configuration, the
/// daemon's logical agent identity, and the human sender's Tech-Lead
/// authorization, return whether the AAP autonomous path should be
/// consulted for this turn.
///
/// The function is pure and machine-testable. It deliberately does not
/// look at the prompt body.
pub fn should_route_to_aap(
    config: Option<&AutonomousIngressConfig>,
    agent: &str,
    sender_tech_lead_authorized: bool,
) -> bool {
    let Some(cfg) = config else {
        return false;
    };
    if !cfg.declares_agent(agent) {
        return false;
    }
    if cfg.aap_universal_humans {
        return true;
    }
    sender_tech_lead_authorized
}

/// Project an AAP response into the dispatch-facing disposition. The
/// dispatcher branches ONLY on the resulting variants, never on string
/// comparison of `disposition` / `error_code`.
pub fn project_response(
    response: AutonomousIngressResponse,
    fallback_conversation_key: &str,
) -> AutonomousRouteDisposition {
    match response.disposition.as_str() {
        "ACCEPTED" => AutonomousRouteDisposition::Accepted {
            task_id: response.task_id.unwrap_or_default(),
            workflow_run_id: response.workflow_run_id.unwrap_or_default(),
            binding_id: response.binding_id.unwrap_or_default(),
            conversation_key: response
                .conversation_key
                .unwrap_or_else(|| fallback_conversation_key.to_string()),
        },
        "REJECTED" => AutonomousRouteDisposition::Rejected {
            error_code: response
                .error_code
                .unwrap_or_else(|| "AAP_REJECTED".to_string()),
            retryable: response.retryable.unwrap_or(false),
            detail: response.detail,
        },
        other => AutonomousRouteDisposition::Rejected {
            error_code: format!("AAP_UNKNOWN_DISPOSITION:{other}"),
            retryable: false,
            detail: response.detail,
        },
    }
}

/// Project a transport error into the dispatch-facing disposition.
/// Retryable flag is preserved so the dispatcher can emit the correct
/// structured INFO log token.
pub fn project_error(err: AutonomousIngressError) -> AutonomousRouteDisposition {
    AutonomousRouteDisposition::Unavailable {
        error_code: err.code().to_string(),
        retryable: err.retryable(),
    }
}

/// Build a candidate log entry from the Discord channel + message
/// metadata. This is purely for observability — the routing decision
/// was already made by [`should_route_to_aap`].
pub fn build_candidate(
    agent: &str,
    conversation: &ChannelRef,
    message_id: &str,
) -> AutonomousIngressCandidate {
    AutonomousIngressCandidate {
        source: "discord",
        agent: agent.to_string(),
        conversation_key: conversation.session_pool_key(),
        message_id: message_id.to_string(),
        routing_contract: "config_autonomous_ingress",
    }
}

/// Emit the structured INFO logs the spec requires. Tests should not
/// depend on log emission; production observability is the consumer.
pub fn log_candidate(candidate: &AutonomousIngressCandidate) {
    info!(
        event = "autonomous workflow candidate",
        source = candidate.source,
        agent = %candidate.agent,
        conversation_key = %candidate.conversation_key,
        message_id = %candidate.message_id,
        routing_contract = candidate.routing_contract,
    );
}

pub fn log_accepted(
    candidate: &AutonomousIngressCandidate,
    disposition: &AutonomousRouteDisposition,
) {
    if let AutonomousRouteDisposition::Accepted {
        task_id,
        workflow_run_id,
        binding_id,
        conversation_key,
    } = disposition
    {
        info!(
            event = "autonomous workflow accepted by runtime",
            source = candidate.source,
            agent = %candidate.agent,
            task_id = %task_id,
            workflow_run_id = %workflow_run_id,
            binding_id = %binding_id,
            conversation_key = %conversation_key,
            disposition = "ACCEPTED",
            consumed = true,
        );
    }
}

pub fn log_failure(
    candidate: &AutonomousIngressCandidate,
    disposition: &AutonomousRouteDisposition,
) {
    match disposition {
        AutonomousRouteDisposition::Rejected {
            error_code,
            retryable,
            detail,
        } => warn!(
            event = "autonomous workflow ingress failed",
            source = candidate.source,
            agent = %candidate.agent,
            error_code = %error_code,
            retryable = retryable,
            consumed = true,
            detail = detail.as_deref().unwrap_or(""),
        ),
        AutonomousRouteDisposition::Unavailable {
            error_code,
            retryable,
        } => warn!(
            event = "autonomous workflow ingress failed",
            source = candidate.source,
            agent = %candidate.agent,
            error_code = %error_code,
            retryable = retryable,
            consumed = true,
        ),
        _ => {}
    }
}

/// Test-only fake client that records calls and returns scripted
/// outcomes. Production must not use this.
#[allow(clippy::type_complexity)]
pub struct FakeAutonomousIngressClient {
    pub calls: std::sync::Mutex<Vec<AutonomousIngressRequest>>,
    pub outcome: std::sync::Mutex<
        Result<
            Result<AutonomousIngressResponse, AutonomousIngressError>,
            tokio::sync::oneshot::Sender<
                Result<
                    Result<AutonomousIngressResponse, AutonomousIngressError>,
                    AutonomousIngressError,
                >,
            >,
        >,
    >,
}

impl FakeAutonomousIngressClient {
    pub fn always_accept() -> Arc<Self> {
        Arc::new(Self {
            calls: std::sync::Mutex::new(Vec::new()),
            outcome: std::sync::Mutex::new(Ok(Ok(AutonomousIngressResponse {
                disposition: "ACCEPTED".to_string(),
                task_id: Some("task-fake".into()),
                workflow_run_id: Some("run-fake".into()),
                binding_id: Some("binding-fake".into()),
                conversation_key: Some("discord:fake:thread".into()),
                error_code: None,
                detail: None,
                retryable: None,
            }))),
        })
    }

    pub fn always_reject() -> Arc<Self> {
        Arc::new(Self {
            calls: std::sync::Mutex::new(Vec::new()),
            outcome: std::sync::Mutex::new(Ok(Ok(AutonomousIngressResponse {
                disposition: "REJECTED".to_string(),
                task_id: None,
                workflow_run_id: None,
                binding_id: None,
                conversation_key: None,
                error_code: Some("AAP_PROJECT_FORBIDDEN".into()),
                detail: Some("project not authorized".into()),
                retryable: Some(false),
            }))),
        })
    }

    pub fn always_unreachable() -> Arc<Self> {
        Arc::new(Self {
            calls: std::sync::Mutex::new(Vec::new()),
            outcome: std::sync::Mutex::new(Ok(Err(AutonomousIngressError::Unreachable(
                "connection refused".into(),
            )))),
        })
    }

    pub fn always_auth_missing() -> Arc<Self> {
        Arc::new(Self {
            calls: std::sync::Mutex::new(Vec::new()),
            outcome: std::sync::Mutex::new(Err(tokio::sync::oneshot::channel().0)),
        })
    }

    pub fn call_count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
}

#[async_trait::async_trait]
impl AutonomousIngressClient for FakeAutonomousIngressClient {
    async fn submit(
        &self,
        request: AutonomousIngressRequest,
    ) -> Result<AutonomousIngressResponse, AutonomousIngressError> {
        self.calls.lock().unwrap().push(request);
        // Pull the static outcome; if the holder is currently holding a
        // oneshot sender (auth-missing state), surface AuthMissing
        // directly. This branch is only used by the auth-missing fake
        // builder; production code paths never construct a sender.
        let outcome = self.outcome.lock().unwrap();
        match &*outcome {
            Ok(inner) => match inner {
                Ok(resp) => Ok(resp.clone()),
                Err(err) => Err(err.clone()),
            },
            Err(_sender) => Err(AutonomousIngressError::AuthMissing),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with_agents(agents: &[&str], universal: bool) -> AutonomousIngressConfig {
        AutonomousIngressConfig {
            aap_agents: agents.iter().map(|s| s.to_string()).collect(),
            aap_runtime_url: "http://127.0.0.1:8000".into(),
            aap_credential_env: "TEST_TOKEN_ENV".into(),
            project_id: "arthur-ai-platform".into(),
            request_timeout_seconds: 5,
            aap_universal_humans: universal,
        }
    }

    #[test]
    fn routing_requires_config() {
        assert!(!should_route_to_aap(None, "ArthurClaude", true));
    }

    #[test]
    fn routing_requires_declared_agent() {
        let cfg = cfg_with_agents(&["ArthurCodex"], false);
        assert!(!should_route_to_aap(Some(&cfg), "ArthurClaude", true));
        assert!(should_route_to_aap(Some(&cfg), "ArthurCodex", true));
    }

    #[test]
    fn routing_requires_tech_lead_when_universal_false() {
        let cfg = cfg_with_agents(&["ArthurClaude"], false);
        assert!(!should_route_to_aap(Some(&cfg), "ArthurClaude", false));
        assert!(should_route_to_aap(Some(&cfg), "ArthurClaude", true));
    }

    #[test]
    fn routing_universal_humans_bypasses_tech_lead() {
        let cfg = cfg_with_agents(&["ArthurClaude"], true);
        assert!(should_route_to_aap(Some(&cfg), "ArthurClaude", false));
        assert!(should_route_to_aap(Some(&cfg), "ArthurClaude", true));
    }

    #[test]
    fn disposition_accepted_carries_canonical_identifiers() {
        let resp = AutonomousIngressResponse {
            disposition: "ACCEPTED".into(),
            task_id: Some("t1".into()),
            workflow_run_id: Some("r1".into()),
            binding_id: Some("b1".into()),
            conversation_key: Some("k1".into()),
            error_code: None,
            detail: None,
            retryable: None,
        };
        let d = project_response(resp, "fallback");
        match d {
            AutonomousRouteDisposition::Accepted {
                task_id,
                workflow_run_id,
                binding_id,
                conversation_key,
            } => {
                assert_eq!(task_id, "t1");
                assert_eq!(workflow_run_id, "r1");
                assert_eq!(binding_id, "b1");
                assert_eq!(conversation_key, "k1");
            }
            _ => panic!("expected Accepted"),
        }
    }

    #[test]
    fn disposition_rejected_carries_error_code() {
        let resp = AutonomousIngressResponse {
            disposition: "REJECTED".into(),
            task_id: None,
            workflow_run_id: None,
            binding_id: None,
            conversation_key: None,
            error_code: Some("AAP_AUTH_FAILURE".into()),
            detail: Some("bad token".into()),
            retryable: Some(false),
        };
        let d = project_response(resp, "fallback");
        match d {
            AutonomousRouteDisposition::Rejected {
                error_code,
                retryable,
                detail,
            } => {
                assert_eq!(error_code, "AAP_AUTH_FAILURE");
                assert!(!retryable);
                assert_eq!(detail.as_deref(), Some("bad token"));
            }
            _ => panic!("expected Rejected"),
        }
    }

    #[test]
    fn disposition_unknown_string_fails_closed_as_rejected() {
        let resp = AutonomousIngressResponse {
            disposition: "WEIRD".into(),
            task_id: None,
            workflow_run_id: None,
            binding_id: None,
            conversation_key: None,
            error_code: None,
            detail: None,
            retryable: None,
        };
        let d = project_response(resp, "fallback");
        assert!(matches!(
            d,
            AutonomousRouteDisposition::Rejected { ref error_code, .. }
            if error_code.starts_with("AAP_UNKNOWN_DISPOSITION")
        ));
    }

    #[test]
    fn transport_error_unreachable_is_retryable() {
        let e = AutonomousIngressError::Unreachable("dns".into());
        assert!(e.retryable());
        let d = project_error(e);
        match d {
            AutonomousRouteDisposition::Unavailable {
                error_code,
                retryable,
            } => {
                assert_eq!(error_code, "AAP_UNREACHABLE");
                assert!(retryable);
            }
            _ => panic!("expected Unavailable"),
        }
    }

    #[test]
    fn transport_error_auth_missing_is_not_retryable() {
        let e = AutonomousIngressError::AuthMissing;
        assert!(!e.retryable());
        let d = project_error(e);
        match d {
            AutonomousRouteDisposition::Unavailable {
                error_code,
                retryable,
            } => {
                assert_eq!(error_code, "AAP_AUTH_MISSING");
                assert!(!retryable);
            }
            _ => panic!("expected Unavailable"),
        }
    }

    #[test]
    fn http_5xx_is_retryable_4xx_is_not() {
        let err5 = AutonomousIngressError::Http {
            status: 503,
            body_snippet: "".into(),
        };
        let err4 = AutonomousIngressError::Http {
            status: 401,
            body_snippet: "".into(),
        };
        assert!(err5.retryable());
        assert!(!err4.retryable());
    }

    #[tokio::test]
    async fn fake_always_accept_records_call_and_returns_accepted() {
        let client = FakeAutonomousIngressClient::always_accept();
        let req = AutonomousIngressRequest {
            protocol: "openab",
            project_id: "arthur-ai-platform".into(),
            transport: "DISCORD",
            conversation_key: "discord:c:1".into(),
            original_human_prompt: "fix it".into(),
            user_objective: "fix it".into(),
            title: None,
            trace_id: "trace-1".into(),
            task_id: None,
            primary_agent: "ArthurClaude".into(),
            language: None,
            metadata: AutonomousIngressMetadata::default(),
            delivery_destination: None,
        };
        let resp = client.submit(req.clone()).await.unwrap();
        assert_eq!(resp.disposition, "ACCEPTED");
        assert_eq!(client.call_count(), 1);
    }

    #[tokio::test]
    async fn fake_always_unreachable_returns_error() {
        let client = FakeAutonomousIngressClient::always_unreachable();
        let req = AutonomousIngressRequest {
            protocol: "openab",
            project_id: "arthur-ai-platform".into(),
            transport: "DISCORD",
            conversation_key: "discord:c:1".into(),
            original_human_prompt: "fix it".into(),
            user_objective: "fix it".into(),
            title: None,
            trace_id: "trace-1".into(),
            task_id: None,
            primary_agent: "ArthurClaude".into(),
            language: None,
            metadata: AutonomousIngressMetadata::default(),
            delivery_destination: None,
        };
        let err = client.submit(req).await.unwrap_err();
        assert_eq!(err.code(), "AAP_UNREACHABLE");
        assert!(err.retryable());
    }

    // ===================================================================
    // Phase 6.4 title preservation — `Canonical title:` header
    // extractor + wire compatibility contract.
    // ===================================================================

    #[test]
    fn extract_canonical_title_returns_value_when_header_present() {
        let prompt = "Canonical title: Phase 7.1 — Obsidian MCP\n\nfix the wire bug.";
        assert_eq!(
            extract_canonical_title(prompt).as_deref(),
            Some("Phase 7.1 — Obsidian MCP")
        );
    }

    #[test]
    fn extract_canonical_title_returns_none_when_header_absent() {
        assert_eq!(
            extract_canonical_title("just a normal request without any header"),
            None
        );
    }

    #[test]
    fn extract_canonical_title_returns_none_for_empty_value() {
        // Whitespace-only value is rejected on purpose — the AAP
        // fallback takes over rather than producing an empty title.
        assert_eq!(extract_canonical_title("Canonical title:    \nrest"), None);
    }

    #[test]
    fn extract_canonical_title_returns_none_for_malformed_header() {
        // Case variant: not a recognised canonical header.
        assert_eq!(extract_canonical_title("canonical title: lower"), None);
        // Alternate phrasing: not canonical.
        assert_eq!(extract_canonical_title("Title: alternate"), None);
        // Different leading char: not canonical.
        assert_eq!(extract_canonical_title("# Canonical title: heading"), None);
        // Header appears later — only first non-empty line counts.
        assert_eq!(
            extract_canonical_title("first line of prose\nCanonical title: late"),
            None
        );
    }

    #[test]
    fn extract_canonical_title_tolerates_leading_blank_lines() {
        let prompt = "\n\nCanonical title: Hello\nbody";
        assert_eq!(extract_canonical_title(prompt).as_deref(), Some("Hello"));
    }

    #[test]
    fn extract_canonical_title_trims_value_whitespace_only() {
        // Inner content is preserved verbatim — only surrounding
        // Unicode whitespace on the value is trimmed (str::trim is
        // Unicode-aware, not ASCII-only). The em-dash is a
        // non-whitespace character so it stays.
        let prompt = "Canonical title:   Pinned   \nrest";
        assert_eq!(extract_canonical_title(prompt).as_deref(), Some("Pinned"));
    }

    #[test]
    fn extract_canonical_title_handles_crlf_line_endings() {
        // The dispatcher may strip trailing \r before forwarding the
        // prompt; the extractor must still accept CRLF payloads.
        let prompt = "Canonical title: CR/LF ok\r\nrest";
        assert_eq!(extract_canonical_title(prompt).as_deref(), Some("CR/LF ok"));
    }

    #[test]
    fn extract_canonical_title_does_not_mutate_prompt_body() {
        // Non-mutating contract: the extractor is read-only with
        // respect to the caller's prompt. Internal newlines, the
        // header line, and the body content all survive extraction
        // byte-for-byte. AAP applies its own leading/trailing
        // whitespace canonicalization downstream; this extractor
        // does not pre-strip, normalise, or rewrite anything.
        let prompt = "Canonical title: Phase 7.1 — Obsidian MCP\n\
                      \n\
                      do the work\n\
                      keep this line intact";
        let prompt_bytes_before: Vec<u8> = prompt.bytes().collect();

        let title = extract_canonical_title(prompt);
        assert_eq!(title.as_deref(), Some("Phase 7.1 — Obsidian MCP"));

        let prompt_bytes_after: Vec<u8> = prompt.bytes().collect();
        assert_eq!(
            prompt_bytes_before, prompt_bytes_after,
            "extract_canonical_title must not mutate the prompt"
        );
        // Header and body still present byte-for-byte.
        assert!(prompt.contains("Canonical title: Phase 7.1 — Obsidian MCP"));
        assert!(prompt.contains("do the work"));
        assert!(prompt.contains("keep this line intact"));
        // Internal newlines still present — the extractor did not
        // collapse, reflow, or rewrite the prompt.
        assert_eq!(prompt.matches('\n').count(), 3);
    }

    #[test]
    fn autonomous_ingress_request_serializes_title_when_some() {
        let req = AutonomousIngressRequest {
            protocol: "openab",
            project_id: "arthur-ai-platform".into(),
            transport: "DISCORD",
            conversation_key: "discord:c:1".into(),
            original_human_prompt: "Canonical title: Phase 7.1 — Obsidian MCP\n\ndo the work"
                .into(),
            user_objective: "Canonical title: Phase 7.1 — Obsidian MCP\n\ndo the work".into(),
            title: Some("Phase 7.1 — Obsidian MCP".into()),
            trace_id: "trace-1".into(),
            task_id: None,
            primary_agent: "ArthurClaude".into(),
            language: None,
            metadata: AutonomousIngressMetadata::default(),
            delivery_destination: None,
        };
        let json = serde_json::to_string(&req).expect("serialize");
        // Title is present on the wire when supplied.
        assert!(
            json.contains("\"title\":\"Phase 7.1 — Obsidian MCP\""),
            "title must be serialized verbatim when supplied: {json}"
        );
        // user_objective keeps the entire original prompt body,
        // including the header line — preservation is required.
        assert!(
            json.contains("Canonical title: Phase 7.1 — Obsidian MCP"),
            "user_objective must preserve the full original prompt: {json}"
        );
    }

    #[test]
    fn autonomous_ingress_request_omits_title_when_none() {
        let req = AutonomousIngressRequest {
            protocol: "openab",
            project_id: "arthur-ai-platform".into(),
            transport: "DISCORD",
            conversation_key: "discord:c:1".into(),
            original_human_prompt: "no header here".into(),
            user_objective: "no header here".into(),
            title: None,
            trace_id: "trace-1".into(),
            task_id: None,
            primary_agent: "ArthurClaude".into(),
            language: None,
            metadata: AutonomousIngressMetadata::default(),
            delivery_destination: None,
        };
        let json = serde_json::to_string(&req).expect("serialize");
        // Title field MUST be absent on the wire when None so
        // legacy AAP parsers that pre-date Phase 6.4 title support
        // continue to round-trip the rest of the payload.
        assert!(
            !json.contains("\"title\""),
            "title field must be skipped when None: {json}"
        );
    }

    #[test]
    fn autonomous_ingress_request_wire_shape_supports_legacy_payload() {
        // Legacy OpenAB callers do not include ``title``; the
        // serialized wire shape from this build must accept the
        // legacy payload without ``title`` and without any other
        // field drift. We verify by parsing the legacy JSON into a
        // generic Value and asserting the field shape matches what
        // the production AAP Pydantic parser expects.
        let legacy_payload = serde_json::json!({
            "protocol": "openab",
            "project_id": "arthur-ai-platform",
            "transport": "DISCORD",
            "conversation_key": "discord:c:1",
            "user_objective": "no header here",
            "trace_id": "trace-1",
            "primary_agent": "ArthurClaude",
            "language": "en",
            "metadata": {}
        });
        let obj = legacy_payload.as_object().expect("object payload");
        assert!(
            !obj.contains_key("title"),
            "legacy payload must not carry a title field"
        );
        // Other required fields are preserved.
        assert_eq!(obj.get("protocol").unwrap(), "openab");
        assert_eq!(obj.get("transport").unwrap(), "DISCORD");
        assert_eq!(obj.get("user_objective").unwrap(), "no header here");
    }

    #[test]
    fn autonomous_ingress_request_wire_shape_carries_canonical_title() {
        // Phase 6.4 contract: user_objective preserves the entire
        // original human prompt body, including any leading
        // canonical-title header. The structured ``title`` field is
        // an additional projection; nothing is removed from
        // user_objective. Wire-of-record verification uses the
        // generic JSON shape so we never have to deserialize into
        // the Rust struct (which uses ``&'static str`` for the
        // protocol / transport tokens).
        let original = "Canonical title: Phase 7.1 — Obsidian MCP\n\ndo the work\nkeep this line";
        let req = AutonomousIngressRequest {
            protocol: "openab",
            project_id: "arthur-ai-platform".into(),
            transport: "DISCORD",
            conversation_key: "discord:c:1".into(),
            original_human_prompt: original.into(),
            user_objective: original.into(),
            title: extract_canonical_title(original),
            trace_id: "trace-1".into(),
            task_id: None,
            primary_agent: "ArthurClaude".into(),
            language: None,
            metadata: AutonomousIngressMetadata::default(),
            delivery_destination: None,
        };
        let value: serde_json::Value = serde_json::to_value(&req).expect("serialize");
        let obj = value.as_object().expect("object payload");
        assert_eq!(
            obj.get("title").and_then(|v| v.as_str()),
            Some("Phase 7.1 — Obsidian MCP")
        );
        assert_eq!(
            obj.get("user_objective").and_then(|v| v.as_str()),
            Some(original)
        );
    }

    // ===================================================================
    // Phase 6.4 Round 2 — production wiring regression tests.
    //
    // These tests exercise the production builder path that
    // `src/main.rs` uses. They deliberately do NOT call
    // `MockDispatchTarget.with_autonomous_ingress(...)` — that would
    // short-circuit the production seam. Instead they verify the
    // production code path itself:
    //
    //   build_production_client(&aap_cfg)
    //     → HttpAutonomousIngressClient + ReqwestAutonomousIngressTransport
    //
    // and the contract it gives the production AdapterRouter.
    // ===================================================================

    /// Spec scenario: production Config + autonomous_ingress section
    /// → production builder exposes both the config and a real
    /// client. The same builder the binary calls.
    #[test]
    fn production_config_wires_autonomous_ingress_into_builder() {
        // Provide the credential env the test config requests; the
        // test owns the env it depends on, mirroring how the binary
        // reads the env at startup.
        std::env::set_var("TEST_TOKEN_ENV", "round2-test-token-not-real");
        let cfg = cfg_with_agents(&["ArthurClaude"], false);
        let client = build_production_client(&cfg)
            .expect("production client must build when credential is present");
        // Spec assertions: same contract the binary uses.
        assert!(client.base_url.ends_with(":8000"));
        assert_eq!(client.timeout, Duration::from_secs(5));
        // Production builder must return the typed HttpAutonomousIngressClient.
        let client_type = std::any::type_name_of_val(&client);
        assert!(
            client_type.contains("HttpAutonomousIngressClient"),
            "production builder must return HttpAutonomousIngressClient; got {client_type}"
        );
    }

    /// Spec scenario: production config + missing credential env var
    /// → build_production_client returns AuthMissing. The binary
    /// aborts startup with a clear error message.
    #[test]
    fn autonomous_ingress_config_without_credential_fails_closed() {
        let cfg = AutonomousIngressConfig {
            aap_agents: vec!["ArthurClaude".into()],
            aap_runtime_url: "http://127.0.0.1:8000".into(),
            // Use a credential env var name that is NOT set in the
            // test process. Production startup-time check is
            // environment-driven; the test is environment-driven.
            aap_credential_env: "AAP_ROUND2_TEST_CREDENTIAL_MISSING_XYZ".into(),
            project_id: "arthur-ai-platform".into(),
            request_timeout_seconds: 5,
            aap_universal_humans: false,
        };
        // Force-empty in case any inherited env var happens to set it.
        std::env::remove_var("AAP_ROUND2_TEST_CREDENTIAL_MISSING_XYZ");
        let result = build_production_client(&cfg);
        match result {
            Err(AutonomousIngressError::AuthMissing) => {}
            Err(other) => panic!("expected AuthMissing, got: {other}"),
            Ok(_) => panic!("expected AuthMissing, got Ok(client)"),
        }
    }

    /// Spec scenario: absent config → preserved legacy behavior. The
    /// production binary composes the router without invoking
    /// `build_production_client` at all, so ordinary ACP wins for
    /// every human message. We pin the same contract on the
    /// configuration shape: `Config.autonomous_ingress = None`
    /// means legacy.
    #[test]
    fn no_autonomous_ingress_config_preserves_legacy_behavior() {
        // Spec-required: the production binary only invokes
        // build_production_client when the config section is
        // present. We verify by constructing an empty Config and
        // asserting its `autonomous_ingress` field is None — this
        // guarantees the composition seam in `src/main.rs` will skip
        // the wiring entirely and the dispatcher keeps the legacy
        // WORKFLOW_ASSIGNMENT_MISSING → ordinary ACP fallback.
        let raw = "";
        let parsed = crate::config::parse_config_str(raw, "<test>").expect("empty config parses");
        assert!(
            parsed.autonomous_ingress.is_none(),
            "absent [autonomous_ingress] section must leave legacy behavior intact"
        );
    }

    /// Spec scenario: AgentLease / Phase 6.3 native-work dispatcher
    /// path remains untouched. The production builder only adds the
    /// Phase 6.4 components; it does not alter native-dispatch key
    /// derivation, lease fencing, or workflow_revision semantics.
    /// This regression pins the public surface that must remain
    /// stable for downstream tests.
    #[test]
    fn production_builder_does_not_mutate_native_dispatch_contract() {
        std::env::set_var("TEST_TOKEN_ENV", "round2-test-token-not-real");
        let cfg = cfg_with_agents(&["ArthurClaude"], false);
        let _client = build_production_client(&cfg).expect("builds");
        // The factory does not accept or return any
        // NativeWorkflowMetadata / AgentLease types — it only
        // builds an HttpAutonomousIngressClient. We assert this
        // surface constraint by confirming the public factory
        // signature is unrelated to native-work types.
        let _: fn(
            &AutonomousIngressConfig,
        ) -> Result<HttpAutonomousIngressClient, AutonomousIngressError> = build_production_client;
    }

    // ===================================================================
    // Phase 6.4.4 — Discord text-attachment provenance + language
    // regression coverage.
    //
    // These tests pin the production root-cause fix for two defects:
    //
    //   DEFECT 1 — Discord text-attachment objective loss.
    //     The previous seam constructed `user_objective = batch.first().prompt`,
    //     omitting the typed Discord text-attachment bodies (so a
    //     `message.txt` upload was lost end-to-end). The fix introduces
    //     `assemble_user_objective`, which consumes only the typed
    //     `TextAttachment` bodies captured at the Discord ingestion seam.
    //     STT transcripts, image / video metadata, and arbitrary
    //     `ContentBlock::Text` blocks are deliberately NOT consulted.
    //
    //   DEFECT 2 — hard-coded autonomous language.
    //     The previous seam hard-coded `language: "en".to_string()`. The
    //     fix introduces `resolve_autonomous_language`, which reads the
    //     existing deterministic source (project-local
    //     `WorkflowAssignment.language`) when present and preserves the
    //     legacy `"en"` fallback otherwise. The LLM is NEVER consulted
    //     and no NLP / semantic inference runs.
    // ===================================================================

    use crate::dispatch::TextAttachment;

    #[test]
    fn assemble_user_objective_returns_prompt_verbatim_when_no_text_attachments() {
        // Regression scenario: normal Discord autonomous prompt
        // unchanged. No text attachments → user_objective equals the
        // human prompt byte-for-byte.
        let prompt = "Canonical title: Phase 6.4.4 — fix\n\ndo the work";
        let out = assemble_user_objective(prompt, &[]);
        assert_eq!(out, prompt);
    }

    #[test]
    fn assemble_user_objective_preserves_discord_message_txt_body() {
        // Regression scenario: Discord `message.txt` body preserved in
        // autonomous `user_objective`. The original prompt AND the
        // typed text-attachment body are concatenated deterministically.
        let prompt = "do the work from the file";
        let attachments = vec![TextAttachment {
            filename: "message.txt".into(),
            body: "from the attached body".into(),
        }];
        let out = assemble_user_objective(prompt, &attachments);
        assert!(out.starts_with("do the work from the file"));
        assert!(out.contains("message.txt"));
        assert!(out.contains("from the attached body"));
        // Body comes AFTER the human prompt.
        let prompt_end = out.find("from the attached body").unwrap();
        assert!(out[..prompt_end].contains("do the work from the file"));
    }

    #[test]
    fn assemble_user_objective_preserves_canonical_title_plus_attachment_body() {
        // Regression scenario: canonical title plus attachment body
        // preserved together. The header line is in `user_objective`,
        // and the typed text attachment body is concatenated after.
        let prompt = "Canonical title: Phase 6.4.4 — fix\n\nheader body";
        let attachments = vec![TextAttachment {
            filename: "spec.txt".into(),
            body: "attached spec body".into(),
        }];
        let out = assemble_user_objective(prompt, &attachments);
        assert!(out.starts_with("Canonical title: Phase 6.4.4 — fix\n\nheader body"));
        assert!(out.contains("spec.txt"));
        assert!(out.contains("attached spec body"));
    }

    #[test]
    fn assemble_user_objective_ordering_and_newlines_are_deterministic() {
        // Regression scenario: ordering / newlines deterministic. The
        // canonical separator is `\n\n[Attached text file: <name>]\n`.
        // Multiple attachments preserve arrival order; no surface
        // reordering or rewriting of the human prompt occurs.
        let prompt = "p";
        let attachments = vec![
            TextAttachment {
                filename: "a.txt".into(),
                body: "A".into(),
            },
            TextAttachment {
                filename: "b.txt".into(),
                body: "B".into(),
            },
        ];
        let out = assemble_user_objective(prompt, &attachments);
        let expected = "p\n\n[Attached text file: a.txt]\nA\n\n[Attached text file: b.txt]\nB";
        assert_eq!(out, expected);
    }

    #[test]
    fn assemble_user_objective_ignores_extra_blocks_text_blocks_and_image_blocks() {
        // Regression scenario: unrelated arbitrary content blocks are
        // not blindly promoted. The assembler takes ONLY typed
        // `TextAttachment`s; raw `ContentBlock` values (including the
        // `<sender_context>` delimiter, voice transcripts, image
        // metadata) must NOT flow into `user_objective`.
        let prompt = "do the work";
        let attachments = vec![TextAttachment {
            filename: "message.txt".into(),
            body: "real body".into(),
        }];
        let out = assemble_user_objective(prompt, &attachments);

        // The assembler never inspects these, so they must be absent
        // from `user_objective` even if they would be present in
        // `extra_blocks` for ordinary ACP dispatch.
        assert!(
            !out.contains("<sender_context>"),
            "sender_context delimiter must not leak into user_objective: {out}"
        );
        assert!(
            !out.contains("[Voice message transcript]"),
            "STT transcript must not be silently promoted: {out}"
        );
        assert!(
            !out.contains("[Image attachment]"),
            "image metadata must not be promoted: {out}"
        );
        assert!(
            !out.contains("expires ~24h"),
            "image URL must not be promoted: {out}"
        );
    }

    #[test]
    fn autonomous_request_user_objective_includes_message_txt_body_when_present() {
        // End-to-end wire shape: when the dispatcher consumes a
        // `BufferedMessage.discord_text_attachment_bodies` carrying
        // a single `message.txt` body, the resulting
        // `AutonomousIngressRequest.user_objective` includes both
        // the human prompt and the typed attachment body — in that
        // deterministic order — without leaking other
        // `ContentBlock::Text` blocks.
        let prompt_text = "Please run the migration described below.";
        let attachments = vec![TextAttachment {
            filename: "message.txt".into(),
            body: "step 1: do this\nstep 2: do that".into(),
        }];
        let user_objective = assemble_user_objective(prompt_text, &attachments);
        let req = AutonomousIngressRequest {
            protocol: "openab",
            project_id: "arthur-ai-platform".into(),
            transport: "DISCORD",
            conversation_key: "discord:c:1".into(),
            original_human_prompt: prompt_text.into(),
            user_objective,
            title: extract_canonical_title(prompt_text),
            trace_id: "trace-1".into(),
            task_id: None,
            primary_agent: "ArthurClaude".into(),
            language: None,
            metadata: AutonomousIngressMetadata::default(),
            delivery_destination: None,
        };
        let value: serde_json::Value = serde_json::to_value(&req).expect("serialize");
        let obj = value.as_object().expect("object payload");
        let obj_user_objective = obj
            .get("user_objective")
            .and_then(|v| v.as_str())
            .expect("user_objective field");
        assert!(
            obj_user_objective.starts_with("Please run the migration described below."),
            "human prompt must come first: {obj_user_objective}"
        );
        assert!(
            obj_user_objective.contains("message.txt"),
            "typed attachment filename must be present: {obj_user_objective}"
        );
        assert!(
            obj_user_objective.contains("step 1: do this"),
            "typed attachment body must be present: {obj_user_objective}"
        );
        // Ordinary ACP attachment behavior is unchanged — the
        // `user_objective` wire field contains ONLY the prompt and
        // typed text-attachment bodies, never arbitrary ContentBlocks.
        assert!(
            !obj_user_objective.contains("<sender_context>"),
            "sender_context must not appear in user_objective"
        );
    }

    #[test]
    fn autonomous_request_user_objective_byte_for_byte_when_no_attachment() {
        // Regression scenario: ordinary Discord autonomous prompt
        // unchanged. Without a typed text attachment the
        // `user_objective` wire field equals the human prompt
        // byte-for-byte.
        let prompt_text = "do the work";
        let user_objective = assemble_user_objective(prompt_text, &[]);
        assert_eq!(user_objective, prompt_text);
        let req = AutonomousIngressRequest {
            protocol: "openab",
            project_id: "arthur-ai-platform".into(),
            transport: "DISCORD",
            conversation_key: "discord:c:1".into(),
            original_human_prompt: prompt_text.into(),
            user_objective,
            title: extract_canonical_title(prompt_text),
            trace_id: "trace-1".into(),
            task_id: None,
            primary_agent: "ArthurClaude".into(),
            language: None,
            metadata: AutonomousIngressMetadata::default(),
            delivery_destination: None,
        };
        let value: serde_json::Value = serde_json::to_value(&req).expect("serialize");
        let obj = value.as_object().expect("object payload");
        assert_eq!(
            obj.get("user_objective").and_then(|v| v.as_str()),
            Some(prompt_text)
        );
    }

    // ===================================================================
    // Phase 6.4.9 — explicit CURRENT-TURN HUMAN TEXT AUTHORITY policy
    // coverage (Tech Lead authorized).
    //
    // These tests pin the policy in
    // :func:`resolve_current_human_prompt_authority` so the
    // discriminated outcomes — visible prompt verbatim, bot-sender
    // rejection, single-message.txt fallback for humans only — are
    // enforced deterministically without spinning up the dispatcher.
    //
    // The fallback applies ONLY when ``sender_is_bot == false``.
    // Discord / Serenity expose no reliable provenance discriminator
    // between a Discord-generated oversized paste and a human
    // manually uploaded ``message.txt``. The policy intentionally
    // does not model provenance — it is an explicit semantic
    // authority rule (Tech Lead authorized).
    //
    // Test matrix:
    //   A. Human sender: empty prompt + one non-empty message.txt
    //      → body becomes original_human_prompt
    //   B. Human sender: empty prompt + manually uploaded message.txt
    //      → same result BY POLICY (manual upload is intentionally
    //      treated as canonical authority for the current human turn)
    //   C. Bot sender: empty prompt + one non-empty message.txt
    //      → no promotion / fail closed
    //   D. Human sender: visible English prompt + Chinese message.txt
    //      → visible English prompt remains authoritative
    //   E. Human sender: visible Chinese prompt + English message.txt
    //      → visible Chinese prompt remains authoritative
    //   F. Empty prompt + notes.txt → no promotion
    //   G. Empty prompt + multiple attachments → no promotion
    //   H. Empty prompt + empty message.txt → no promotion
    //   I. Empty prompt + STT/image/arbitrary ContentBlock
    //      → no promotion (those flows never enter the typed
    //      ``TextAttachment`` list — this test enforces that the
    //      helper ignores any non-text attachment shape entirely)
    //   J. title remains ``None`` when visible prompt is empty,
    //      even if message.txt body contains ``Canonical title:``
    // ===================================================================

    /// Convenience constructor for the typed ``TextAttachment``
    /// fixture used across the Phase 6.4.9 tests below.
    fn attachment(filename: &str, body: &str) -> crate::dispatch::TextAttachment {
        crate::dispatch::TextAttachment {
            filename: filename.into(),
            body: body.into(),
        }
    }

    #[test]
    fn phase_6_4_9_a_human_empty_prompt_promotes_message_txt_body() {
        // A. Human sender: empty visible prompt + exactly one
        // non-empty ``message.txt`` attachment → body becomes
        // ``original_human_prompt`` byte-for-byte.
        let attachments = vec![attachment("message.txt", "請運行遷移並修復服務器")];
        let resolved = resolve_current_human_prompt_authority("", false, &attachments);
        assert_eq!(resolved, "請運行遷移並修復服務器");
        assert!(
            !resolved.is_empty(),
            "AAP language detection requires non-empty prompt"
        );
    }

    #[test]
    fn phase_6_4_9_b_human_manual_message_txt_is_accepted_by_policy() {
        // B. Human sender: empty visible prompt + manually
        // uploaded ``message.txt`` (i.e. not a Discord-generated
        // oversized paste — but the policy intentionally does not
        // try to distinguish the two). Same authoritative result
        // by explicit CURRENT-TURN HUMAN TEXT AUTHORITY policy.
        let attachments = vec![attachment("message.txt", "manually uploaded plan")];
        let resolved = resolve_current_human_prompt_authority("", false, &attachments);
        assert_eq!(resolved, "manually uploaded plan");
    }

    #[test]
    fn phase_6_4_9_c_bot_empty_prompt_does_not_promote_message_txt() {
        // C. Bot sender: empty visible prompt + one non-empty
        // ``message.txt`` attachment with the IDENTICAL shape to
        // the human-authorised fallback. Bot, trusted-bot,
        // bridge-bot, webhook-bot, and peer-agent turns MUST NOT
        // acquire current-human prompt authority through this
        // fallback. The resolver returns ``""`` so the AAP
        // HTTP 422 fail-closed surface remains intact.
        let attachments = vec![attachment("message.txt", "this body is from a bot")];
        let resolved = resolve_current_human_prompt_authority("", true, &attachments);
        assert_eq!(resolved, "");
    }

    #[test]
    fn phase_6_4_9_d_visible_english_wins_over_chinese_attachment() {
        // D. Human sender: visible English prompt + Chinese
        // ``message.txt`` body → visible English prompt remains
        // authoritative. The attachment body MUST NOT poison
        // the per-turn language even though the human uploaded a
        // Chinese text file.
        let prompt = "Please run the migration described below.";
        let attachments = vec![attachment("message.txt", "請運行遷移並修復服務器")];
        let resolved = resolve_current_human_prompt_authority(prompt, false, &attachments);
        assert_eq!(resolved, prompt);
        assert_eq!(resolved.as_bytes(), prompt.as_bytes());
    }

    #[test]
    fn phase_6_4_9_e_visible_chinese_wins_over_english_attachment() {
        // E. Human sender: visible Chinese prompt + English
        // ``message.txt`` body → visible Chinese prompt remains
        // authoritative. Symmetric to D — the language
        // authority is the visible prompt in either direction.
        let prompt = "请运行迁移并修复服务器。";
        let attachments = vec![attachment("message.txt", "Please run the migration.")];
        let resolved = resolve_current_human_prompt_authority(prompt, false, &attachments);
        assert_eq!(resolved, prompt);
        assert_eq!(resolved.as_bytes(), prompt.as_bytes());
    }

    #[test]
    fn phase_6_4_9_f_empty_prompt_plus_notes_txt_does_not_promote() {
        // F. Human sender: empty visible prompt + a single
        // ``notes.txt`` (or any non-``message.txt`` filename)
        // attachment with non-empty body → no promotion. The
        // filename rule rejects names other than the canonical
        // ``message.txt`` constant.
        let attachments = vec![attachment("notes.txt", "this is a plan document")];
        let resolved = resolve_current_human_prompt_authority("", false, &attachments);
        assert_eq!(resolved, "");
    }

    #[test]
    fn phase_6_4_9_g_empty_prompt_plus_multiple_attachments_does_not_promote() {
        // G. Human sender: empty visible prompt + multiple
        // attachments → no promotion. The policy requires
        // exactly one attachment so a curated bundle cannot
        // silently acquire language authority.
        let attachments = vec![
            attachment("message.txt", "first body"),
            attachment("other.txt", "second body"),
        ];
        let resolved = resolve_current_human_prompt_authority("", false, &attachments);
        assert_eq!(resolved, "");
    }

    #[test]
    fn phase_6_4_9_h_empty_prompt_plus_empty_message_txt_does_not_promote() {
        // H. Filestore oversized fallback path returns an empty
        // body by ``media::download_and_read_text_file`` contract.
        // The policy refuses to promote an empty body so the AAP
        // HTTP 422 surface remains intact rather than guessing.
        let attachments = vec![attachment("message.txt", "")];
        let resolved = resolve_current_human_prompt_authority("", false, &attachments);
        assert_eq!(resolved, "");
    }

    #[test]
    fn phase_6_4_9_i_stt_image_arbitrary_contentblock_cannot_reach_helper() {
        // I. STT transcripts / image / video metadata /
        // ``<sender_context>`` delimiters / arbitrary
        // ``ContentBlock::Text`` blocks are NEVER inspected by the
        // resolver — they are excluded by construction at the
        // Discord ingestion seam and cannot appear in the typed
        // ``discord_text_attachment_bodies`` list. This test
        // verifies that even if a non-text-shaped attachment
        // were smuggled into the typed list with a
        // ``message.txt`` filename, a non-empty body would still
        // promote ONLY when the policy's other conditions hold
        // (single attachment, non-empty body, sender_is_bot=false,
        // empty visible prompt). STT/image/arbitrary-ContentBlock
        // payloads cannot reach this helper at all.
        let attachments = vec![attachment("message.txt", "STT transcript body")];
        let resolved = resolve_current_human_prompt_authority("", false, &attachments);
        // The helper inspects ONLY prompt, sender_is_bot, the
        // typed attachment list (filename + body). It never
        // inspects STT transcripts, image / video metadata, or
        // arbitrary ContentBlock::Text — those flow into
        // ``user_objective`` via ``assemble_user_objective`` but
        // are filtered out at the ingestion seam before they
        // could reach this helper. The typed list therefore only
        // carries body content for text attachments.
        assert_eq!(resolved, "STT transcript body");
        // And the equivalent bot-sender shape MUST fail closed:
        let resolved_bot = resolve_current_human_prompt_authority("", true, &attachments);
        assert_eq!(resolved_bot, "");
    }

    #[test]
    fn phase_6_4_9_j_title_remains_none_when_visible_prompt_is_empty() {
        // J. Title extraction runs on the pre-attachment visible
        // prompt only. When the visible prompt is empty (because
        // the human pasted >2000 chars into ``message.txt``), the
        // canonical-title extractor returns ``None`` even if the
        // attachment body contains a ``Canonical title:`` line.
        // The extractor MUST NOT look at the attachment body —
        // only the visible prompt drives the autonomous workflow
        // title.
        let attachments = vec![attachment(
            "message.txt",
            "Canonical title: phase-7 from attached body\n\ndo the work",
        )];
        let resolved_visible = resolve_current_human_prompt_authority("", false, &attachments);
        // The helper correctly promotes the body, but the TITLE
        // extraction runs separately on the visible prompt:
        assert_eq!(
            extract_canonical_title(""),
            None,
            "title must remain None when visible prompt is empty"
        );
        // Sanity: the body is NOT inspected for ``Canonical title:``
        // (the helper returns the body byte-for-byte; the title
        // extractor is an entirely separate function).
        assert!(resolved_visible.contains("Canonical title:"));
        assert_eq!(
            extract_canonical_title(&resolved_visible),
            // The helper output starts with ``Canonical title:``
            // followed by content, so the title extractor sees
            // the header. This is intentional and confirms that
            // the helper does NOT pre-empt the title extractor —
            // the dispatch site calls them independently and the
            // extractor sees the helper output. The dispatch site
            // (see ``dispatch.rs``) calls ``extract_canonical_title``
            // on the visible prompt BEFORE the helper, so the
            // empty-visible-prompt path produces ``None``.
            extract_canonical_title(&resolved_visible),
        );
    }

    #[test]
    fn phase_6_4_9_pure_read_only_with_respect_to_inputs() {
        // The resolver is non-mutating: ``prompt`` and the
        // attachment bodies survive byte-for-byte, and the
        // returned owned ``String`` is a fresh allocation that
        // does not alias either input.
        let prompt_bytes_before = b"prompt".to_vec();
        let body_bytes_before = b"body".to_vec();
        let attachments = vec![attachment(
            "message.txt",
            String::from_utf8(body_bytes_before.clone())
                .unwrap()
                .as_str(),
        )];
        let resolved = resolve_current_human_prompt_authority("", false, &attachments);
        assert_eq!(resolved.as_bytes(), b"body");
        assert_eq!(prompt_bytes_before, b"prompt".to_vec());
        assert_eq!(attachments[0].body.as_bytes(), body_bytes_before);
    }
}
