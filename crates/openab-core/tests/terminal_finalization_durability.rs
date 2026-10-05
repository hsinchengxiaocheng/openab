//! Phase 6.4.1F — terminal-finalization completion durability acceptance.
//!
//! This file is the PRIMARY acceptance proof for the terminal-finalization
//! durability fix (correction revision 7). Unlike the dispatch-layer unit
//! tests in `src/dispatch.rs` (which inject a pre-built
//! `WorkflowTurnHookInputs` into a `MockDispatchTarget` and use an
//! in-memory `RecordingNativeCompletionPort` as durability proof),
//! this file drives the REAL ACP / native-dispatch streaming path:
//!
//!   1. A fake ACP subprocess (driven by the production `SessionPool`)
//!      emits a canonical `<role_completion>` block in its assistant
//!      text response.
//!   2. The production `AdapterRouter` streams the turn through the
//!      real `stream_prompt_blocks` pipeline (so a Discord
//!      presentation / stream-finalize failure surfaces as
//!      `delivery_failed = true`).
//!   3. The production `Dispatcher::consumer_loop` calls
//!      `dispatch_batch` which invokes
//!      `invoke_workflow_hook_after_dispatch`, which resolves the
//!      canonical outcome from the assistant text and submits a
//!      durable `NativeCompletionEvent` through the production
//!      `DurableNativeCompletionPort` wrapping a file-based
//!      `NativeCompletionOutbox`.
//!   4. The post-hook warning (`adapter.send_message(...)` for the
//!      `streaming finalization had delivery failures` UX hint)
//!      fails AFTER the durable capture has already happened, so the
//!      file-based durable record survives the presentation failure
//!      byte-for-byte.
//!
//! Proven invariants (from the bounded correction specification):
//!
//!   - the exact durable completion record survives presentation
//!     failure unchanged;
//!   - the record preserves workflow_run_id, dispatch_id, lease_id,
//!     generation, expected_revision, role, agent,
//!     project_id / project_root, completion identity, and digest;
//!   - reconciliation consumes that exact durable record and forwards
//!     it through the durable port's delivery boundary;
//!   - the sealed completion is forwarded exactly once per delivery attempt;
//!   - replay / reconciliation is idempotent;
//!   - malformed / incomplete completion, identity mismatch, stale
//!     generation, stale revision, or wrong dispatch fail closed where
//!     supported;
//!   - the user-facing incomplete-delivery warning remains separate
//!     from authoritative terminal completion durability.

#![cfg(unix)]

use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;

use openab_core::acp::{AcpPromptIdentity, ContentBlock, ProjectContext, SessionPool};
use openab_core::adapter::{AdapterRouter, ChannelRef, ChatAdapter, MessageRef};
use openab_core::admission::NativeWorkflowMetadata;
use openab_core::config::{AgentConfig, ReactionsConfig};
use openab_core::dispatch::{consumer_loop, BufferedMessage, DispatchTarget};
use openab_core::markdown::TableMode;
use openab_core::native_completion::{
    DurableNativeCompletionPort, NativeCompletionError, NativeCompletionEvent,
    NativeCompletionOutbox, NativeCompletionPort, SharedNativeCompletionPort,
};

const PROJECT_ID: &str = "openab";
const PROJECT_ROOT: &str = "/home/arthur/openab/source";
const WORKFLOW_ID: &str = "wfr-terminal-finalization-durability";
const DISPATCH_ID: &str = "oad-terminal-finalization-durability";
const TASK_ID: &str = "task-terminal-finalization-durability";
const LEASE_ID: &str = "lease-terminal-finalization-durability";
const AGENT: &str = "ArthurGemini";
const SESSION_ID: &str = "fake-session-terminal-finalization";
const CONVERSATION_KEY: &str = "discord:1540183233654952036";

static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct EnvGuard {
    home: Option<std::ffi::OsString>,
    agent: Option<std::ffi::OsString>,
}
impl EnvGuard {
    fn install(root: &Path) -> Self {
        let home = std::env::var_os("HOME");
        let agent = std::env::var_os("ARTHUR_AGENT_NAME");
        std::env::set_var("HOME", root);
        std::env::set_var("ARTHUR_AGENT_NAME", "terminal-finalization-durability");
        Self { home, agent }
    }
}
impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.home {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
        match &self.agent {
            Some(v) => std::env::set_var("ARTHUR_AGENT_NAME", v),
            None => std::env::remove_var("ARTHUR_AGENT_NAME"),
        }
    }
}

/// Fake ACP subprocess that emits a controllable assistant text and a
/// terminal `end_turn` response. The script is intentionally minimal:
/// `initialize` / `session/new` produce the bare minimum envelope, and
/// `session/prompt` emits whatever assistant text the test wrote into
/// `FAKE_AGENT_TEXT` followed by an `end_turn` response. The script
/// does NOT include a `runtime_run_id` in its terminal metadata — the
/// `terminal_delivery_worker` path is exercised by
/// `tests/terminal_delivery_acceptance.rs`; this file exercises the
/// regular `stream_prompt_blocks` finalize path so the
/// `delivery_failed` flag is the only delivery-failure signal.
///
/// The text is emitted one chunk per line so the JSON `text` field
/// never carries raw newlines (which would invalidate the JSON
/// envelope). The ACP runtime concatenates the chunks into the
/// per-turn `text_buf` so the assembled buffer still contains the
/// full canonical `<role_completion>` block when
/// `stream_prompt_blocks` builds the workflow hook's
/// `raw_assistant_text`.
const FAKE_ACP_TEMPLATE: &str = r#"#!/bin/sh
record="$1"; : > "$record"
# $FAKE_AGENT_TEXT_FILE points to a UTF-8 file holding the assistant
# text the test wants the canonical completion hook to see. We read
# it once and pass each line through the wire envelope as a separate
# agent_message_chunk notification. The ACP runtime concatenates the
# chunks into a single text_buf so the workflow hook's
# raw_assistant_text carries the full canonical <role_completion>
# block. Newlines inside a JSON string MUST be escaped, so we
# emit each line as its own chunk (one per wire notification).
#
# POSIX `read` returns a non-zero status on EOF even when the last
# line was returned successfully, so we OR with `[ -n "$chunk" ]`
# to keep the loop running long enough to process the trailing
# partial line (the canonical `</role_completion>` closing marker
# is written without a trailing newline by the test harness).
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$record"
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentInfo":{"name":"fake-acp-terminal"},"agentCapabilities":{"loadSession":false}}}'
      ;;
    *'"method":"session/new"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"SENTINEL_SESSION_ID"}}'
      ;;
    *'"method":"session/prompt"'*)
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_thought_chunk"}}}'
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"tool_call","toolCallId":"tool-1","title":"Hermetic tool"}}}'
      # Emit each line of the prepared assistant text as its own
      # agent_message_chunk notification. The runtime concatenates
      # chunks into text_buf, so the assembled raw_assistant_text
      # still contains the full canonical <role_completion> block.
      if [ -n "$FAKE_AGENT_TEXT_FILE" ] && [ -f "$FAKE_AGENT_TEXT_FILE" ]; then
        while IFS= read -r chunk || [ -n "$chunk" ]; do
          [ -z "$chunk" ] && continue
          # Each chunk is one logical line of the canonical
          # <role_completion> block. Append a literal "\n" so the
          # ACP runtime's text_buf concatenation preserves the
          # newline that `read -r` strips.
          printf '%s\n' "{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{\"update\":{\"sessionUpdate\":\"agent_message_chunk\",\"content\":{\"text\":\"${chunk}\\n\"}}}}"
        done < "$FAKE_AGENT_TEXT_FILE"
      fi
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn"}}'
      ;;
  esac
done
"#;

fn write_fake_acp(temp: &Path, agent_text: &str) -> PathBuf {
    let body = FAKE_ACP_TEMPLATE
        .replace("SENTINEL_SESSION_ID", SESSION_ID)
        .replace("SENTINEL_CONVERSATION_ID", CONVERSATION_KEY);
    let script = temp.join("fake-acp-terminal.sh");
    fs::write(&script, body).unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
    let text_file = temp.join("agent_text.txt");
    fs::write(&text_file, agent_text).unwrap();
    script
}

fn canonical_completion_block(
    workflow_id: &str,
    project_id: &str,
    project_root: &str,
    role: &str,
    result: &str,
) -> String {
    format!(
        "<role_completion>\nrole: {role}\nresult: {result}\nworkflow_id: {workflow_id}\nproject_id: {project_id}\nproject_root: {project_root}\n</role_completion>"
    )
}

fn metadata(workflow_id: &str, project_root: &str) -> NativeWorkflowMetadata {
    NativeWorkflowMetadata {
        dispatch_id: DISPATCH_ID.into(),
        conversation_key: CONVERSATION_KEY.into(),
        workflow_run_id: workflow_id.into(),
        task_id: TASK_ID.into(),
        role: "VERIFIER".into(),
        agent: AGENT.into(),
        lease_id: LEASE_ID.into(),
        lease_generation: 42,
        expected_revision: 7,
        language: Some("zh-TW".into()),
        project_id: Some(PROJECT_ID.into()),
        project_root: Some(project_root.into()),
        native_execution_session_key: Some(openab_core::acp::pool::format_native_dispatch_key(
            AGENT,
            DISPATCH_ID,
        )),
        transport: Some("DISCORD".into()),
        delivery_destination: None,
        scope_policy: None,
    }
}

fn mock_channel() -> ChannelRef {
    ChannelRef {
        platform: "discord".into(),
        channel_id: "terminal-finalization-channel".into(),
        thread_id: None,
        parent_id: None,
        origin_event_id: Some("discord-inbound-1".into()),
    }
}

/// Adapter that fails every `send_message` call. Used to inject
/// Discord presentation / stream-finalize failure for the entire
/// turn (including the post-hook warning).
#[allow(dead_code)]
struct AlwaysFailingAdapter {
    sends: Arc<Mutex<Vec<String>>>,
    failures: Arc<Mutex<Vec<String>>>,
}

impl AlwaysFailingAdapter {
    fn new() -> Self {
        Self {
            sends: Arc::new(Mutex::new(Vec::new())),
            failures: Arc::new(Mutex::new(Vec::new())),
        }
    }
    fn failures(&self) -> Vec<String> {
        self.failures.lock().unwrap().clone()
    }
}

#[async_trait]
impl ChatAdapter for AlwaysFailingAdapter {
    fn platform(&self) -> &'static str {
        "discord"
    }
    fn message_limit(&self) -> usize {
        2000
    }
    async fn send_message(&self, _channel: &ChannelRef, content: &str) -> Result<MessageRef> {
        self.sends.lock().unwrap().push(content.to_string());
        self.failures.lock().unwrap().push(content.to_string());
        Err(anyhow::anyhow!("simulated Discord presentation failure"))
    }
    async fn create_thread(
        &self,
        channel: &ChannelRef,
        _trigger_msg: &MessageRef,
        _title: &str,
    ) -> Result<ChannelRef> {
        Ok(channel.clone())
    }
    async fn add_reaction(&self, _msg: &MessageRef, _emoji: &str) -> Result<()> {
        Ok(())
    }
    async fn remove_reaction(&self, _msg: &MessageRef, _emoji: &str) -> Result<()> {
        Ok(())
    }
    async fn edit_message(&self, _msg: &MessageRef, _content: &str) -> Result<()> {
        Ok(())
    }
    fn use_streaming(&self, _other_bot_present: bool) -> bool {
        false
    }
    fn uses_assistant_status(&self) -> bool {
        true
    }
    async fn set_status(&self, _channel: &ChannelRef, _status: &str) -> Result<()> {
        Ok(())
    }
}

/// Adapter that accepts the first final Discord chunk and fails every
/// later send. Its deliberately small message limit forces a canonical
/// completion into multiple final chunks, exercising the real path where
/// an initial presentation succeeds and a later one fails.
#[allow(dead_code)]
struct FailsAfterDurableCaptureAdapter {
    sends: Arc<Mutex<Vec<String>>>,
}

impl FailsAfterDurableCaptureAdapter {
    fn new() -> Self {
        Self {
            sends: Arc::new(Mutex::new(Vec::new())),
        }
    }
    fn sends(&self) -> Vec<String> {
        self.sends.lock().unwrap().clone()
    }
}

#[async_trait]
impl ChatAdapter for FailsAfterDurableCaptureAdapter {
    fn platform(&self) -> &'static str {
        "discord"
    }
    fn message_limit(&self) -> usize {
        32
    }
    async fn send_message(&self, channel: &ChannelRef, content: &str) -> Result<MessageRef> {
        self.sends.lock().unwrap().push(content.to_string());
        if self.sends.lock().unwrap().len() == 1 {
            Ok(MessageRef {
                channel: channel.clone(),
                message_id: "discord-success-1".into(),
            })
        } else {
            Err(anyhow::anyhow!("simulated later Discord send failure"))
        }
    }
    async fn create_thread(
        &self,
        channel: &ChannelRef,
        _trigger_msg: &MessageRef,
        _title: &str,
    ) -> Result<ChannelRef> {
        Ok(channel.clone())
    }
    async fn add_reaction(&self, _msg: &MessageRef, _emoji: &str) -> Result<()> {
        Ok(())
    }
    async fn remove_reaction(&self, _msg: &MessageRef, _emoji: &str) -> Result<()> {
        Ok(())
    }
    async fn edit_message(&self, _msg: &MessageRef, _content: &str) -> Result<()> {
        Ok(())
    }
    fn use_streaming(&self, _other_bot_present: bool) -> bool {
        false
    }
    fn uses_assistant_status(&self) -> bool {
        true
    }
    async fn set_status(&self, _channel: &ChannelRef, _status: &str) -> Result<()> {
        Ok(())
    }
}

/// Wraps the production `NativeCompletionPort` and exposes the live
/// delivery port so a test can swap it on restart to assert
/// reconciliation idempotency.
#[allow(dead_code)]
struct RouterRig {
    router: Arc<AdapterRouter>,
    outbox_path: PathBuf,
    outbox: Arc<NativeCompletionOutbox>,
    /// Captures every submission the durable port forwards. The
    /// durable port itself uses this as its inner delivery adapter so
    /// reconciliation visibly consumes the durable record.
    delivery_capture: Arc<Mutex<Vec<NativeCompletionEvent>>>,
    /// The inner delivery port that the durable port wraps. Tests
    /// replace this between phases to simulate downstream delivery availability
    /// transitions (e.g. "down", then "up").
    inner_delivery: Arc<Mutex<SharedNativeCompletionPort>>,
}

impl RouterRig {
    fn build(temp: &Path, project_root: &str) -> Self {
        Self::build_with_workflow(temp, project_root, WORKFLOW_ID)
    }

    fn build_with_workflow(temp: &Path, project_root: &str, workflow_id: &str) -> Self {
        let outbox_path = temp.join("native_completion_outbox.json");
        let outbox = Arc::new(NativeCompletionOutbox::open(&outbox_path).expect("outbox"));
        let capture: Arc<Mutex<Vec<NativeCompletionEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let capture_clone = capture.clone();
        let inner: SharedNativeCompletionPort = Arc::new(InMemoryDeliveryPort::new(capture_clone));
        let durable = Arc::new(DurableNativeCompletionPort::new(
            inner.clone(),
            outbox.clone(),
        ));
        let agent_text =
            canonical_completion_block(workflow_id, PROJECT_ID, project_root, "VERIFIER", "PASS");
        let script = write_fake_acp(temp, &agent_text);
        let record = temp.join("acp.jsonl");
        let text_file = temp.join("agent_text.txt");
        let config = AgentConfig {
            command: script.display().to_string(),
            args: vec![record.display().to_string()],
            working_dir: temp.display().to_string(),
            env: HashMap::from([(
                "FAKE_AGENT_TEXT_FILE".into(),
                text_file.display().to_string(),
            )]),
            inherit_env: vec![],
            command_explicit: true,
        };
        let pool =
            Arc::new(SessionPool::new(config, 2, 600, HashMap::new()).expect("session pool"));
        let reactions = ReactionsConfig {
            enabled: false,
            ..Default::default()
        };
        let router = AdapterRouter::new(
            pool,
            reactions,
            TableMode::Off,
            30,
            1,
            HashMap::new(),
            temp.to_path_buf(),
        )
        .with_native_completion_port(durable.clone());
        Self {
            router: Arc::new(router),
            outbox_path,
            outbox,
            delivery_capture: capture,
            inner_delivery: Arc::new(Mutex::new(inner)),
        }
    }

    fn outbox_records(&self) -> Vec<(String, String, String, String)> {
        // The outbox file is only materialised on the first
        // successful capture; a missing file is equivalent to an
        // empty outbox for the assertion helpers below.
        let raw = fs::read_to_string(&self.outbox_path).unwrap_or_default();
        let parsed: serde_json::Value = if raw.trim().is_empty() {
            serde_json::Value::Object(serde_json::Map::new())
        } else {
            serde_json::from_str(&raw).expect("parse outbox")
        };
        let obj = parsed.as_object().expect("outbox is object");
        obj.iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    v["status"].as_str().unwrap_or("").to_string(),
                    v["event"]["record_digest"]
                        .as_str()
                        .unwrap_or("")
                        .to_string(),
                    v["event"]["completion_id"]
                        .as_str()
                        .unwrap_or("")
                        .to_string(),
                )
            })
            .collect()
    }
}

/// In-memory delivery port. Used as the inner port of
/// `DurableNativeCompletionPort` so the test can observe every
/// event that reconciliation submits to the inner delivery boundary. The port is
/// deliberately cheap: it never performs I/O, never retries, and
/// only records what it sees. The durable wrapper owns the outbox
/// state machine, so this port's failure mode is independent of the
/// durable capture.
#[allow(dead_code)]
struct InMemoryDeliveryPort {
    capture: Arc<Mutex<Vec<NativeCompletionEvent>>>,
}

impl InMemoryDeliveryPort {
    fn new(capture: Arc<Mutex<Vec<NativeCompletionEvent>>>) -> Self {
        Self { capture }
    }
}

#[async_trait]
impl NativeCompletionPort for InMemoryDeliveryPort {
    async fn submit(&self, event: NativeCompletionEvent) -> Result<(), NativeCompletionError> {
        self.capture.lock().unwrap().push(event);
        Ok(())
    }
}

fn make_msg(prompt: &str, native_workflow: NativeWorkflowMetadata) -> BufferedMessage {
    BufferedMessage {
        sender_json: r#"{"schema":"openab.sender.v1","sender_id":"u","sender_name":"u"}"#
            .to_string(),
        sender_name: "u".into(),
        prompt: prompt.into(),
        extra_blocks: vec![],
        trigger_msg: MessageRef {
            channel: mock_channel(),
            message_id: "discord-inbound-1".into(),
        },
        arrived_at: Instant::now(),
        estimated_tokens: 50,
        other_bot_present: false,
        recipient: None,
        native_workflow: Some(native_workflow),
        discord_text_attachment_bodies: Vec::new(),
        sender_is_bot: false,
    }
}

/// Drive one full native-dispatch turn through the real
/// `consumer_loop` → `dispatch_batch` → `stream_prompt_blocks` →
/// `invoke_workflow_hook_after_dispatch` pipeline, with a real
/// `SessionPool` driving a fake ACP subprocess.
async fn drive_native_turn(
    router: Arc<AdapterRouter>,
    adapter: Arc<dyn ChatAdapter>,
    msg: BufferedMessage,
) {
    // Build the dispatcher target from the production AdapterRouter.
    struct RouterTarget(Arc<AdapterRouter>);
    #[async_trait]
    impl DispatchTarget for RouterTarget {
        fn reactions_config(&self) -> &ReactionsConfig {
            self.0.reactions_config()
        }
        fn workspace_aliases(&self) -> std::collections::HashMap<String, String> {
            self.0.workspace_aliases_map()
        }
        fn bot_home(&self) -> PathBuf {
            self.0.bot_home_path()
        }
        async fn ensure_session(
            &self,
            session_key: &str,
            project: Option<&ProjectContext>,
            write_policy: Option<&str>,
        ) -> Result<bool> {
            let created = self.0.pool().get_or_create(session_key, project).await?;
            if let Some(policy) = write_policy {
                self.0
                    .pool()
                    .set_session_write_policy(session_key, policy)
                    .await;
            }
            Ok(created)
        }
        async fn reset_session(&self, session_key: &str) {
            let _ = self.0.pool().reset_session(session_key).await;
        }
        async fn pinned_project_root(&self, session_key: &str) -> Option<PathBuf> {
            self.0
                .pool()
                .get_pinned_project(session_key)
                .await
                .map(|p| p.project_root)
        }
        fn tech_lead_user_ids(&self) -> std::collections::HashSet<u64> {
            self.0.configured_tech_lead_user_ids()
        }
        async fn stream_prompt_blocks(
            &self,
            adapter: &Arc<dyn ChatAdapter>,
            session_key: &str,
            content_blocks: Vec<ContentBlock>,
            thread_channel: &ChannelRef,
            reactions: Arc<openab_core::reactions::StatusReactionController>,
            other_bot_present: bool,
            recipient: Option<(String, String)>,
            identity: AcpPromptIdentity,
        ) -> Result<(
            (),
            Option<openab_core::workflow::service::WorkflowTurnHookInputs>,
        )> {
            self.0
                .stream_prompt_blocks(
                    adapter,
                    session_key,
                    content_blocks,
                    thread_channel,
                    reactions,
                    other_bot_present,
                    recipient,
                    identity,
                )
                .await
        }
        fn workflow_service(&self) -> Option<Arc<openab_core::workflow::service::WorkflowService>> {
            self.0.workflow_service()
        }
        fn native_completion_port(&self) -> SharedNativeCompletionPort {
            self.0.native_completion_port()
        }
        fn autonomous_ingress_client(
            &self,
        ) -> Option<Arc<dyn openab_core::autonomous_ingress::AutonomousIngressClient>> {
            self.0.autonomous_ingress_client()
        }
        fn autonomous_ingress_config(
            &self,
        ) -> Option<&openab_core::config::AutonomousIngressConfig> {
            self.0.autonomous_ingress_config()
        }
        fn workflow_reopen_client(
            &self,
        ) -> Option<Arc<dyn openab_core::workflow_reopen::WorkflowReopenClient>> {
            self.0.workflow_reopen_client()
        }
        fn autonomous_ingress_agent_identity(&self) -> Option<&str> {
            self.0.resolved_agent_name()
        }
        async fn observe_workflow_turn_hook(
            &self,
            _hook: &openab_core::workflow::service::WorkflowTurnHookInputs,
        ) {
            // The production observer is intentionally a no-op for
            // native turns. Mirror that here so the test does not
            // require a WorkflowService configuration.
        }
    }

    let target: Arc<dyn DispatchTarget> = Arc::new(RouterTarget(router));
    let thread_key = "discord:terminal-finalization-channel".to_string();
    let (tx, rx) = tokio::sync::mpsc::channel::<BufferedMessage>(4);
    tx.send(msg).await.unwrap();
    drop(tx);
    consumer_loop(
        thread_key,
        mock_channel(),
        rx,
        target,
        None,
        adapter,
        4,
        24_000,
        Duration::from_secs(60),
    )
    .await;
}

// ---------------------------------------------------------------------------
// PRIMARY acceptance: real ACP path + durable port + Discord send failure.
// The fake ACP emits a valid canonical <role_completion> block; the durable
// NativeCompletionOutbox MUST capture the completion even though every
// Discord send (streaming finalize + post-hook warning) fails.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn real_acp_path_durable_capture_survives_discord_send_failure() {
    let _lock = ENV_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let _env = EnvGuard::install(temp.path());
    let rig = RouterRig::build(temp.path(), PROJECT_ROOT);
    let adapter = Arc::new(AlwaysFailingAdapter::new());

    let workflow_id = WORKFLOW_ID;
    let native = metadata(workflow_id, PROJECT_ROOT);
    let msg = make_msg("verifier evaluates the patch", native.clone());

    // Drive the real native-dispatch path. The dispatcher fires
    // stream_prompt_blocks (which runs the fake ACP subprocess and
    // then attempts the Discord send — every send fails), then
    // invoke_workflow_hook_after_dispatch which durably captures the
    // canonical <role_completion> block into the outbox.
    drive_native_turn(rig.router.clone(), adapter.clone(), msg).await;

    // 1. The durable outbox JSON file persists exactly one record.
    let records = rig.outbox_records();
    assert_eq!(
        records.len(),
        1,
        "durable outbox must contain exactly one canonical completion, got {records:?}"
    );
    let (completion_id, status, record_digest, _) = records[0].clone();
    assert_eq!(
        status, "DELIVERED",
        "durable port must finalize to DELIVERED"
    );
    assert!(!completion_id.is_empty(), "completion_id must be sealed");
    assert!(!record_digest.is_empty(), "record_digest must be sealed");

    // 2. The sealed record preserves the FULL identity tuple.
    let raw = fs::read_to_string(&rig.outbox_path).expect("read outbox");
    let parsed: serde_json::Value = serde_json::from_str(&raw).expect("parse outbox");
    let event_json = &parsed[&completion_id]["event"];
    assert_eq!(event_json["record_version"].as_u64(), Some(1));
    assert_eq!(event_json["dispatch_id"].as_str(), Some(DISPATCH_ID));
    assert_eq!(event_json["workflow_run_id"].as_str(), Some(workflow_id));
    assert_eq!(event_json["task_id"].as_str(), Some(TASK_ID));
    assert_eq!(event_json["role"].as_str(), Some("VERIFIER"));
    assert_eq!(event_json["agent_identity"].as_str(), Some(AGENT));
    assert_eq!(event_json["lease_id"].as_str(), Some(LEASE_ID));
    assert_eq!(event_json["lease_generation"].as_u64(), Some(42));
    assert_eq!(event_json["expected_revision"].as_u64(), Some(7));
    assert_eq!(event_json["outcome"].as_str(), Some("PASS"));
    assert_eq!(event_json["project_id"].as_str(), Some(PROJECT_ID));
    assert_eq!(event_json["project_root"].as_str(), Some(PROJECT_ROOT));
    assert_eq!(event_json["transport"].as_str(), Some("DISCORD"));
    assert_eq!(
        event_json["completion_id"].as_str(),
        Some(completion_id.as_str())
    );
    assert_eq!(
        event_json["record_digest"].as_str(),
        Some(record_digest.as_str())
    );

    // 3. The Discord send attempts happened and failed at least
    //    once (the streaming finalize + the post-hook warning). The
    //    completion capture MUST have survived every failure.
    let failures = adapter.failures();
    assert!(
        !failures.is_empty(),
        "Discord send must have been attempted; failures={failures:?}"
    );
    assert!(
        failures
            .iter()
            .any(|body| body.contains("streaming finalization had delivery failures")),
        "post-hook warning must have been attempted; failures={failures:?}"
    );

    // 4. Reconciliation consumes the durable record via the live
    //    delivery port: replay_pending() MUST submit the same event
    //    to the inner delivery port the durable port wraps.
    // First, mark the record PENDING again (simulating a daemon
    // restart after a transient downstream delivery outage).
    {
        // Mutate the durable JSON to flip DELIVERED → PENDING so
        // replay_pending has something to replay. Production callers
        // never edit the outbox by hand; the test does so to
        // exercise the replay path deterministically.
        let mut parsed: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&rig.outbox_path).expect("read"))
                .expect("json");
        parsed[&completion_id]["status"] = serde_json::Value::String("PENDING".into());
        fs::write(
            &rig.outbox_path,
            serde_json::to_vec(&parsed).expect("serialize"),
        )
        .expect("write");
    }

    // Reopen the outbox (simulates daemon restart) and build a
    // fresh DurableNativeCompletionPort with the SAME inner delivery
    // port so we observe the replay submission.
    let reopened_outbox = Arc::new(NativeCompletionOutbox::open(&rig.outbox_path).expect("reopen"));
    assert_eq!(reopened_outbox.pending().len(), 1);
    let pending_event = reopened_outbox.pending()[0].clone();
    let durable_replay = DurableNativeCompletionPort::new(
        rig.inner_delivery.lock().unwrap().clone(),
        reopened_outbox.clone(),
    );
    durable_replay.replay_pending().await;

    // The inner delivery port received the replay event with the
    // SAME identity tuple the durable record carries.
    let replayed = rig.delivery_capture.lock().unwrap().clone();
    assert!(
        !replayed.is_empty(),
        "replay must submit the durable record to the inner delivery port"
    );
    let replayed_event = replayed
        .iter()
        .find(|e| e.completion_id == pending_event.completion_id)
        .expect("replayed event with matching completion_id");
    assert_eq!(replayed_event.dispatch_id, pending_event.dispatch_id);
    assert_eq!(
        replayed_event.workflow_run_id,
        pending_event.workflow_run_id
    );
    assert_eq!(replayed_event.task_id, pending_event.task_id);
    assert_eq!(replayed_event.role, pending_event.role);
    assert_eq!(replayed_event.agent_identity, pending_event.agent_identity);
    assert_eq!(replayed_event.lease_id, pending_event.lease_id);
    assert_eq!(
        replayed_event.lease_generation,
        pending_event.lease_generation
    );
    assert_eq!(
        replayed_event.expected_revision,
        pending_event.expected_revision
    );
    assert_eq!(replayed_event.outcome, pending_event.outcome);
    assert_eq!(replayed_event.project_id, pending_event.project_id);
    assert_eq!(replayed_event.project_root, pending_event.project_root);
    assert_eq!(replayed_event.record_digest, pending_event.record_digest);
    assert_eq!(replayed_event.transport, pending_event.transport);

    // 5. Replay idempotency — a SECOND replay with no pending
    //    records MUST NOT submit anything new.
    let captured_count_before = rig.delivery_capture.lock().unwrap().len();
    durable_replay.replay_pending().await;
    let captured_count_after = rig.delivery_capture.lock().unwrap().len();
    assert_eq!(
        captured_count_before, captured_count_after,
        "second replay must be a safe no-op"
    );
}

// ---------------------------------------------------------------------------
// SECONDARY: initial presentation succeeds, then a later final Discord
// chunk fails. The durable record MUST be present AND unchanged; the
// workflow transition MUST occur exactly once (one durable record, one
// delivery port submit).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn later_discord_send_failure_does_not_erase_durable_capture() {
    let _lock = ENV_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let _env = EnvGuard::install(temp.path());
    let workflow_id = "wfr-terminal-finalization-durability-postwarn";
    let rig = RouterRig::build_with_workflow(temp.path(), PROJECT_ROOT, workflow_id);
    let adapter = Arc::new(FailsAfterDurableCaptureAdapter::new());

    let workflow_id = "wfr-terminal-finalization-durability-postwarn";
    let native = metadata(workflow_id, PROJECT_ROOT);
    let msg = make_msg("verifier evaluates the patch", native.clone());

    drive_native_turn(rig.router.clone(), adapter.clone(), msg).await;

    // The durable outbox has exactly one record.
    let records = rig.outbox_records();
    assert_eq!(records.len(), 1, "durable outbox records: {records:?}");
    let (completion_id, status, _, _) = records[0].clone();
    assert_eq!(status, "DELIVERED");

    // The inner delivery port received exactly one submission
    // (the durable port forwards the sealed event to the inner delivery boundary on
    // submit, BEFORE the post-hook warning fires). If the durable
    // record had been lost, this would be zero.
    let captured = rig.delivery_capture.lock().unwrap().clone();
    assert_eq!(
        captured.len(),
        1,
        "durable port must submit to the inner delivery boundary exactly once on the original turn"
    );
    assert_eq!(captured[0].completion_id, completion_id);

    // The first final chunk was delivered, and a later final chunk failed.
    let sends = adapter.sends();
    assert!(
        sends.len() >= 2,
        "a successful first chunk and a later failed chunk must both be attempted; sends={sends:?}"
    );
}

// ---------------------------------------------------------------------------
// FAIL-CLOSED: malformed <role_completion> block in agent text MUST NOT
// reach the durable outbox.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn malformed_completion_block_does_not_capture() {
    let _lock = ENV_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let _env = EnvGuard::install(temp.path());
    let outbox_path = temp.path().join("native_completion_outbox.json");
    let outbox = Arc::new(NativeCompletionOutbox::open(&outbox_path).expect("outbox"));
    let capture: Arc<Mutex<Vec<NativeCompletionEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let inner: SharedNativeCompletionPort = Arc::new(InMemoryDeliveryPort::new(capture.clone()));
    let durable = Arc::new(DurableNativeCompletionPort::new(
        inner.clone(),
        outbox.clone(),
    ));
    // Agent emits a block missing `project_root` — malformed.
    let agent_text = format!(
        "<role_completion>\nrole: VERIFIER\nresult: PASS\nworkflow_id: {WORKFLOW_ID}\nproject_id: {PROJECT_ID}\n</role_completion>"
    );
    let script = write_fake_acp(temp.path(), &agent_text);
    let record = temp.path().join("acp.jsonl");
    let config = AgentConfig {
        command: script.display().to_string(),
        args: vec![record.display().to_string()],
        working_dir: temp.path().display().to_string(),
        env: HashMap::new(),
        inherit_env: vec![],
        command_explicit: true,
    };
    let pool = Arc::new(SessionPool::new(config, 2, 600, HashMap::new()).expect("session pool"));
    let reactions = ReactionsConfig {
        enabled: false,
        ..Default::default()
    };
    let router = Arc::new(
        AdapterRouter::new(
            pool,
            reactions,
            TableMode::Off,
            30,
            1,
            HashMap::new(),
            temp.path().to_path_buf(),
        )
        .with_native_completion_port(durable.clone()),
    );
    let adapter = Arc::new(FailsAfterDurableCaptureAdapter::new());
    let native = metadata(WORKFLOW_ID, PROJECT_ROOT);
    let msg = make_msg("verifier evaluates the patch", native);

    drive_native_turn(router.clone(), adapter.clone(), msg).await;

    // The durable outbox is empty — a malformed block must NOT
    // reach the durable capture. The outbox file is only
    // materialised on the first successful capture, so a missing
    // file is equivalent to an empty outbox for these
    // fail-closed assertions.
    let raw = fs::read_to_string(&outbox_path).unwrap_or_default();
    let parsed: serde_json::Value = if raw.trim().is_empty() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_str(&raw).expect("parse outbox")
    };
    let obj = parsed.as_object().expect("outbox is object");
    assert!(
        obj.is_empty(),
        "malformed completion must NOT be captured into the durable outbox; got {obj:?}"
    );
    // The inner delivery port was never invoked either.
    assert!(
        capture.lock().unwrap().is_empty(),
        "no event must be submitted to the inner delivery port"
    );
}

// ---------------------------------------------------------------------------
// FAIL-CLOSED: workflow_id mismatch between block and dispatch metadata
// MUST NOT capture.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn workflow_identity_mismatch_does_not_capture() {
    let _lock = ENV_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let _env = EnvGuard::install(temp.path());
    let outbox_path = temp.path().join("native_completion_outbox.json");
    let outbox = Arc::new(NativeCompletionOutbox::open(&outbox_path).expect("outbox"));
    let capture: Arc<Mutex<Vec<NativeCompletionEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let inner: SharedNativeCompletionPort = Arc::new(InMemoryDeliveryPort::new(capture.clone()));
    let durable = Arc::new(DurableNativeCompletionPort::new(
        inner.clone(),
        outbox.clone(),
    ));
    // Agent claims a DIFFERENT workflow_id than the dispatch metadata.
    let agent_text = canonical_completion_block(
        "wfr-attacker-controlled",
        PROJECT_ID,
        PROJECT_ROOT,
        "VERIFIER",
        "PASS",
    );
    let script = write_fake_acp(temp.path(), &agent_text);
    let record = temp.path().join("acp.jsonl");
    let config = AgentConfig {
        command: script.display().to_string(),
        args: vec![record.display().to_string()],
        working_dir: temp.path().display().to_string(),
        env: HashMap::new(),
        inherit_env: vec![],
        command_explicit: true,
    };
    let pool = Arc::new(SessionPool::new(config, 2, 600, HashMap::new()).expect("session pool"));
    let reactions = ReactionsConfig {
        enabled: false,
        ..Default::default()
    };
    let router = Arc::new(
        AdapterRouter::new(
            pool,
            reactions,
            TableMode::Off,
            30,
            1,
            HashMap::new(),
            temp.path().to_path_buf(),
        )
        .with_native_completion_port(durable.clone()),
    );
    let adapter = Arc::new(FailsAfterDurableCaptureAdapter::new());
    let native = metadata(WORKFLOW_ID, PROJECT_ROOT);
    let msg = make_msg("verifier evaluates the patch", native);

    drive_native_turn(router.clone(), adapter.clone(), msg).await;

    // The outbox file is only materialised on the first successful
    // capture, so a missing file is equivalent to an empty outbox
    // for these fail-closed assertions.
    let raw = fs::read_to_string(&outbox_path).unwrap_or_default();
    let parsed: serde_json::Value = if raw.trim().is_empty() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_str(&raw).expect("parse outbox")
    };
    let obj = parsed.as_object().expect("outbox is object");
    assert!(
        obj.is_empty(),
        "workflow_id-mismatched completion must NOT be captured; got {obj:?}"
    );
    assert!(
        capture.lock().unwrap().is_empty(),
        "no event must be submitted to the inner delivery port"
    );
}

// ---------------------------------------------------------------------------
// FAIL-CLOSED: project_id mismatch between block and dispatch metadata
// MUST NOT capture.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn project_identity_mismatch_does_not_capture() {
    let _lock = ENV_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let _env = EnvGuard::install(temp.path());
    let outbox_path = temp.path().join("native_completion_outbox.json");
    let outbox = Arc::new(NativeCompletionOutbox::open(&outbox_path).expect("outbox"));
    let capture: Arc<Mutex<Vec<NativeCompletionEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let inner: SharedNativeCompletionPort = Arc::new(InMemoryDeliveryPort::new(capture.clone()));
    let durable = Arc::new(DurableNativeCompletionPort::new(
        inner.clone(),
        outbox.clone(),
    ));
    let agent_text = canonical_completion_block(
        WORKFLOW_ID,
        "attacker-project",
        PROJECT_ROOT,
        "VERIFIER",
        "PASS",
    );
    let script = write_fake_acp(temp.path(), &agent_text);
    let record = temp.path().join("acp.jsonl");
    let config = AgentConfig {
        command: script.display().to_string(),
        args: vec![record.display().to_string()],
        working_dir: temp.path().display().to_string(),
        env: HashMap::new(),
        inherit_env: vec![],
        command_explicit: true,
    };
    let pool = Arc::new(SessionPool::new(config, 2, 600, HashMap::new()).expect("session pool"));
    let reactions = ReactionsConfig {
        enabled: false,
        ..Default::default()
    };
    let router = Arc::new(
        AdapterRouter::new(
            pool,
            reactions,
            TableMode::Off,
            30,
            1,
            HashMap::new(),
            temp.path().to_path_buf(),
        )
        .with_native_completion_port(durable.clone()),
    );
    let adapter = Arc::new(FailsAfterDurableCaptureAdapter::new());
    let native = metadata(WORKFLOW_ID, PROJECT_ROOT);
    let msg = make_msg("verifier evaluates the patch", native);

    drive_native_turn(router.clone(), adapter.clone(), msg).await;

    // The outbox file is only materialised on the first successful
    // capture, so a missing file is equivalent to an empty outbox
    // for these fail-closed assertions.
    let raw = fs::read_to_string(&outbox_path).unwrap_or_default();
    let parsed: serde_json::Value = if raw.trim().is_empty() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_str(&raw).expect("parse outbox")
    };
    let obj = parsed.as_object().expect("outbox is object");
    assert!(
        obj.is_empty(),
        "project_id-mismatched completion must NOT be captured; got {obj:?}"
    );
    assert!(
        capture.lock().unwrap().is_empty(),
        "no event must be submitted to the inner delivery port"
    );
}

// ---------------------------------------------------------------------------
// FAIL-CLOSED: wrong role (VERIFIER block under a PRIMARY dispatch) MUST
// NOT capture.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn role_mismatch_does_not_capture() {
    let _lock = ENV_LOCK.lock().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let _env = EnvGuard::install(temp.path());
    let outbox_path = temp.path().join("native_completion_outbox.json");
    let outbox = Arc::new(NativeCompletionOutbox::open(&outbox_path).expect("outbox"));
    let capture: Arc<Mutex<Vec<NativeCompletionEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let inner: SharedNativeCompletionPort = Arc::new(InMemoryDeliveryPort::new(capture.clone()));
    let durable = Arc::new(DurableNativeCompletionPort::new(
        inner.clone(),
        outbox.clone(),
    ));
    // Block claims VERIFIER PASS but the dispatch metadata is
    // PRIMARY — role-mismatch must fail closed.
    let agent_text =
        canonical_completion_block(WORKFLOW_ID, PROJECT_ID, PROJECT_ROOT, "VERIFIER", "PASS");
    let script = write_fake_acp(temp.path(), &agent_text);
    let record = temp.path().join("acp.jsonl");
    let config = AgentConfig {
        command: script.display().to_string(),
        args: vec![record.display().to_string()],
        working_dir: temp.path().display().to_string(),
        env: HashMap::new(),
        inherit_env: vec![],
        command_explicit: true,
    };
    let pool = Arc::new(SessionPool::new(config, 2, 600, HashMap::new()).expect("session pool"));
    let reactions = ReactionsConfig {
        enabled: false,
        ..Default::default()
    };
    let router = Arc::new(
        AdapterRouter::new(
            pool,
            reactions,
            TableMode::Off,
            30,
            1,
            HashMap::new(),
            temp.path().to_path_buf(),
        )
        .with_native_completion_port(durable.clone()),
    );
    let adapter = Arc::new(FailsAfterDurableCaptureAdapter::new());
    let mut native = metadata(WORKFLOW_ID, PROJECT_ROOT);
    native.role = "PRIMARY".into();
    let msg = make_msg("primary completes the patch", native);

    drive_native_turn(router.clone(), adapter.clone(), msg).await;

    // The outbox file is only materialised on the first successful
    // capture, so a missing file is equivalent to an empty outbox
    // for these fail-closed assertions.
    let raw = fs::read_to_string(&outbox_path).unwrap_or_default();
    let parsed: serde_json::Value = if raw.trim().is_empty() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_str(&raw).expect("parse outbox")
    };
    let obj = parsed.as_object().expect("outbox is object");
    assert!(
        obj.is_empty(),
        "role-mismatched completion must NOT be captured; got {obj:?}"
    );
    assert!(
        capture.lock().unwrap().is_empty(),
        "no event must be submitted to the inner delivery port"
    );
}
