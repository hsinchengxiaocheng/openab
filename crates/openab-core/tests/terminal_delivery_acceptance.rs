//! Hermetic ACP-to-durable-terminal-delivery acceptance coverage.
#![cfg(unix)]

use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use openab_core::acp::SessionPool;
use openab_core::adapter::{AdapterRouter, ChannelRef, ChatAdapter, MessageContext, MessageRef};
use openab_core::config::{AgentConfig, ReactionsConfig};
use openab_core::markdown::TableMode;
use openab_core::terminal_delivery::TerminalDeliveryRepository;
use openab_core::terminal_delivery_worker::{
    AcpTerminalMetadata, DiscordTerminalDeliverySender, LookupError, TerminalDeliveryPort,
    TerminalDeliveryWorker, TerminalResultLookup,
};
use rusqlite::Connection;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::sync::Mutex as AsyncMutex;

const RUNTIME_ID: &str = "RUNTIME-A";
const WORKFLOW_ID: &str = "WORKFLOW-B";
static ENV_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

struct EnvGuard {
    home: Option<std::ffi::OsString>,
    agent: Option<std::ffi::OsString>,
}
impl EnvGuard {
    fn install(root: &Path) -> Self {
        let home = std::env::var_os("HOME");
        let agent = std::env::var_os("ARTHUR_AGENT_NAME");
        std::env::set_var("HOME", root);
        std::env::set_var("ARTHUR_AGENT_NAME", "terminal-delivery-acceptance");
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

const FAKE_ACP: &str = r#"#!/bin/sh
record="$1"; : > "$record"
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$record"
  case "$line" in
    *'"method":"initialize"'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentInfo":{"name":"fake-acp"},"agentCapabilities":{"loadSession":false}}}' ;;
    *'"method":"session/new"'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"fake-session"}}' ;;
    *'"method":"session/prompt"'*)
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_thought_chunk"}}}'
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"tool_call","toolCallId":"tool-1","title":"Hermetic tool"}}}'
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk","content":{"text":"progress"}}}}'
      if [ "$FAKE_MODE" = "missing-runtime" ]; then
        printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","metadata":{"workflow_run_id":"WORKFLOW-B","conversation_id":"CONVERSATION-C"}}}'
      else
        printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn","metadata":{"runtime_run_id":"RUNTIME-A","run_id":"RUNTIME-A","workflow_run_id":"WORKFLOW-B","conversation_id":"CONVERSATION-C"}}}'
      fi ;;
  esac
done
"#;

fn write_fake_acp(temp: &TempDir) -> PathBuf {
    let script = temp.path().join("fake-acp.sh");
    fs::write(&script, FAKE_ACP).unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
    script
}
#[derive(Default)]
struct RecordingLookup {
    calls: Mutex<Vec<String>>,
}
#[async_trait]
impl TerminalResultLookup for RecordingLookup {
    async fn get_terminal_result(&self, id: &str) -> Result<Value, LookupError> {
        self.calls.lock().unwrap().push(id.into());
        Ok(
            json!({"terminal_status":"completed","terminal_result":{"final_answer":"durable final"}}),
        )
    }
}

struct FailingTerminalPort;

#[async_trait]
impl TerminalDeliveryPort for FailingTerminalPort {
    async fn capture_and_deliver(
        &self,
        _: AcpTerminalMetadata,
        _: ChannelRef,
        _: &str,
        _: &dyn DiscordTerminalDeliverySender,
    ) -> Result<openab_core::terminal_delivery::TerminalDeliveryRecordV1, LookupError> {
        Err(LookupError::Unavailable)
    }
}
#[derive(Default)]
struct Observations {
    sends: Vec<String>,
    edits: usize,
    statuses: Vec<String>,
    pre_send_durable: bool,
}
struct RecordingDiscord {
    observations: Arc<Mutex<Observations>>,
    database_path: PathBuf,
}
#[async_trait]
impl ChatAdapter for RecordingDiscord {
    fn platform(&self) -> &'static str {
        "discord"
    }
    fn message_limit(&self) -> usize {
        2000
    }
    async fn send_message(&self, channel: &ChannelRef, content: &str) -> Result<MessageRef> {
        let conn = Connection::open(&self.database_path)?;
        let row: (String, String, String, Option<String>, Option<String>) = conn.query_row("SELECT state, terminal_payload_digest, attempt_count, delivery_lease_token, delivery_started_at FROM terminal_deliveries", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
        let mut o = self.observations.lock().unwrap();
        o.pre_send_durable = row.0 == "DELIVERING"
            && !row.1.is_empty()
            && row.2 == "1"
            && row.3.is_some()
            && row.4.is_some();
        o.sends.push(content.into());
        Ok(MessageRef {
            channel: channel.clone(),
            message_id: "discord-message-1".into(),
        })
    }
    async fn create_thread(&self, c: &ChannelRef, _: &MessageRef, _: &str) -> Result<ChannelRef> {
        Ok(c.clone())
    }
    async fn add_reaction(&self, _: &MessageRef, _: &str) -> Result<()> {
        Ok(())
    }
    async fn remove_reaction(&self, _: &MessageRef, _: &str) -> Result<()> {
        Ok(())
    }
    async fn edit_message(&self, _: &MessageRef, _: &str) -> Result<()> {
        self.observations.lock().unwrap().edits += 1;
        Ok(())
    }
    fn use_streaming(&self, _: bool) -> bool {
        true
    }
    fn uses_assistant_status(&self) -> bool {
        true
    }
    async fn set_status(&self, _: &ChannelRef, status: &str) -> Result<()> {
        self.observations
            .lock()
            .unwrap()
            .statuses
            .push(status.into());
        Ok(())
    }
}
fn channel() -> ChannelRef {
    ChannelRef {
        platform: "discord".into(),
        channel_id: "1".into(),
        thread_id: None,
        parent_id: None,
        origin_event_id: None,
    }
}
fn context() -> MessageContext {
    let c = channel();
    MessageContext {
        thread_channel: c.clone(),
        sender_json: r#"{"sender_id":"user-1"}"#.into(),
        prompt: "exercise fake ACP".into(),
        extra_blocks: vec![],
        trigger_msg: MessageRef {
            channel: c,
            message_id: "inbound-1".into(),
        },
        other_bot_present: false,
    }
}
fn router_for(
    temp: &TempDir,
    mode: &str,
    db: Arc<TerminalDeliveryRepository>,
    lookup: Arc<RecordingLookup>,
) -> AdapterRouter {
    let script = write_fake_acp(temp);
    let record = temp.path().join(format!("{mode}.jsonl"));
    let mut env = HashMap::new();
    env.insert("FAKE_MODE".into(), mode.into());
    let config = AgentConfig {
        command: script.display().to_string(),
        args: vec![record.display().to_string()],
        working_dir: temp.path().display().to_string(),
        env,
        inherit_env: vec![],
        command_explicit: true,
    };
    let pool = Arc::new(SessionPool::new(config, 2, 600, HashMap::new()).unwrap());
    let worker = Arc::new(TerminalDeliveryWorker::new(db, lookup));
    let reactions = ReactionsConfig {
        enabled: false,
        ..Default::default()
    };
    AdapterRouter::new(
        pool,
        reactions,
        TableMode::Off,
        30,
        1,
        HashMap::new(),
        temp.path().to_path_buf(),
    )
    .with_terminal_delivery_worker(worker)
}

#[tokio::test]
async fn hermetic_acp_lifecycle_durable_authority_and_runtime_identity() {
    let _lock = ENV_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let _env = EnvGuard::install(temp.path());
    let db_path = temp.path().join("terminal.db");
    let repo = Arc::new(TerminalDeliveryRepository::open_path(&db_path).unwrap());
    let lookup = Arc::new(RecordingLookup::default());
    let observations = Arc::new(Mutex::new(Observations::default()));
    let adapter: Arc<dyn ChatAdapter> = Arc::new(RecordingDiscord {
        observations: observations.clone(),
        database_path: db_path.clone(),
    });
    let router = router_for(&temp, "success", repo, lookup.clone());
    router.handle_message(&adapter, context()).await.unwrap();
    let row: (String,String,Option<String>,String) = Connection::open(&db_path).unwrap().query_row("SELECT state,runtime_run_id,workflow_run_id,discord_message_id FROM terminal_deliveries", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
    assert_eq!(row.0, "DELIVERED");
    assert_eq!(row.1, RUNTIME_ID);
    assert_eq!(row.2.as_deref(), Some(WORKFLOW_ID));
    assert_eq!(row.3, "discord-message-1");
    assert_eq!(*lookup.calls.lock().unwrap(), vec![RUNTIME_ID]);
    {
        let seen = observations.lock().unwrap();
        assert!(seen.pre_send_durable);
        assert_eq!(seen.sends, vec!["durable final"]);
        assert_eq!(seen.edits, 0);
        assert!(seen.statuses.iter().any(|s| s == "Thinking…"));
    }
    let events: Vec<String> = Connection::open(&db_path)
        .unwrap()
        .prepare("SELECT safe_reason_code FROM terminal_delivery_events ORDER BY event_id")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        events,
        vec![
            "CREATED",
            "RESULT_MATERIALIZED",
            "DELIVERY_CLAIMED",
            "DISCORD_ACCEPTED_MESSAGE_ID_PERSISTED",
            "DISCORD_DELIVERED"
        ]
    );
    let missing_db =
        Arc::new(TerminalDeliveryRepository::open_path(temp.path().join("missing.db")).unwrap());
    let missing_lookup = Arc::new(RecordingLookup::default());
    let missing = router_for(&temp, "missing-runtime", missing_db, missing_lookup.clone());
    missing.handle_message(&adapter, context()).await.unwrap();
    assert!(missing_lookup.calls.lock().unwrap().is_empty());
    assert_eq!(observations.lock().unwrap().sends.len(), 1);
    assert!(fs::read_to_string(temp.path().join("success.jsonl"))
        .unwrap()
        .contains("session/prompt"));
}

#[tokio::test]
async fn durable_terminal_port_failure_has_no_legacy_discord_fallback() {
    let _lock = ENV_LOCK.lock().await;
    let temp = TempDir::new().unwrap();
    let _env = EnvGuard::install(temp.path());
    let observations = Arc::new(Mutex::new(Observations::default()));
    let adapter: Arc<dyn ChatAdapter> = Arc::new(RecordingDiscord {
        observations: observations.clone(),
        database_path: temp.path().join("unused.db"),
    });
    let script = write_fake_acp(&temp);
    let record = temp.path().join("failure.jsonl");
    let config = AgentConfig {
        command: script.display().to_string(),
        args: vec![record.display().to_string()],
        working_dir: temp.path().display().to_string(),
        env: HashMap::new(),
        inherit_env: vec![],
        command_explicit: true,
    };
    let reactions = ReactionsConfig {
        enabled: false,
        ..Default::default()
    };
    let router = AdapterRouter::new(
        Arc::new(SessionPool::new(config, 2, 600, HashMap::new()).unwrap()),
        reactions,
        TableMode::Off,
        30,
        1,
        HashMap::new(),
        temp.path().to_path_buf(),
    )
    .with_terminal_delivery_worker(Arc::new(FailingTerminalPort));
    router.handle_message(&adapter, context()).await.unwrap();
    let seen = observations.lock().unwrap();
    assert!(seen.sends.is_empty());
    assert_eq!(seen.edits, 0);
}
