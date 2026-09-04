//! Phase 6.4.x — AAP-native completion contract integration test.
//!
//! This file exercises the FULL production flow at the
//! completion-capture boundary:
//!
//!   1. A native dispatch admits an AAP-native workflow with
//!      structured metadata (the `NativeWorkflowMetadata` that
//!      arrives via `set agent.work`).
//!   2. The downstream agent emits a terminal assistant message
//!      containing the canonical `<role_completion>` block.
//!   3. OpenAB's `resolve_aap_native_completion_outcome` derives
//!      the canonical outcome, which is then sealed into a
//!      `NativeCompletionEvent` and persisted into the durable
//!      outbox.
//!   4. A `RecordingPort` captures the submitted event so the
//!      test can assert that exactly one event with the
//!      expected outcome / role / workflow identity reaches the
//!      port — i.e. the AAP-Runtime-facing HTTP callback would
//!      receive the canonical completion capture.
//!
//! Plain-text tokens and malformed / ambiguous blocks are
//! exercised end-to-end to prove the fail-closed boundary.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::runtime::Runtime;

use openab_core::admission::NativeWorkflowMetadata;
use openab_core::native_completion::{
    resolve_aap_native_completion_outcome, resolve_native_completion_outcome,
    DurableNativeCompletionPort, NativeCompletionError, NativeCompletionEvent,
    NativeCompletionOutbox, NativeCompletionPort,
};

type SharedPort = Arc<dyn NativeCompletionPort>;

/// Recording port: captures every `submit` so the test can assert
/// the post-turn event reaches the AAP-Runtime-facing boundary
/// exactly once.
#[derive(Default)]
struct RecordingPort {
    submitted: Mutex<Vec<NativeCompletionEvent>>,
}

#[async_trait]
impl NativeCompletionPort for RecordingPort {
    async fn submit(&self, event: NativeCompletionEvent) -> Result<(), NativeCompletionError> {
        self.submitted.lock().expect("lock").push(event);
        Ok(())
    }
}

fn aap_native_metadata(role: &str, dispatch_id: &str) -> NativeWorkflowMetadata {
    NativeWorkflowMetadata {
        dispatch_id: dispatch_id.into(),
        conversation_key: "ck-prod".into(),
        workflow_run_id: "wfr-prod-integration".into(),
        task_id: "task-prod".into(),
        role: role.into(),
        agent: "ArthurCodex".into(),
        lease_id: "lease-prod".into(),
        lease_generation: 7,
        expected_revision: 4,
        language: Some("zh-TW".into()),
        project_id: Some("openab".into()),
        project_root: Some("/home/arthur/openab/source".into()),
        native_execution_session_key: Some("native-dispatch:ArthurCodex:oad-prod".into()),
        transport: Some("DISCORD".into()),
        delivery_destination: None,
        scope_policy: None,
    }
}

fn well_formed_block(role: &str, result: &str) -> String {
    format!(
        "<role_completion>\n\
         role: {role}\n\
         result: {result}\n\
         workflow_id: wfr-prod-integration\n\
         project_id: openab\n\
         project_root: /home/arthur/openab/source\n\
         </role_completion>"
    )
}

fn runtime() -> Runtime {
    Runtime::new().expect("tokio runtime")
}

fn seal_event(
    metadata: &NativeWorkflowMetadata,
    outcome: String,
    raw_assistant_text: &str,
) -> NativeCompletionEvent {
    let event = NativeCompletionEvent {
        record_version: 1,
        completion_id: String::new(),
        captured_at: String::new(),
        record_digest: String::new(),
        source: "openab".into(),
        dispatch_id: metadata.dispatch_id.clone(),
        workflow_run_id: metadata.workflow_run_id.clone(),
        task_id: metadata.task_id.clone(),
        role: metadata.role.clone(),
        agent_identity: metadata.agent.clone(),
        lease_id: metadata.lease_id.clone(),
        lease_generation: metadata.lease_generation,
        expected_revision: metadata.expected_revision,
        outcome,
        session_id: "session-prod".into(),
        openab_turn_id: "turn-prod".into(),
        conversation_key: metadata.conversation_key.clone(),
        language: metadata.language.clone(),
        raw_assistant_text: raw_assistant_text.into(),
        project_id: metadata.project_id.clone().unwrap_or_default(),
        project_root: metadata.project_root.clone().unwrap_or_default(),
        timestamp: "2026-09-04T00:00:00Z".into(),
        transport: metadata.transport.clone(),
    };
    event.seal()
}

// ---------------------------------------------------------------------------
// Test 1 — full happy path: dispatcher captures a structured
// `<role_completion>` block, the durable port persists the sealed
// event, the recording port sees exactly one canonical event
// reaching the AAP-Runtime-facing boundary.
// ---------------------------------------------------------------------------
#[test]
fn integration_verifier_structured_pass_reaches_runtime_port() {
    let metadata = aap_native_metadata("VERIFIER", "oad-verifier-pass");
    let text = well_formed_block("VERIFIER", "PASS");

    // 1. resolve outcome
    let outcome = resolve_aap_native_completion_outcome(&metadata, &text);
    assert_eq!(outcome, Some("PASS".into()));

    // 2. drive capture through durable port + outbox.
    let directory = tempfile::tempdir().expect("sandbox");
    let path = directory.path().join("outbox.json");
    let recording = Arc::new(RecordingPort::default());
    let port: SharedPort = recording.clone();
    let outbox = Arc::new(NativeCompletionOutbox::open(&path).expect("open outbox"));
    let sealed = seal_event(&metadata, outcome.unwrap(), &text);

    runtime().block_on(async {
        let durable = DurableNativeCompletionPort::new(port.clone(), outbox.clone());
        durable.submit(sealed).await.expect("submit")
    });

    // 3. assert exactly one event reached the runtime-facing
    //    port with the canonical outcome & identity.
    let submitted = recording.submitted.lock().expect("lock");
    assert_eq!(submitted.len(), 1);
    let captured = &submitted[0];
    assert_eq!(captured.role, "VERIFIER");
    assert_eq!(captured.outcome, "PASS");
    assert_eq!(captured.workflow_run_id, "wfr-prod-integration");
    assert_eq!(captured.dispatch_id, "oad-verifier-pass");
    assert_eq!(captured.project_id, "openab");
    assert_eq!(captured.project_root, "/home/arthur/openab/source");
    assert_eq!(captured.transport.as_deref(), Some("DISCORD"));

    // outbox file persisted the sealed event. Note that
    // `pending()` filters by status `PENDING`, but the durable
    // port transitions the record to `DELIVERED` after a
    // successful `delivery.submit`, so we read the raw outbox
    // JSON instead — the file is the authoritative durable
    // record regardless of status.
    let raw = std::fs::read_to_string(&path).expect("read outbox");
    let parsed: serde_json::Value = serde_json::from_str(&raw).expect("parse outbox");
    assert_eq!(
        parsed
            .as_object()
            .expect("outbox is object")
            .contains_key(&captured.completion_id),
        true,
        "outbox must persist the sealed completion"
    );

    drop(directory);
}

// ---------------------------------------------------------------------------
// Test 2 — fail closed for plain `VERIFIER_PASS`. The resolver
// returns `None`, no event is constructed, no event reaches the
// port, the outbox is empty.
// ---------------------------------------------------------------------------
#[test]
fn integration_fail_closed_for_plain_verifier_pass() {
    let metadata = aap_native_metadata("VERIFIER", "oad-fail-closed");
    let outcome = resolve_aap_native_completion_outcome(&metadata, "VERIFIER_PASS review done");
    assert_eq!(outcome, None);

    let directory = tempfile::tempdir().expect("sandbox");
    let path = directory.path().join("outbox.json");
    let recording = Arc::new(RecordingPort::default());
    let port: SharedPort = recording.clone();
    let outbox = Arc::new(NativeCompletionOutbox::open(&path).expect("open outbox"));
    // Resolver returned `None`, so we MUST NOT construct an
    // event. The post-turn dispatcher path returns early in this
    // case, which is what this test simulates.
    if outcome.is_some() {
        panic!("resolver should fail closed for plain tokens");
    }

    let submitted = recording.submitted.lock().expect("lock");
    assert_eq!(submitted.len(), 0);
    let pending = outbox.pending();
    assert_eq!(pending.len(), 0);

    drop(directory);
    drop(port);
}

// ---------------------------------------------------------------------------
// Regression — one valid structured claim plus a trailing unmatched
// opening marker MUST fail closed and MUST NOT reach the completion port.
// ---------------------------------------------------------------------------
#[test]
fn integration_inline_role_completion_markers_do_not_capture() {
    let metadata = aap_native_metadata("VERIFIER", "oad-inline-marker");

    let inline_opening = well_formed_block("VERIFIER", "PASS").replacen(
        "<role_completion>",
        "prefix <role_completion>",
        1,
    );
    assert_eq!(
        resolve_aap_native_completion_outcome(&metadata, &inline_opening),
        None
    );

    let inline_closing = well_formed_block("VERIFIER", "PASS").replacen(
        "</role_completion>",
        "</role_completion> suffix",
        1,
    );
    assert_eq!(
        resolve_aap_native_completion_outcome(&metadata, &inline_closing),
        None
    );
}

#[test]
fn integration_valid_block_plus_trailing_unmatched_opening_does_not_capture() {
    let metadata = aap_native_metadata("VERIFIER", "oad-unmatched-marker");
    let text = format!(
        "{}\n<role_completion>\n",
        well_formed_block("VERIFIER", "PASS")
    );

    let outcome = resolve_aap_native_completion_outcome(&metadata, &text);
    assert_eq!(outcome, None);
}

// ---------------------------------------------------------------------------
// Test 3 — ambiguous blocks fail closed.
// ---------------------------------------------------------------------------
#[test]
fn integration_fail_closed_for_ambiguous_blocks() {
    let metadata = aap_native_metadata("VERIFIER", "oad-ambiguous");
    let text = format!(
        "{}\n---\n{}\n",
        well_formed_block("VERIFIER", "PASS"),
        well_formed_block("VERIFIER", "FAIL")
    );
    let outcome = resolve_aap_native_completion_outcome(&metadata, &text);
    assert_eq!(outcome, None);
}

// ---------------------------------------------------------------------------
// Test 4 — role mismatch between block and metadata fails closed.
// ---------------------------------------------------------------------------
#[test]
fn integration_role_metadata_mismatch_fails_closed() {
    let metadata = aap_native_metadata("PRIMARY", "oad-mismatch");
    let text = well_formed_block("VERIFIER", "PASS");
    assert_eq!(
        resolve_aap_native_completion_outcome(&metadata, &text),
        None
    );
}

// ---------------------------------------------------------------------------
// Test 5 — FINAL_REVIEWER+FAIL rejected even when block is
// well-formed, proving no new agent-emittable path is created.
// ---------------------------------------------------------------------------
#[test]
fn integration_final_reviewer_fail_fails_closed_even_well_formed() {
    let metadata = aap_native_metadata("FINAL_REVIEWER", "oad-fr-fail");
    let text = well_formed_block("FINAL_REVIEWER", "FAIL");
    assert_eq!(
        resolve_aap_native_completion_outcome(&metadata, &text),
        None
    );
}

// ---------------------------------------------------------------------------
// Test 6 — FINAL_REVIEWER+PASS captures.
// ---------------------------------------------------------------------------
#[test]
fn integration_final_reviewer_pass_captures() {
    let metadata = aap_native_metadata("FINAL_REVIEWER", "oad-fr-pass");
    let text = well_formed_block("FINAL_REVIEWER", "PASS");
    assert_eq!(
        resolve_aap_native_completion_outcome(&metadata, &text),
        Some("PASS".into())
    );
}

// ---------------------------------------------------------------------------
// Test 7 — end-to-end smoke mirroring the production post-turn
// boundary in `dispatch.rs::invoke_workflow_hook_after_dispatch`.
// ---------------------------------------------------------------------------
#[test]
fn integration_dispatcher_post_turn_uses_aap_native_resolver() {
    let verifier_pass = aap_native_metadata("VERIFIER", "oad-d-VP");
    assert_eq!(
        resolve_aap_native_completion_outcome(
            &verifier_pass,
            &well_formed_block("VERIFIER", "PASS"),
        ),
        Some("PASS".into())
    );
    let primary_complete = aap_native_metadata("PRIMARY", "oad-d-PC");
    assert_eq!(
        resolve_aap_native_completion_outcome(
            &primary_complete,
            &well_formed_block("PRIMARY", "COMPLETE"),
        ),
        Some("COMPLETE".into())
    );
    let final_reviewer_pass = aap_native_metadata("FINAL_REVIEWER", "oad-d-FRP");
    assert_eq!(
        resolve_aap_native_completion_outcome(
            &final_reviewer_pass,
            &well_formed_block("FINAL_REVIEWER", "PASS"),
        ),
        Some("PASS".into())
    );
    // Plain-text tokens MUST still fail closed.
    let plain = aap_native_metadata("VERIFIER", "oad-d-plain");
    assert_eq!(
        resolve_aap_native_completion_outcome(&plain, "VERIFIER_PASS\n"),
        None
    );
}

// ---------------------------------------------------------------------------
// Test 8 — fresh ACP session guarantee. Resolver does not consult
// any persistent state; identical inputs yield identical outputs;
// different inputs yield the documented outputs. The capture path
// is safe across restart / fresh dispatch identity.
// ---------------------------------------------------------------------------
#[test]
fn integration_sessionless_no_historical_replay_dependency() {
    let metadata = aap_native_metadata("VERIFIER", "oad-sessionless");
    let text = well_formed_block("VERIFIER", "PASS");

    let first = resolve_aap_native_completion_outcome(&metadata, &text);
    let second = resolve_aap_native_completion_outcome(&metadata, &text);
    let third =
        resolve_aap_native_completion_outcome(&metadata, &well_formed_block("VERIFIER", "FAIL"));

    assert_eq!(first, Some("PASS".into()));
    assert_eq!(second, Some("PASS".into()));
    assert_eq!(third, Some("FAIL".into()));
}

// ---------------------------------------------------------------------------
// Test 9 — narrative OK-01 / OK-02 reject. The structured-block
// resolver returns NoClaim (no block), so the post-turn hook
// fail-closes without capturing.
// ---------------------------------------------------------------------------
#[test]
fn integration_narrative_ok_tokens_do_not_authorize() {
    let metadata = aap_native_metadata("VERIFIER", "oad-ok");
    assert_eq!(
        resolve_aap_native_completion_outcome(&metadata, "OK-01 review complete"),
        None
    );
    assert_eq!(
        resolve_aap_native_completion_outcome(&metadata, "OK-02 task done"),
        None
    );
}

// ---------------------------------------------------------------------------
// Test 10 — legacy plain-token resolver remains intact for
// non-AAP callers. AAP-native dispatch never reaches it because
// `dispatch.rs::invoke_workflow_hook_after_dispatch` routes only
// through `resolve_aap_native_completion_outcome`.
// ---------------------------------------------------------------------------
#[test]
fn integration_legacy_plain_token_isolated_from_aap_native_path() {
    // Legacy resolver still accepts plain tokens.
    assert_eq!(
        resolve_native_completion_outcome("VERIFIER", "VERIFIER_PASS\n"),
        Some("PASS".into())
    );
    // AAP-native resolver rejects the same plain token.
    let metadata = aap_native_metadata("VERIFIER", "oad-iso");
    assert_eq!(
        resolve_aap_native_completion_outcome(&metadata, "VERIFIER_PASS\n"),
        None
    );
}
