# Discord Tech Lead Workflow Reopen — Production Acceptance Closure

Date: 2026-09-19  
Status: **PASS / CLOSED**

## Objective

Provide a bounded Discord Tech Lead control path that can reopen reviewed work
from a terminal AAP WorkflowRun without mutating terminal history.

The accepted production path is:

```text
Discord canonical command
→ OpenAB exact-authority parser
→ Tech Lead sender authorization
→ dedicated workflow reopen client
→ AAP terminal reopen API
→ immutable predecessor
→ fresh successor WorkflowRun
→ fresh ConversationBinding
→ PRIMARY
→ VERIFIER
→ FINAL_REVIEWER
→ TECH_LEAD_WAIT
→ lease and binding cleanup
```

## OpenAB implementation

Feature commit:

```text
f220c95e feat(openab): add bounded Discord workflow reopen
```

OpenAB version:

```text
openab 0.10.0
```

Production release SHA-256:

```text
ea198268d789a5a2fd2476a0c9a34ce847a530d0a9fd07b677a34f7613dbd449
```

Rollback binary:

```text
/home/arthur/.local/bin/openab.backup.20260919T081311
```

Rollback binary SHA-256:

```text
f888044d599021b001ad7aeac5ffd1d78b5b73af02dcde7542307d9e15d44f1a
```

Production service:

```text
openab-arthur.service
ExecStart=/home/arthur/.local/bin/openab run
WorkingDirectory=/home/arthur/openab/arthur
```

Production activation completed at:

```text
2026-09-19 08:13:12 CST
```

Startup evidence confirmed:

```text
Phase 8.x: dedicated workflow reopen client wired into production AdapterRouter
aap_runtime_url=http://127.0.0.1:8000
credential_env=ARTHUR_RUNTIME_TECH_LEAD_KEY
```

## Canonical Discord authority

Accepted command shape:

```text
@Arthuraap
Canonical workflow: <workflow-run-id>
Canonical action: reopen-work
```

Production acceptance used:

```text
Canonical workflow: wfr4dfa5d1c7a2a97bd
Canonical action: reopen-work
```

Mutation authority is fail-closed.

The implementation and regression suite verify:

- exact `reopen-work` action is required;
- missing workflow identity does not mutate;
- unsupported exact action is consumed fail-closed;
- duplicate exact `Canonical action:` headers are invalid;
- a late exact `Canonical action:` after ordinary prose is invalid;
- case variants such as `canonical action:` do not acquire mutation authority;
- bot senders cannot acquire Tech Lead mutation authority;
- unauthorized human senders cannot mutate;
- `aap_universal_humans=true` does not grant Tech Lead mutation authority;
- workflow and action authority cannot be borrowed across co-batched messages;
- explicit reopen never falls through to ordinary autonomous ingress or ACP;
- failed reopen is consumed and does not fall through.

## Test gates

Before production deployment:

```text
cargo check --workspace --all-targets
PASS

cargo test -p openab-core --lib
1363 passed / 0 failed

terminal_delivery_acceptance
4 passed / 0 failed

phase8_workflow_reopen_action_tests
9 passed / 0 failed

phase8_reopen dispatch security
7 passed / 0 failed

universal-human mutation isolation
1 passed / 0 failed

workflow_reopen client
6 passed / 0 failed

git diff --check
PASS
```

Repository-wide `cargo fmt --all --check` was not used as the release gate because
the existing repository baseline contains unrelated formatting differences in
pre-existing ACP source and test files.

No repository-wide formatter mutation was applied.

## Live production acceptance

### Predecessor

```text
workflow_run_id:   wfr4dfa5d1c7a2a97bd
task_id:           task-55814a8b853b480e9676693518a1fda9
project_id:        arthur-ai-platform
state:             TECH_LEAD_WAIT
revision:          6
defect_loop_count: 0
```

The predecessor remained immutable throughout acceptance.

### Reopen authority / causation

```text
predecessor_run_id: wfr4dfa5d1c7a2a97bd
actor_client_id:    runtime_operator_tech_lead
reason:             TECH_LEAD_POST_REVIEW_REOPEN
successor_run_id:   wfr52c086a38d6e6a2e
```

AAP returned HTTP 200 for:

```text
POST /v1/workflows/wfr4dfa5d1c7a2a97bd/tech-lead/reopen-work
```

OpenAB recorded:

```text
workflow terminal work reopened;
ordinary ingress and ACP suppressed
```

### Successor

```text
workflow_run_id:   wfr52c086a38d6e6a2e
task_id:           task-55814a8b853b480e9676693518a1fda9
project_id:        arthur-ai-platform
initial state:     PRIMARY_ACTIVE
initial revision:  1
```

The successor completed the normal multi-agent lifecycle:

```text
PRIMARY
ArthurClaude
dispatch: oad-d080f365442c450494ccb3fd52fbf811
PASS

VERIFIER
ArthurCodex
dispatch: oad-1276ac2c90f44e5ca0f71ffa065e176d
PASS

FINAL_REVIEWER
ArthurGemini
dispatch: oad-358bbb75fc1a430b97b9daf492eadf18
PASS
```

Final successor state:

```text
state:             TECH_LEAD_WAIT
revision:          6
defect_loop_count: 0
```

### Conversation binding

Predecessor binding:

```text
wfb4115222ea167eec0
status=RELEASED
```

Successor binding:

```text
wfb425456294b3bc0e0
transport=DISCORD
conversation_key=discord:1550493075988815903
```

The successor reused the authoritative Discord destination while receiving a
fresh binding identity.

During execution the successor binding was ACTIVE.

On terminal transition it was released:

```text
terminal_binding_cleanup_released
released_count=1
```

Final status:

```text
RELEASED
```

### Agent leases

Successor leases:

```text
PRIMARY         ArthurClaude  RELEASED
VERIFIER        ArthurCodex   RELEASED
FINAL_REVIEWER  ArthurGemini  RELEASED
```

No successor lease remained live after terminal completion.

## Acceptance result

**PASS**

The production acceptance demonstrates that a Tech Lead can explicitly reopen
reviewed terminal work from Discord while preserving:

- terminal WorkflowRun immutability;
- explicit mutation authority;
- task and project identity;
- Discord routing continuity;
- single-run lifecycle safety;
- normal automatic PRIMARY → VERIFIER → FINAL_REVIEWER execution.

No legacy terminal run was mutated back to an active state.

No duplicate workflow execution was observed.

No ordinary autonomous-ingress or ACP fallback occurred for the explicit reopen
command.

## Operational rule

Do not resend the same reopen command based solely on lack of immediate Discord
UI feedback.

Always verify WorkflowRun state and causation first.

A successful reopen creates a fresh successor WorkflowRun; the predecessor
remains immutable terminal history.
