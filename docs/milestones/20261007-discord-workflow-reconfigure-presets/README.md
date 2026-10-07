# Discord Workflow Reconfigure Presets — Production Acceptance Closure

Date: 2026-10-07  
Status: **PASS / CLOSED**

## Objective

Provide a bounded Discord Tech Lead topology-reconfiguration surface that
allows an eligible nonterminal AAP WorkflowRun to switch between supported
agent topologies through named presets without weakening Runtime mutation
authority.

The accepted production path is:

```text
Discord /workflow reconfigure
→ OpenAB native workflow command adapter
→ preset expansion
→ canonical Runtime topology options
→ Tech Lead mutation authority
→ Runtime topology intervention
→ revision CAS
→ topology commit
→ operator-hold migration
→ scheduler wake
→ new-role lease reconciliation
→ authoritative Discord status rendering
```

The accepted presets are:

```text
STANDARD_3_AGENT

ArthurClaude
→ ArthurCodex
→ ArthurGemini
```

and:

```text
DEGRADED_CODEX_GEMINI_2_AGENT

ArthurCodex
→ ArthurGemini
→ ArthurGemini
```

The degraded topology intentionally shares ArthurGemini across
VERIFIER and FINAL_REVIEWER roles.

## OpenAB implementation

Feature commit:

```text
7b534fca72e06308b58abf2089084ceddf8de60d
feat(discord): add workflow reconfigure presets
```

Parent production source commit:

```text
1c76303d260d6247574ec5759a50275d75392efa
feat(discord): add workflow reopen reason choices
```

OpenAB version:

```text
openab 0.10.0
```

Production release SHA-256:

```text
6c5c827004d76cf630d7d2572b15ca7d67485a37c5cb32c577250d0a3c037251
```

Production binary:

```text
/home/arthur/.local/bin/openab
```

Preserved rollback images:

```text
564854cf080da3ac9ad46c52696dbb83f61ae8ea48517490be4ffc442f8f6c5c
42b3a5147740175f28aab1cb727a4e616824e963b6cd506fcee582b6d04cd9e4
```

Rollback directory:

```text
/home/arthur/.local/lib/openab-rollbacks
```

Production services converged on the accepted release:

```text
openab-arthur.service
openab-claude.service
openab-codex.service
openab-gemini.service
```

All four services were verified running:

```text
6c5c827004d76cf630d7d2572b15ca7d67485a37c5cb32c577250d0a3c037251
```

Git source convergence completed with:

```text
local main  = 7b534fca72e06308b58abf2089084ceddf8de60d
origin/main = 7b534fca72e06308b58abf2089084ceddf8de60d
```

## Discord command contract

The production `/workflow reconfigure` surface exposes a preset option in
addition to the canonical manual agent fields.

Accepted preset choices:

```text
STANDARD_3_AGENT
DEGRADED_CODEX_GEMINI_2_AGENT
```

Preset expansion remains presentation-layer behavior in OpenAB.

Runtime continues to receive canonical topology fields.

Mixed preset/manual-agent input is fail-closed.

Read-only workflow commands were also revalidated in production.

The following commands successfully resolved the authoritative WorkflowRun
from the existing Discord binding without requiring an explicit
`workflow_run_id`:

```text
/workflow status
/workflow agents
```

## Test gates

Before production promotion, the feature commit passed:

```text
workflow command regression
27 passed / 0 failed
```

Focused contract coverage passed for:

```text
workflow command registration
preset expansion to canonical Runtime options
mixed preset/manual-agent rejection
```

Candidate production binary was built from:

```text
7b534fca72e06308b58abf2089084ceddf8de60d
```

Candidate SHA-256:

```text
6c5c827004d76cf630d7d2572b15ca7d67485a37c5cb32c577250d0a3c037251
```

Production CLI compatibility was verified before promotion:

```text
openab run --help
candidate == production
PASS
```

Rollback material was preserved before production replacement.

## Fail-closed production validation

Before the successful mutation, two rejection paths were intentionally
observed.

### Terminal WorkflowRun rejection

Workflow:

```text
wfrafc00805bb46a752
```

State:

```text
TECH_LEAD_WAIT
```

Runtime rejected topology reconfiguration because the WorkflowRun was
terminal.

The authoritative state and revision remained unchanged.

### Wrong operator-hold identity rejection

Workflow:

```text
wfre58ccc879eeded53
```

State:

```text
PRIMARY_ACTIVE
```

The workflow had no active execution or live lease, but its dispatch hold was:

```text
PERMANENT_FAILURE
MISSING_WORK_OBJECTIVE
operator_hold=1
```

Runtime rejected topology reconfiguration with:

```text
Active operator hold does not exactly match the bounded verifier defect identity.
```

No mutation occurred.

This confirmed that topology intervention authority cannot be borrowed from an
unrelated operator hold.

## Live production acceptance

Production acceptance used:

```text
workflow_run_id:
wfrfc44f206978ccd6f
```

Pre-mutation state:

```text
state:             VERIFIER_ACTIVE
revision:          6
defect_loop_count: 1

primary:           ArthurClaude
verifier:          ArthurCodex
final_reviewer:    ArthurGemini
```

Pre-mutation topology:

```text
ArthurClaude
→ ArthurCodex
→ ArthurGemini
```

The canonical bounded-defect hold was present:

```text
role:              VERIFIER
expected_revision: 6
last_status:       OPERATOR_HOLD
last_error_code:   BOUNDED_DEFECT_LOOP_EXHAUSTED
operator_hold:     1
```

No live execution existed before mutation:

```text
live leases:                  0
unfinished dispatch intents:  0
unfinished execution receipt: 0
unfinished OpenAB execution:  0
```

The Discord ConversationBinding remained active.

## Reconfigure mutation

The accepted production mutation used:

```text
workflow_run_id:
wfrfc44f206978ccd6f

expected_revision:
6

preset:
DEGRADED_CODEX_GEMINI_2_AGENT

reason:
TECH_LEAD_TOPOLOGY_RECONFIGURE
```

OpenAB expanded the preset into the canonical Runtime topology:

```text
primary:        ArthurCodex
verifier:       ArthurGemini
final_reviewer: ArthurGemini
```

Discord returned:

```text
Workflow reconfigure complete
revision: 6 → 7
scheduler woken: True
```

Intervention identity:

```text
wfi-fed97b6da08a4c35b6373f781e688d7a
```

## Intervention ledger

The authoritative intervention record was committed:

```text
state:
COMMITTED

expected_revision:
6

authoritative_pre_mutation_revision:
6
```

Previous topology:

```text
ArthurClaude
→ ArthurCodex
→ ArthurGemini
```

Proposed topology:

```text
ArthurCodex
→ ArthurGemini
→ ArthurGemini
```

Intervention reason:

```text
TECH_LEAD_TOPOLOGY_RECONFIGURE
```

Next role:

```text
VERIFIER
```

Operator-hold migration:

```text
1
```

The intervention recorded a non-null scheduler wake delivery time.

## Post-mutation WorkflowRun

Authoritative post-mutation state:

```text
workflow_run_id:   wfrfc44f206978ccd6f
state:             VERIFIER_ACTIVE
revision:          7
defect_loop_count: 1

primary:           ArthurCodex
verifier:          ArthurGemini
final_reviewer:    ArthurGemini
```

Post-mutation topology:

```text
ArthurCodex
→ ArthurGemini
→ ArthurGemini
```

Mode:

```text
2-agent (shared roles)
```

Shared role:

```text
ArthurGemini = VERIFIER + FINAL_REVIEWER
```

## Scheduler wake and lease safety

The topology intervention woke the scheduler against the new topology.

A new VERIFIER lease was created for:

```text
ArthurGemini
```

Lease generation:

```text
3
```

The lease was associated with:

```text
ufdi-wfi-fed97b6da08a4c35b6373f781e688d7a
```

The lease was then released.

No active lease remained.

The dispatch hold was migrated to revision 7:

```text
role:              VERIFIER
expected_revision: 7
last_status:       OPERATOR_HOLD
last_error_code:   BOUNDED_DEFECT_LOOP_EXHAUSTED
operator_hold:     1
```

No execution receipt was created.

No OpenAB execution record was created.

Therefore the scheduler wake exercised the new topology without producing an
unintended agent execution.

## Conversation binding

The authoritative Discord binding remained:

```text
binding_id:
wfb12ac763a0c352d69

transport:
DISCORD

conversation_key:
discord:1556455350985555988

status:
ACTIVE
```

The same binding was used for pre-mutation status, mutation, and post-mutation
status verification.

## Fleet convergence

After production acceptance, Git and the OpenAB service fleet were converged.

Final Git state:

```text
local main:
7b534fca72e06308b58abf2089084ceddf8de60d

origin/main:
7b534fca72e06308b58abf2089084ceddf8de60d
```

Final running image:

```text
openab-arthur.service
6c5c827004d76cf630d7d2572b15ca7d67485a37c5cb32c577250d0a3c037251

openab-claude.service
6c5c827004d76cf630d7d2572b15ca7d67485a37c5cb32c577250d0a3c037251

openab-codex.service
6c5c827004d76cf630d7d2572b15ca7d67485a37c5cb32c577250d0a3c037251

openab-gemini.service
6c5c827004d76cf630d7d2572b15ca7d67485a37c5cb32c577250d0a3c037251
```

All four services were active and running after convergence.

No restart loop was observed.

## Acceptance result

**PASS**

Production acceptance demonstrates that Discord can safely request a supported
workflow topology preset while preserving Runtime authority and lifecycle
safety.

Verified properties:

- preset expansion produces canonical Runtime topology fields;
- mixed preset/manual-agent mutation remains fail-closed;
- terminal WorkflowRuns cannot be reconfigured;
- unrelated operator holds cannot authorize reconfiguration;
- expected revision participates in Runtime mutation authority;
- the committed topology exactly matches the requested preset;
- shared VERIFIER / FINAL_REVIEWER topology is represented correctly;
- bounded-defect operator hold identity is preserved across the revision;
- scheduler wake targets the new verifier;
- no stale ArthurCodex verifier execution is dispatched;
- no unintended OpenAB execution occurs;
- no active lease remains after the held wake;
- Discord conversation binding continuity is preserved;
- production Git and all OpenAB services converge on the same accepted release.

Production acceptance result:

```text
SLICE_3C_PRODUCTION_ACCEPTANCE=PASS
PRESET_EXPANSION=PASS
REVISION_CAS=PASS
TOPOLOGY_MUTATION=PASS
SHARED_ROLE_TOPOLOGY=PASS
OPERATOR_HOLD_MIGRATION=PASS
SCHEDULER_WAKE_NEW_TOPOLOGY=PASS
NO_UNINTENDED_EXECUTION=PASS
NO_ACTIVE_LEASE_REMAINS=PASS
OPENAB_FLEET_CONVERGENCE=PASS
```

## Operational rule

Do not use `/workflow reconfigure` as a generic way to bypass arbitrary
operator holds.

Runtime topology intervention remains bound to the eligible workflow identity,
state, revision, hold identity, and Tech Lead authority.

Before reconfiguration, refresh authoritative state with:

```text
/workflow status
```

Use the current authoritative revision.

For preset mode, do not simultaneously supply manual agent fields.

After reconfiguration, verify the resulting topology and revision with:

```text
/workflow status
```

For production troubleshooting, authoritative Runtime state remains the source
of truth rather than immediate Discord UI timing alone.

<!-- SLICE3C_CLOSURE_COMPLETE -->
