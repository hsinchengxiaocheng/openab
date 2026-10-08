# OpenAB P1 Production Reconciliation

Date: 2026-10-08
Host: AMDPC

## 1. Closure Classification

Status: QUALIFIED — PENDING APPROVAL

This document records the evidence collected for OpenAB P1
production reconciliation.

Qualified status does not constitute proof of exact
source-to-binary provenance or complete production
end-to-end acceptance.

## 2. Source Identity

Repository: openab
Branch at verification: main

P1 commit:

71f35f14c22ef7422e3cba9384889ff060809854

Subject:

fix(pool): preserve anonymous workspace across session eviction

Related commits confirmed as ancestors of main:

- Native: 422a91f9
- Autonomous: edd8bb47

The P1 source working tree was clean during verification.

## 3. P1 Change Surface

Files changed:

- crates/openab-core/src/acp/pool.rs
- crates/openab-core/src/dispatch.rs
- crates/openab-core/tests/terminal_finalization_durability.rs

Change summary:

- 608 insertions
- 79 deletions

## 4. Production Binary Identity

Installed binary:

/home/arthur/.local/bin/openab

Release artifact:

/home/arthur/openab/source/target/release/openab

SHA256:

5c5296864c9c419e799772791583ce64e42c7c3979ed1c260fcd106dcef64c02

Verified:

- Installed binary equals release artifact.
- openab-arthur running executable equals installed binary.
- openab-claude running executable equals installed binary.
- openab-codex running executable equals installed binary.
- openab-gemini running executable equals installed binary.

All four daemon processes were active during verification.

These checks establish executable identity, not exact
Git source-to-binary provenance.

## 5. Build and Deployment Timeline

All timestamps are Asia/Taipei (+08:00).

P1 commit:
2026-10-08 09:11:43

Release artifact:
2026-10-08 09:22:20

Installed binary:
2026-10-08 09:29:43

Daemon startup:

- Claude: 09:29:44
- Codex: 09:29:44
- Gemini: 09:29:44
- Arthur: 09:35:11

The timeline is consistent with a build and installation
after the P1 commit.

Timeline consistency alone does not prove build provenance.

## 6. Functional Regression Evidence

### 6.1 SessionPool Unit Regression

Command:

cargo test --offline --locked -p openab-core \
  --lib acp::pool::tests:: -- --test-threads=1

Result:

- Passed: 82
- Failed: 0
- Ignored: 0

Coverage includes:

- Anonymous workspace preservation
- Session discard and directive rollback
- Hung session eviction
- Explicit workspace reset
- Pinned project binding preservation
- Concurrent session capacity
- Eviction and reservation atomicity
- Native dispatch session isolation

### 6.2 Terminal Finalization Integration

Command:

cargo test --offline --locked -p openab-core \
  --test terminal_finalization_durability \
  -- --test-threads=1

Result:

- Passed: 6
- Failed: 0
- Ignored: 0

Verified scenarios:

- Durable capture survives Discord send failure
- Malformed completion is rejected
- Project identity mismatch is rejected
- Real ACP path durable capture survives send failure
- Role mismatch is rejected
- Workflow identity mismatch is rejected

### 6.3 Combined Regression Result

Total passed: 88
Total failed: 0

These are focused regression results, not a complete
repository-wide or production end-to-end test suite.

## 7. Test Isolation

Tests were executed using an isolated HOME, XDG directories,
TMPDIR and CARGO_TARGET_DIR.

Test root:

/tmp/openab-p1-test-z0MIoavf

The existing Rust toolchain and Cargo cache were reused.

Environment isolation is not an operating-system sandbox.

No production daemon restart or binary deployment was
performed by the regression commands.

The source working tree remained clean after both test runs.

## 8. Cargo Build Evidence

Release fingerprint:

target/release/.fingerprint/openab-32f6a87fbf8d8403

Cargo feature set:

- agentcore
- config-s3
- default
- discord
- filestore
- pre-seed
- secrets-aws
- slack

The release artifact and fingerprint timestamps are
consistent with the recorded build timeline.

The executable did not expose an embedded P1 commit SHA.

A trusted build record binding the exact source commit
to the production executable has not been established.

EXACT_BINARY_PROVENANCE=UNVERIFIED

## 9. Production Safety

During the evidence collection and focused regression:

- No OpenAB deployment was performed.
- No OpenAB daemon restart was performed.
- No production configuration change was performed.
- No AAP Operator Hold was released.
- No AAP workflow scheduler resumption was authorized.

The AAP Atomic Lease Fencing issue remains independent
of this OpenAB P1 reconciliation.

AAP_BLOCKED_1=OPEN
AAP_OPERATOR_HOLD=MAINTAINED

## 10. Remaining Limitations

- Exact binary provenance is unverified.
- Complete end-to-end production behavior is not established
  by the focused tests.
- The available evidence does not establish that every
  repository-wide regression test has passed.
- No formal closure approval is recorded here.

## 11. Closure Decision

Recommended classification:

OPENAB_P1_CLOSURE_RECOMMENDATION=QUALIFIED

Formal state:

OPENAB_P1_FORMAL_CLOSURE=PENDING_APPROVAL

Approval requires explicit acceptance of the remaining
provenance and production end-to-end evidence limitations.

This record does not authorize deployment, restart,
workflow resumption, or removal of production safeguards.
