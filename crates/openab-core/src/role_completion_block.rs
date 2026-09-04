//! Parser for the AAP-canonical `<role_completion>…</role_completion>`
//! structured completion block. This is the trust boundary between the
//! untrusted agent terminal text and OpenAB's authoritative native
//! completion capture.
//!
//! # Trust model
//!
//! The agent may emit arbitrary text. Plain-text forms such as
//! `VERIFIER_PASS`, `OK-01`, `HANDOFF`, `@ArthurGemini`, or
//! `VERIFIER_FAIL` are **not** AAP-native canonical completion
//! claims. Only an exact `<role_completion>…</role_completion>`
//! block counts at the AAP-native seam.
//!
//! A successful parse is still UNTRUSTED for full workflow
//! authority (workflow identity, revision, state machine,
//! recipient routing) — AAP Runtime performs its own fail-closed
//! validation against the canonical Task snapshot. This parser
//! only enforces the minimum transport/native-dispatch boundary
//! checks: required fields present, role canonical, role-result
//! consistent with the dispatch metadata, identity tuple
//! plausibility (workflow_id/project_id/project_root present
//! and equal to dispatch metadata when metadata is available).
//!
//! # Multiple blocks
//!
//! Like the legacy prose parser, this parser rejects any turn
//! that contains more than one `<role_completion>` block as
//! [`RoleCompletionParse::Ambiguous`]. The whole turn is
//! rejected so OpenAB never has to guess which claim the agent
//! intended.
//!
//! # Fenced output
//!
//! The parser accepts both raw blocks and blocks wrapped in a
//! triple-backtick `text` fenced region. The fence is irrelevant
//! to parsing — the inner `<role_completion>` markers are what
//! matter.

use std::collections::HashMap;
use std::sync::OnceLock;

use regex::Regex;

use crate::admission::NativeWorkflowMetadata;

/// Opening marker for a completion block.
const OPENING_MARKER: &str = "<role_completion>";

/// Closing marker for a completion block.
const CLOSING_MARKER: &str = "</role_completion>";

/// Fields that MUST be present in every well-formed block.
///
/// This set is also the CLOSED set of fields the agent is allowed
/// to author. Any other key (including trusted-system keys such as
/// `transition_id`, `workflow_revision`, `next_role`, `next_stage`,
/// or `target_user_id`) is rejected as an unknown field — those
/// fields belong to OpenAB's trusted native-admission metadata
/// (and ultimately AAP Runtime), not to the agent-emittable block.
const REQUIRED_FIELDS: &[&str] = &[
    "role",
    "result",
    "workflow_id",
    "project_id",
    "project_root",
];

/// Regex matching one `<role_completion>…</role_completion>` block
/// (non-greedy). Used with `captures_iter` so we can count every
/// block in the input rather than only the first one.
///
/// The markers are sourced from [`OPENING_MARKER`] and
/// [`CLOSING_MARKER`] so the regex and the constants cannot drift.
fn block_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        // (?s) enables dot-matches-newline. (?m) is not needed
        // because we accept the whole span and then split on
        // newlines for field parsing.
        let pattern = format!(r"(?s){}(.*?){}", OPENING_MARKER, CLOSING_MARKER);
        Regex::new(&pattern).unwrap()
    })
}

/// One well-formed completion claim parsed from a single block.
///
/// Every field is UNTRUSTED until the AAP-native caller verifies
/// it against the dispatch metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleCompletionClaim {
    pub role: String,
    pub result: String,
    pub workflow_id: String,
    pub project_id: String,
    pub project_root: String,
}

/// What `parse_role_completion_block` actually returns.
///
/// The AAP-native caller only proceeds with authority when the
/// outcome is [`RoleCompletionParse::Claim`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleCompletionParse {
    /// No `<role_completion>` block was present. Plain text like
    /// `VERIFIER_PASS` / `OK-01` / `HANDOFF` lands here.
    NoClaim,

    /// Exactly one well-formed block was present. Trust is still
    /// untrusted — the caller MUST verify role-result and identity
    /// against the dispatch metadata before treating it as
    /// canonical.
    Claim(RoleCompletionClaim),

    /// More than one `<role_completion>` block was present. The
    /// capture is rejected as ambiguous so OpenAB never has to
    /// guess which claim the agent intended.
    Ambiguous,

    /// A single block was found but it was malformed (missing
    /// required field, forbidden field, malformed line, unknown
    /// field, etc.). The `reason` is a stable diagnostic token
    /// suitable for audit logging.
    Malformed { reason: String },
}

/// Parse an assistant reply for AAP-canonical `<role_completion>`
/// blocks.
///
/// Behaviour:
/// - **Zero** blocks → [`RoleCompletionParse::NoClaim`].
/// - **Exactly one** well-formed block → [`RoleCompletionParse::Claim`].
/// - **Exactly one** malformed block → [`RoleCompletionParse::Malformed`]
///   with the first diagnostic reason.
/// - **More than one** block (regardless of well-formed-ness) →
///   [`RoleCompletionParse::Ambiguous`]. The whole turn is rejected.
///
/// Plain text outside the markers is ignored. Narrative phrases
/// like `OK-01`, `OK-02`, `VERIFIER_PASS`, `HANDOFF`, `@ArthurGemini`,
/// etc. are NOT recognised as completion claims at this boundary.
pub fn parse_role_completion_block(text: &str) -> RoleCompletionParse {
    let opening_count = text.matches(OPENING_MARKER).count();
    let closing_count = text.matches(CLOSING_MARKER).count();
    let opening_line_count = text
        .lines()
        .filter(|line| line.trim() == OPENING_MARKER)
        .count();
    let closing_line_count = text
        .lines()
        .filter(|line| line.trim() == CLOSING_MARKER)
        .count();

    // No marker substring anywhere means ordinary narrative text:
    // there is no completion claim.
    if opening_count == 0 && closing_count == 0 {
        return RoleCompletionParse::NoClaim;
    }

    // Canonical authority markers MUST each occur exactly once and
    // MUST occupy their own line (leading/trailing whitespace on that
    // line is allowed). Inline markers such as
    // `prefix <role_completion>` or `</role_completion> suffix`
    // fail closed. This keeps the Rust seam aligned with Runtime's
    // line-oriented canonical parser.
    if opening_count != 1
        || closing_count != 1
        || opening_line_count != 1
        || closing_line_count != 1
    {
        if opening_count > 1
            && closing_count > 1
            && opening_count == closing_count
            && opening_line_count == opening_count
            && closing_line_count == closing_count
        {
            return RoleCompletionParse::Ambiguous;
        }

        return RoleCompletionParse::Malformed {
            reason: format!(
                "invalid role_completion marker structure: opening={opening_count} closing={closing_count} opening_lines={opening_line_count} closing_lines={closing_line_count}"
            ),
        };
    }

    let captures: Vec<&str> = block_re()
        .captures_iter(text)
        .map(|c| c.get(1).unwrap().as_str())
        .collect();

    match captures.len() {
        1 => parse_single_block(captures[0]),
        _ => RoleCompletionParse::Malformed {
            reason: "role_completion markers could not form one closed block".to_string(),
        },
    }
}

fn parse_single_block(body: &str) -> RoleCompletionParse {
    let fields = match parse_block_fields(body) {
        Ok(f) => f,
        Err(reason) => return RoleCompletionParse::Malformed { reason },
    };

    // Required fields: every one must be present and non-empty.
    for &f in REQUIRED_FIELDS {
        match fields.get(f) {
            Some(v) if !v.is_empty() => {}
            Some(_) => {
                return RoleCompletionParse::Malformed {
                    reason: format!("required field {f:?} is empty"),
                };
            }
            None => {
                return RoleCompletionParse::Malformed {
                    reason: format!("missing required field {f:?}"),
                };
            }
        }
    }

    // Role / result must be one of the canonical enum spellings
    // (PRIMARY / VERIFIER / FINAL_REVIEWER for role;
    // COMPLETE / PASS / FAIL for result). Anything else is
    // fail-closed.
    let role_str = fields.get("role").unwrap();
    if !is_canonical_role(role_str) {
        return RoleCompletionParse::Malformed {
            reason: format!("invalid role {role_str:?}"),
        };
    }
    let result_str = fields.get("result").unwrap();
    if !is_canonical_result(result_str) {
        return RoleCompletionParse::Malformed {
            reason: format!("invalid result {result_str:?}"),
        };
    }

    RoleCompletionParse::Claim(RoleCompletionClaim {
        role: role_str.to_string(),
        result: result_str.to_string(),
        workflow_id: fields.get("workflow_id").unwrap().clone(),
        project_id: fields.get("project_id").unwrap().clone(),
        project_root: fields.get("project_root").unwrap().clone(),
    })
}

/// Parse `key: value` lines into a `HashMap`. Empty /
/// whitespace-only lines are skipped. Duplicate keys fail
/// closed. A line without a `:` separator fails closed.
/// Parse `key: value` lines into a `HashMap`. Empty /
/// whitespace-only lines are skipped. Duplicate keys fail
/// closed. A line without a `:` separator fails closed.
///
/// IMPORTANT: this function ALSO fails closed on any key that is
/// not in [`REQUIRED_FIELDS`]. Even if the agent supplies extra
/// unknown keys with valid syntax, the block is malformed —
/// AAP-native authority is restricted to the five canonical
/// fields and the agent must not author anything else.
///
/// The whitespace check happens BEFORE the key is trimmed, so a
/// line like `role : VERIFIER` (with embedded whitespace in the
/// key) fails closed. The agent must write the keys exactly.
fn parse_block_fields(body: &str) -> Result<HashMap<String, String>, String> {
    let mut fields: HashMap<String, String> = HashMap::new();
    for raw_line in body.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let (raw_key, value) = line
            .split_once(':')
            .ok_or_else(|| format!("invalid line {line:?}"))?;
        if raw_key.chars().any(char::is_whitespace) {
            return Err(format!("whitespace in key {raw_key:?}"));
        }
        let key = raw_key.trim();
        let value = value.trim();
        if key.is_empty() {
            return Err(format!("empty key in line {raw_line:?}"));
        }
        // Reject any field outside the canonical set. The agent
        // is forbidden from authoring arbitrary additional keys
        // even with valid syntax — OpenAB's native-admission
        // metadata (and ultimately AAP Runtime) owns the rest of
        // the workflow routing / revision / transition identity.
        if !REQUIRED_FIELDS.contains(&key) {
            return Err(format!("unknown field {key:?}"));
        }
        if fields.insert(key.to_string(), value.to_string()).is_some() {
            return Err(format!("duplicate key {key:?}"));
        }
    }
    Ok(fields)
}

/// Recognise the canonical AAP-native role spellings.
fn is_canonical_role(role: &str) -> bool {
    matches!(role, "PRIMARY" | "VERIFIER" | "FINAL_REVIEWER")
}

/// Recognise the canonical AAP-native result spellings.
fn is_canonical_result(result: &str) -> bool {
    matches!(result, "COMPLETE" | "PASS" | "FAIL")
}

/// Verify role vs. metadata, role-result consistency per the AAP
/// agent-facing rules table, and identity-tuple continuity
/// against the dispatch metadata.
///
/// This is a boundary check — AAP Runtime still re-validates the
/// full workflow identity / revision / state authority against
/// the canonical Task snapshot. OpenAB only enforces the
/// minimum transport/native-dispatch seam checks: a downstream
/// agent cannot author a claim for the wrong workflow identity
/// or attach an impossible role-result combination.
///
/// Returns `None` for any violation so the caller (`dispatch.rs`
/// post-turn hook) leaves the turn uncaptured — the existing
/// `"native terminal turn has no unambiguous canonical role
/// outcome"` warning fires and the workflow loops remain
/// unchanged (fail-closed).
pub fn check_aap_native_claim_against_metadata(
    claim: &RoleCompletionClaim,
    metadata: &NativeWorkflowMetadata,
) -> Result<(), String> {
    // 1. Role match: the structured block's `role` field MUST
    //    equal the dispatch metadata's assigned role. A
    //    downstream agent cannot claim authority on behalf of a
    //    different role.
    if claim.role != metadata.role {
        return Err(format!(
            "role mismatch: block claims {:?} but dispatch is {:?}",
            claim.role, metadata.role
        ));
    }

    // 2. Role-result consistency: the AAP agent-facing rules
    //    table. FINAL_REVIEWER+FAIL is rejected even from a
    //    well-formed block on the AAP-native path — the agent
    //    MUST NOT have an emission path that combines these
    //    tokens. The legacy prose validator vocabulary may
    //    still accept FINAL_REVIEWER+FAIL for backwards
    //    compatibility, but the AAP-native contract projection
    //    advertises PASS only.
    let role_result_ok = matches!(
        (claim.role.as_str(), claim.result.as_str()),
        ("PRIMARY", "COMPLETE")
            | ("VERIFIER", "PASS")
            | ("VERIFIER", "FAIL")
            | ("FINAL_REVIEWER", "PASS")
    );
    if !role_result_ok {
        return Err(format!(
            "role-result combination not allowed for AAP-native completion: {}+{}",
            claim.role, claim.result
        ));
    }

    // 3. Identity-tuple continuity: the block's identity fields
    //    must equal the dispatch metadata. An agent cannot attach
    //    authority to a different workflow / project / workspace
    //    even if it claims the correct role.
    if claim.workflow_id != metadata.workflow_run_id {
        return Err(format!(
            "workflow_id mismatch: block claims {:?} but dispatch is {:?}",
            claim.workflow_id, metadata.workflow_run_id
        ));
    }
    // project_id / project_root are mandatory trusted identity
    // for the AAP-native path. Legacy/non-AAP compatibility must
    // remain outside this resolver. Missing or blank trusted
    // metadata fails closed before any agent-authored value can
    // acquire completion authority.
    let metadata_project_id = metadata
        .project_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "trusted project_id missing or blank".to_string())?;

    if claim.project_id != metadata_project_id {
        return Err(format!(
            "project_id mismatch: block claims {:?} but dispatch is {:?}",
            claim.project_id, metadata_project_id
        ));
    }

    let metadata_project_root = metadata
        .project_root
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "trusted project_root missing or blank".to_string())?;

    if claim.project_root != metadata_project_root {
        return Err(format!(
            "project_root mismatch: block claims {:?} but dispatch is {:?}",
            claim.project_root, metadata_project_root
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_metadata() -> NativeWorkflowMetadata {
        NativeWorkflowMetadata {
            dispatch_id: "oad-test".into(),
            conversation_key: "ck-test".into(),
            workflow_run_id: "wf-test".into(),
            task_id: "task-test".into(),
            role: "VERIFIER".into(),
            agent: "ArthurCodex".into(),
            lease_id: "lease-test".into(),
            lease_generation: 1,
            expected_revision: 1,
            language: Some("zh-TW".into()),
            project_id: Some("proj-test".into()),
            project_root: Some("/home/arthur/openab/source".into()),
            native_execution_session_key: None,
            transport: None,
            delivery_destination: None,
            scope_policy: None,
        }
    }

    #[test]
    fn parse_well_formed_verifier_pass_block() {
        let text = "<role_completion>\nrole: VERIFIER\nresult: PASS\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /home/arthur/openab/source\n</role_completion>";
        match parse_role_completion_block(text) {
            RoleCompletionParse::Claim(c) => {
                assert_eq!(c.role, "VERIFIER");
                assert_eq!(c.result, "PASS");
                assert_eq!(c.workflow_id, "wf-test");
            }
            other => panic!("expected Claim, got {other:?}"),
        }
    }

    #[test]
    fn parse_well_formed_fenced_block() {
        let text = "Here is my verdict.\n\n```text\n<role_completion>\nrole: VERIFIER\nresult: PASS\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /home/arthur/openab/source\n</role_completion>\n```\n";
        match parse_role_completion_block(text) {
            RoleCompletionParse::Claim(c) => {
                assert_eq!(c.role, "VERIFIER");
                assert_eq!(c.result, "PASS");
            }
            other => panic!("expected Claim, got {other:?}"),
        }
    }

    #[test]
    fn plain_text_ok_01_is_not_a_claim() {
        assert_eq!(
            parse_role_completion_block("OK-01 review complete"),
            RoleCompletionParse::NoClaim
        );
    }

    #[test]
    fn plain_text_ok_02_is_not_a_claim() {
        assert_eq!(
            parse_role_completion_block("OK-02 task done"),
            RoleCompletionParse::NoClaim
        );
    }

    #[test]
    fn plain_text_verifier_pass_is_not_a_claim() {
        // AAP-native path does not recognise plain-text tokens.
        assert_eq!(
            parse_role_completion_block("VERIFIER_PASS review complete"),
            RoleCompletionParse::NoClaim
        );
    }

    #[test]
    fn plain_text_final_reviewer_pass_is_not_a_claim() {
        assert_eq!(
            parse_role_completion_block("FINAL_REVIEWER_PASS look good"),
            RoleCompletionParse::NoClaim
        );
    }

    #[test]
    fn plain_text_handoff_is_not_a_claim() {
        assert_eq!(
            parse_role_completion_block("HANDOFF COMPLETE — review done"),
            RoleCompletionParse::NoClaim
        );
    }

    #[test]
    fn at_mention_is_not_a_claim() {
        assert_eq!(
            parse_role_completion_block("Please @ArthurGemini review this"),
            RoleCompletionParse::NoClaim
        );
    }

    #[test]
    fn missing_required_role_field_is_malformed() {
        let text = "<role_completion>\nresult: PASS\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\n</role_completion>";
        match parse_role_completion_block(text) {
            RoleCompletionParse::Malformed { reason } => {
                assert!(reason.contains("role"), "reason={reason}");
            }
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn missing_required_result_field_is_malformed() {
        let text = "<role_completion>\nrole: VERIFIER\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\n</role_completion>";
        match parse_role_completion_block(text) {
            RoleCompletionParse::Malformed { reason } => {
                assert!(reason.contains("result"), "reason={reason}");
            }
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn empty_required_field_is_malformed() {
        let text = "<role_completion>\nrole: VERIFIER\nresult: \nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\n</role_completion>";
        match parse_role_completion_block(text) {
            RoleCompletionParse::Malformed { reason } => {
                assert!(reason.contains("empty"), "reason={reason}");
            }
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn forbidden_field_fails_closed() {
        let text = "<role_completion>\nrole: VERIFIER\nresult: PASS\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\ntransition_id: 0\n</role_completion>";
        match parse_role_completion_block(text) {
            RoleCompletionParse::Malformed { reason } => {
                assert!(
                    reason.contains("transition_id") || reason.contains("forbidden"),
                    "reason={reason}"
                );
            }
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn unknown_field_fails_closed() {
        let text = "<role_completion>\nrole: VERIFIER\nresult: PASS\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\nnovel_field: surprising\n</role_completion>";
        match parse_role_completion_block(text) {
            RoleCompletionParse::Malformed { reason } => {
                assert!(reason.contains("novel_field"), "reason={reason}");
            }
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn ambiguous_multiple_blocks_rejected() {
        let text = "<role_completion>\nrole: VERIFIER\nresult: PASS\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\n</role_completion>\nthen second\n<role_completion>\nrole: VERIFIER\nresult: FAIL\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\n</role_completion>";
        assert_eq!(
            parse_role_completion_block(text),
            RoleCompletionParse::Ambiguous
        );
    }

    #[test]
    fn inline_opening_marker_is_malformed() {
        let text = "prefix <role_completion>\nrole: VERIFIER\nresult: PASS\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\n</role_completion>";
        assert!(matches!(
            parse_role_completion_block(text),
            RoleCompletionParse::Malformed { .. }
        ));
    }

    #[test]
    fn inline_closing_marker_is_malformed() {
        let text = "<role_completion>\nrole: VERIFIER\nresult: PASS\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\n</role_completion> suffix";
        assert!(matches!(
            parse_role_completion_block(text),
            RoleCompletionParse::Malformed { .. }
        ));
    }

    #[test]
    fn marker_lines_allow_surrounding_whitespace() {
        let text = "  <role_completion>  \nrole: VERIFIER\nresult: PASS\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\n  </role_completion>  ";
        assert!(matches!(
            parse_role_completion_block(text),
            RoleCompletionParse::Claim(_)
        ));
    }

    #[test]
    fn unmatched_opening_marker_is_malformed() {
        let text = "<role_completion>\nrole: VERIFIER\nresult: PASS";
        assert!(matches!(
            parse_role_completion_block(text),
            RoleCompletionParse::Malformed { .. }
        ));
    }

    #[test]
    fn unmatched_closing_marker_is_malformed() {
        let text = "narrative\n</role_completion>";
        assert!(matches!(
            parse_role_completion_block(text),
            RoleCompletionParse::Malformed { .. }
        ));
    }

    #[test]
    fn valid_block_plus_trailing_unmatched_opening_is_malformed() {
        let text = "<role_completion>\nrole: VERIFIER\nresult: PASS\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\n</role_completion>\n<role_completion>";
        assert!(matches!(
            parse_role_completion_block(text),
            RoleCompletionParse::Malformed { .. }
        ));
    }

    #[test]
    fn valid_block_plus_trailing_unmatched_closing_is_malformed() {
        let text = "<role_completion>\nrole: VERIFIER\nresult: PASS\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\n</role_completion>\n</role_completion>";
        assert!(matches!(
            parse_role_completion_block(text),
            RoleCompletionParse::Malformed { .. }
        ));
    }

    #[test]
    fn two_openings_one_closing_is_malformed() {
        let text = "<role_completion>\n<role_completion>\nrole: VERIFIER\nresult: PASS\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\n</role_completion>";
        assert!(matches!(
            parse_role_completion_block(text),
            RoleCompletionParse::Malformed { .. }
        ));
    }

    #[test]
    fn one_opening_two_closings_is_malformed() {
        let text = "<role_completion>\nrole: VERIFIER\nresult: PASS\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\n</role_completion>\n</role_completion>";
        assert!(matches!(
            parse_role_completion_block(text),
            RoleCompletionParse::Malformed { .. }
        ));
    }

    #[test]
    fn invalid_role_value_is_malformed() {
        let text = "<role_completion>\nrole: NOT_A_ROLE\nresult: PASS\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\n</role_completion>";
        match parse_role_completion_block(text) {
            RoleCompletionParse::Malformed { reason } => {
                assert!(
                    reason.contains("role") || reason.contains("invalid"),
                    "reason={reason}"
                );
            }
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn invalid_result_value_is_malformed() {
        let text = "<role_completion>\nrole: VERIFIER\nresult: MAYBE\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\n</role_completion>";
        match parse_role_completion_block(text) {
            RoleCompletionParse::Malformed { reason } => {
                assert!(
                    reason.contains("result") || reason.contains("invalid"),
                    "reason={reason}"
                );
            }
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn keys_with_whitespace_or_invalid_format_fail_closed() {
        let text = "<role_completion>\nrole : VERIFIER\nresult: PASS\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\n</role_completion>";
        match parse_role_completion_block(text) {
            RoleCompletionParse::Malformed { reason } => {
                assert!(
                    reason.contains("whitespace") || reason.contains("role"),
                    "reason={reason}"
                );
            }
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_key_is_malformed() {
        let text = "<role_completion>\nrole: VERIFIER\nrole: PRIMARY\nresult: PASS\nworkflow_id: wf-test\nproject_id: proj-test\nproject_root: /tmp\n</role_completion>";
        match parse_role_completion_block(text) {
            RoleCompletionParse::Malformed { reason } => {
                assert!(
                    reason.contains("duplicate") || reason.contains("role"),
                    "reason={reason}"
                );
            }
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn empty_string_is_no_claim() {
        assert_eq!(
            parse_role_completion_block(""),
            RoleCompletionParse::NoClaim
        );
    }

    #[test]
    fn whitespace_only_is_no_claim() {
        assert_eq!(
            parse_role_completion_block("   \n\n  "),
            RoleCompletionParse::NoClaim
        );
    }

    // ---- check_aap_native_claim_against_metadata ----

    #[test]
    fn check_against_metadata_happy_path_verifier_pass() {
        let metadata = sample_metadata();
        let claim = RoleCompletionClaim {
            role: "VERIFIER".into(),
            result: "PASS".into(),
            workflow_id: "wf-test".into(),
            project_id: "proj-test".into(),
            project_root: "/home/arthur/openab/source".into(),
        };
        assert!(check_aap_native_claim_against_metadata(&claim, &metadata).is_ok());
    }

    #[test]
    fn check_against_metadata_wrong_role_rejects() {
        let mut metadata = sample_metadata();
        metadata.role = "PRIMARY".into();
        let claim = RoleCompletionClaim {
            role: "VERIFIER".into(),
            result: "PASS".into(),
            workflow_id: "wf-test".into(),
            project_id: "proj-test".into(),
            project_root: "/home/arthur/openab/source".into(),
        };
        let err = check_aap_native_claim_against_metadata(&claim, &metadata).unwrap_err();
        assert!(err.contains("role"), "err={err}");
    }

    #[test]
    fn check_against_metadata_workflow_id_mismatch_rejects() {
        let metadata = sample_metadata();
        let claim = RoleCompletionClaim {
            role: "VERIFIER".into(),
            result: "PASS".into(),
            workflow_id: "wf-attacker".into(),
            project_id: "proj-test".into(),
            project_root: "/home/arthur/openab/source".into(),
        };
        let err = check_aap_native_claim_against_metadata(&claim, &metadata).unwrap_err();
        assert!(err.contains("workflow_id"), "err={err}");
    }

    #[test]
    fn check_against_metadata_project_root_mismatch_rejects() {
        let metadata = sample_metadata();
        let claim = RoleCompletionClaim {
            role: "VERIFIER".into(),
            result: "PASS".into(),
            workflow_id: "wf-test".into(),
            project_id: "proj-test".into(),
            project_root: "/attacker/path".into(),
        };
        let err = check_aap_native_claim_against_metadata(&claim, &metadata).unwrap_err();
        assert!(err.contains("project_root"), "err={err}");
    }

    #[test]
    fn check_against_metadata_final_reviewer_fail_rejected() {
        let mut metadata = sample_metadata();
        metadata.role = "FINAL_REVIEWER".into();
        let claim = RoleCompletionClaim {
            role: "FINAL_REVIEWER".into(),
            result: "FAIL".into(),
            workflow_id: "wf-test".into(),
            project_id: "proj-test".into(),
            project_root: "/home/arthur/openab/source".into(),
        };
        let err = check_aap_native_claim_against_metadata(&claim, &metadata).unwrap_err();
        assert!(
            err.contains("role-result") || err.contains("FINAL_REVIEWER"),
            "err={err}"
        );
    }

    #[test]
    fn check_against_metadata_final_reviewer_pass_accepted() {
        let mut metadata = sample_metadata();
        metadata.role = "FINAL_REVIEWER".into();
        let claim = RoleCompletionClaim {
            role: "FINAL_REVIEWER".into(),
            result: "PASS".into(),
            workflow_id: "wf-test".into(),
            project_id: "proj-test".into(),
            project_root: "/home/arthur/openab/source".into(),
        };
        assert!(check_aap_native_claim_against_metadata(&claim, &metadata).is_ok());
    }

    #[test]
    fn check_against_metadata_primary_complete_accepted() {
        let mut metadata = sample_metadata();
        metadata.role = "PRIMARY".into();
        let claim = RoleCompletionClaim {
            role: "PRIMARY".into(),
            result: "COMPLETE".into(),
            workflow_id: "wf-test".into(),
            project_id: "proj-test".into(),
            project_root: "/home/arthur/openab/source".into(),
        };
        assert!(check_aap_native_claim_against_metadata(&claim, &metadata).is_ok());
    }

    #[test]
    fn check_against_metadata_primary_pass_rejected() {
        let mut metadata = sample_metadata();
        metadata.role = "PRIMARY".into();
        let claim = RoleCompletionClaim {
            role: "PRIMARY".into(),
            result: "PASS".into(),
            workflow_id: "wf-test".into(),
            project_id: "proj-test".into(),
            project_root: "/home/arthur/openab/source".into(),
        };
        assert!(check_aap_native_claim_against_metadata(&claim, &metadata).is_err());
    }

    #[test]
    fn check_against_metadata_missing_or_blank_trusted_project_identity_rejects() {
        let claim = RoleCompletionClaim {
            role: "VERIFIER".into(),
            result: "PASS".into(),
            workflow_id: "wf-test".into(),
            project_id: "proj-test".into(),
            project_root: "/home/arthur/openab/source".into(),
        };

        let mut missing_project_id = sample_metadata();
        missing_project_id.project_id = None;
        let err = check_aap_native_claim_against_metadata(&claim, &missing_project_id).unwrap_err();
        assert!(err.contains("project_id"), "err={err}");

        let mut blank_project_id = sample_metadata();
        blank_project_id.project_id = Some("   ".into());
        let err = check_aap_native_claim_against_metadata(&claim, &blank_project_id).unwrap_err();
        assert!(err.contains("project_id"), "err={err}");

        let mut missing_project_root = sample_metadata();
        missing_project_root.project_root = None;
        let err =
            check_aap_native_claim_against_metadata(&claim, &missing_project_root).unwrap_err();
        assert!(err.contains("project_root"), "err={err}");

        let mut blank_project_root = sample_metadata();
        blank_project_root.project_root = Some("   ".into());
        let err = check_aap_native_claim_against_metadata(&claim, &blank_project_root).unwrap_err();
        assert!(err.contains("project_root"), "err={err}");
    }

    #[test]
    fn check_against_metadata_project_id_mismatch_rejects() {
        let metadata = sample_metadata();
        let claim = RoleCompletionClaim {
            role: "VERIFIER".into(),
            result: "PASS".into(),
            workflow_id: "wf-test".into(),
            project_id: "attacker-project".into(),
            project_root: "/home/arthur/openab/source".into(),
        };

        let err = check_aap_native_claim_against_metadata(&claim, &metadata).unwrap_err();
        assert!(err.contains("project_id"), "err={err}");
    }
}
