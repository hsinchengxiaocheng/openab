//! Durable, OpenAB-owned terminal user-delivery records.
//!
//! This module deliberately owns only the terminal-delivery contract and its
//! SQLite persistence.  It does not render Runtime results, call Discord, or
//! decide retry/reconciliation policy.

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, Error as SqlError, ErrorCode, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

pub const TERMINAL_DELIVERY_SCHEMA_VERSION: i32 = 1;
pub const TERMINAL_DELIVERY_RECORD_VERSION: i32 = 1;
pub const TERMINAL_DELIVERY_PAYLOAD_CONFLICT: &str = "TERMINAL_DELIVERY_PAYLOAD_CONFLICT";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TerminalDeliveryState {
    PendingResult,
    ReadyToDeliver,
    Delivering,
    RetryScheduled,
    Ambiguous,
    Delivered,
    PermanentRejected,
    OperatorHold,
}

impl TerminalDeliveryState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PendingResult => "PENDING_RESULT",
            Self::ReadyToDeliver => "READY_TO_DELIVER",
            Self::Delivering => "DELIVERING",
            Self::RetryScheduled => "RETRY_SCHEDULED",
            Self::Ambiguous => "AMBIGUOUS",
            Self::Delivered => "DELIVERED",
            Self::PermanentRejected => "PERMANENT_REJECTED",
            Self::OperatorHold => "OPERATOR_HOLD",
        }
    }

    fn parse(value: &str) -> Result<Self, TerminalDeliveryError> {
        match value {
            "PENDING_RESULT" => Ok(Self::PendingResult),
            "READY_TO_DELIVER" => Ok(Self::ReadyToDeliver),
            "DELIVERING" => Ok(Self::Delivering),
            "RETRY_SCHEDULED" => Ok(Self::RetryScheduled),
            "AMBIGUOUS" => Ok(Self::Ambiguous),
            "DELIVERED" => Ok(Self::Delivered),
            "PERMANENT_REJECTED" => Ok(Self::PermanentRejected),
            "OPERATOR_HOLD" => Ok(Self::OperatorHold),
            _ => Err(TerminalDeliveryError::MalformedRecord(
                "unknown state".into(),
            )),
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Delivered | Self::PermanentRejected)
    }

    pub const fn may_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::PendingResult,
                Self::ReadyToDeliver | Self::OperatorHold
            ) | (Self::ReadyToDeliver, Self::Delivering | Self::OperatorHold)
                | (
                    Self::Delivering,
                    Self::Delivered
                        | Self::RetryScheduled
                        | Self::PermanentRejected
                        | Self::Ambiguous
                )
                | (Self::RetryScheduled, Self::Delivering | Self::OperatorHold)
                | (Self::Ambiguous, Self::Delivered | Self::OperatorHold)
                | (
                    Self::OperatorHold,
                    Self::ReadyToDeliver | Self::Delivered | Self::PermanentRejected
                )
        )
    }
}

impl fmt::Display for TerminalDeliveryState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Immutable material supplied by the later payload-materialisation phase.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewTerminalDeliveryRecord {
    pub openab_inbound_turn_id: String,
    pub platform: String,
    pub channel_id: String,
    pub thread_id: Option<String>,
    pub acp_request_id: String,
    pub acp_session_id: String,
    pub runtime_run_id: String,
    pub workflow_run_id: Option<String>,
    pub conversation_id: String,
    pub response_sequence: u64,
    pub terminal_payload: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TerminalDeliveryRecordV1 {
    pub record_version: i32,
    pub terminal_delivery_key: String,
    pub openab_inbound_turn_id: String,
    pub platform: String,
    pub channel_id: String,
    pub thread_id: Option<String>,
    pub acp_request_id: String,
    /// Transport correlation only: deliberately excluded from durable identity.
    pub acp_session_id: String,
    pub runtime_run_id: String,
    pub workflow_run_id: Option<String>,
    pub conversation_id: String,
    pub response_sequence: u64,
    pub terminal_payload: Value,
    pub terminal_payload_digest: String,
    pub state: TerminalDeliveryState,
    pub attempt_count: u64,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub last_failure_classification: Option<String>,
    pub last_safe_error: Option<String>,
    pub discord_message_id: Option<String>,
    pub operator_hold_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub delivered_at: Option<DateTime<Utc>>,
    pub state_revision: u64,
    pub delivery_lease_token: Option<String>,
    pub delivery_started_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalDeliveryEvent {
    pub terminal_delivery_key: String,
    pub previous_state: Option<TerminalDeliveryState>,
    pub new_state: TerminalDeliveryState,
    pub state_revision: u64,
    pub safe_reason_code: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct TransitionUpdate {
    pub attempt_count: Option<u64>,
    pub next_attempt_at: Option<Option<DateTime<Utc>>>,
    pub last_failure_classification: Option<Option<String>>,
    pub last_safe_error: Option<Option<String>>,
    pub discord_message_id: Option<String>,
    pub operator_hold_reason: Option<Option<String>>,
    pub delivery_lease_token: Option<Option<String>>,
    pub delivery_started_at: Option<Option<DateTime<Utc>>>,
}

#[derive(Debug)]
pub enum TerminalDeliveryError {
    Storage(String),
    SchemaVersion {
        found: i32,
    },
    Corrupt(String),
    MalformedRecord(String),
    InvalidInput(&'static str),
    PayloadConflict {
        code: &'static str,
    },
    IllegalTransition {
        from: TerminalDeliveryState,
        to: TerminalDeliveryState,
    },
    ImmutableTerminalState(TerminalDeliveryState),
    RevisionConflict {
        expected: u64,
        actual: u64,
    },
    DiscordMessageIdWriteOnce,
    UnsafeAuditField(&'static str),
}

impl fmt::Display for TerminalDeliveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(detail) => write!(f, "terminal delivery storage failure: {detail}"),
            Self::SchemaVersion { found } => {
                write!(f, "unsupported terminal delivery schema version {found}")
            }
            Self::Corrupt(detail) => write!(f, "terminal delivery database corruption: {detail}"),
            Self::MalformedRecord(detail) => {
                write!(f, "malformed terminal delivery record: {detail}")
            }
            Self::InvalidInput(field) => write!(f, "invalid terminal delivery input: {field}"),
            Self::PayloadConflict { code } => {
                write!(f, "terminal delivery payload conflict: {code}")
            }
            Self::IllegalTransition { from, to } => {
                write!(f, "illegal terminal delivery transition: {from} -> {to}")
            }
            Self::ImmutableTerminalState(state) => {
                write!(f, "terminal delivery state is immutable: {state}")
            }
            Self::RevisionConflict { expected, actual } => write!(
                f,
                "terminal delivery revision conflict: expected {expected}, actual {actual}"
            ),
            Self::DiscordMessageIdWriteOnce => f.write_str("discord_message_id is write-once"),
            Self::UnsafeAuditField(field) => {
                write!(f, "unsafe terminal delivery audit field: {field}")
            }
        }
    }
}
impl std::error::Error for TerminalDeliveryError {}

#[derive(Debug, Clone)]
pub struct TerminalDeliveryRepository {
    database_path: PathBuf,
}

impl TerminalDeliveryRepository {
    /// Opens and validates `<data_dir>/openab/terminal-deliveries.db`.
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, TerminalDeliveryError> {
        let path = data_dir
            .as_ref()
            .join("openab")
            .join("terminal-deliveries.db");
        Self::open_path(path)
    }

    /// Kept public for hermetic tests and callers that already own a data path.
    pub fn open_path(path: impl Into<PathBuf>) -> Result<Self, TerminalDeliveryError> {
        let database_path = path.into();
        if let Some(parent) = database_path.parent() {
            fs::create_dir_all(parent).map_err(|e| storage(e.to_string()))?;
        }
        let repo = Self { database_path };
        let mut conn = repo.connect()?;
        repo.initialize_and_validate(&mut conn)?;
        Ok(repo)
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub fn create_or_reuse(
        &self,
        input: NewTerminalDeliveryRecord,
    ) -> Result<TerminalDeliveryRecordV1, TerminalDeliveryError> {
        validate_new(&input)?;
        let key = terminal_delivery_key(&input)?;
        let payload = canonical_json(&input.terminal_payload)?;
        let digest = sha256_hex(payload.as_bytes());
        let slot = SemanticSlot::from_input(&input);
        let now = Utc::now();
        let mut conn = self.connect()?;
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if let Some(existing) = select_record(&tx, &key)? {
            tx.commit().map_err(sql_error)?;
            return Ok(existing);
        }
        if let Some(existing) = select_slot(&tx, &slot)? {
            if existing.terminal_payload_digest == digest {
                tx.commit().map_err(sql_error)?;
                return Ok(existing);
            }
            return Err(TerminalDeliveryError::PayloadConflict {
                code: TERMINAL_DELIVERY_PAYLOAD_CONFLICT,
            });
        }
        tx.execute(
            "INSERT INTO terminal_deliveries (record_version, terminal_delivery_key, openab_inbound_turn_id, platform, channel_id, thread_id_normalized, thread_id, acp_request_id, acp_session_id, runtime_run_id, workflow_run_id_normalized, workflow_run_id, conversation_id, response_sequence, terminal_payload, terminal_payload_digest, state, attempt_count, next_attempt_at, last_failure_classification, last_safe_error, discord_message_id, operator_hold_reason, created_at, updated_at, delivered_at, state_revision, delivery_lease_token, delivery_started_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, 'PENDING_RESULT', 0, NULL, NULL, NULL, NULL, NULL, ?17, ?17, NULL, 0, NULL, NULL)",
            params![TERMINAL_DELIVERY_RECORD_VERSION, key, input.openab_inbound_turn_id, input.platform, input.channel_id, slot.thread_id_normalized, input.thread_id, input.acp_request_id, input.acp_session_id, input.runtime_run_id, slot.workflow_run_id_normalized, input.workflow_run_id, input.conversation_id, input.response_sequence.to_string(), payload, digest, timestamp(now)],
        ).map_err(sql_error)?;
        insert_event(
            &tx,
            &key,
            None,
            TerminalDeliveryState::PendingResult,
            0,
            "CREATED",
            now,
        )?;
        let record = select_record(&tx, &key)?
            .ok_or_else(|| TerminalDeliveryError::Storage("created row unavailable".into()))?;
        tx.commit().map_err(sql_error)?;
        Ok(record)
    }

    pub fn get(
        &self,
        key: &str,
    ) -> Result<Option<TerminalDeliveryRecordV1>, TerminalDeliveryError> {
        let conn = self.connect()?;
        select_record(&conn, key)
    }

    pub fn transition(
        &self,
        key: &str,
        expected_revision: u64,
        new_state: TerminalDeliveryState,
        update: TransitionUpdate,
        safe_reason_code: &str,
    ) -> Result<TerminalDeliveryRecordV1, TerminalDeliveryError> {
        validate_safe_field(safe_reason_code, "safe_reason_code")?;
        validate_transition_update(&update)?;
        let now = Utc::now();
        let mut conn = self.connect()?;
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let current = select_record(&tx, key)?
            .ok_or(TerminalDeliveryError::InvalidInput("terminal_delivery_key"))?;
        if current.state.is_terminal() {
            return Err(TerminalDeliveryError::ImmutableTerminalState(current.state));
        }
        if current.state_revision != expected_revision {
            return Err(TerminalDeliveryError::RevisionConflict {
                expected: expected_revision,
                actual: current.state_revision,
            });
        }
        if !current.state.may_transition_to(new_state) {
            return Err(TerminalDeliveryError::IllegalTransition {
                from: current.state,
                to: new_state,
            });
        }
        if update
            .attempt_count
            .is_some_and(|count| count < current.attempt_count)
        {
            return Err(TerminalDeliveryError::InvalidInput(
                "attempt_count must be monotonic",
            ));
        }
        if current.discord_message_id.is_some() && update.discord_message_id.is_some() {
            return Err(TerminalDeliveryError::DiscordMessageIdWriteOnce);
        }
        let revision =
            current
                .state_revision
                .checked_add(1)
                .ok_or(TerminalDeliveryError::InvalidInput(
                    "state_revision overflow",
                ))?;
        let attempt_count = update.attempt_count.unwrap_or(current.attempt_count);
        let next_attempt_at = update.next_attempt_at.unwrap_or(current.next_attempt_at);
        let failure_classification = update
            .last_failure_classification
            .unwrap_or(current.last_failure_classification);
        let safe_error = update.last_safe_error.unwrap_or(current.last_safe_error);
        let message_id = update.discord_message_id.or(current.discord_message_id);
        let hold_reason = update
            .operator_hold_reason
            .unwrap_or(current.operator_hold_reason);
        let lease = update
            .delivery_lease_token
            .unwrap_or(current.delivery_lease_token);
        let started = update
            .delivery_started_at
            .unwrap_or(current.delivery_started_at);
        let delivered_at = if new_state == TerminalDeliveryState::Delivered {
            Some(now)
        } else {
            current.delivered_at
        };
        tx.execute(
            "UPDATE terminal_deliveries SET state=?1, attempt_count=?2, next_attempt_at=?3, last_failure_classification=?4, last_safe_error=?5, discord_message_id=?6, operator_hold_reason=?7, updated_at=?8, delivered_at=?9, state_revision=?10, delivery_lease_token=?11, delivery_started_at=?12 WHERE terminal_delivery_key=?13 AND state_revision=?14",
            params![new_state.as_str(), attempt_count.to_string(), opt_timestamp(next_attempt_at), failure_classification, safe_error, message_id, hold_reason, timestamp(now), opt_timestamp(delivered_at), revision.to_string(), lease, opt_timestamp(started), key, expected_revision.to_string()],
        ).map_err(sql_error)?;
        insert_event(
            &tx,
            key,
            Some(current.state),
            new_state,
            revision,
            safe_reason_code,
            now,
        )?;
        let updated = select_record(&tx, key)?
            .ok_or_else(|| TerminalDeliveryError::Storage("transitioned row unavailable".into()))?;
        tx.commit().map_err(sql_error)?;
        Ok(updated)
    }

    pub fn events(&self, key: &str) -> Result<Vec<TerminalDeliveryEvent>, TerminalDeliveryError> {
        let conn = self.connect()?;
        let mut statement = conn.prepare("SELECT terminal_delivery_key, previous_state, new_state, state_revision, safe_reason_code, timestamp FROM terminal_delivery_events WHERE terminal_delivery_key=?1 ORDER BY event_id ASC").map_err(sql_error)?;
        let events = statement
            .query_map(params![key], event_from_row)
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        Ok(events)
    }

    fn connect(&self) -> Result<Connection, TerminalDeliveryError> {
        let conn = Connection::open(&self.database_path).map_err(sql_error)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(sql_error)?;
        Ok(conn)
    }

    fn initialize_and_validate(&self, conn: &mut Connection) -> Result<(), TerminalDeliveryError> {
        let version: i32 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(sql_error)?;
        if version == 0 {
            let tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            create_schema(&tx)?;
            tx.commit().map_err(sql_error)?;
        } else if version != TERMINAL_DELIVERY_SCHEMA_VERSION {
            return Err(TerminalDeliveryError::SchemaVersion { found: version });
        }
        let check: String = conn
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .map_err(sql_error)?;
        if check != "ok" {
            return Err(TerminalDeliveryError::Corrupt(check));
        }
        let mut statement = conn.prepare("SELECT record_version, terminal_delivery_key, openab_inbound_turn_id, platform, channel_id, thread_id, acp_request_id, acp_session_id, runtime_run_id, workflow_run_id, conversation_id, response_sequence, terminal_payload, terminal_payload_digest, state, attempt_count, next_attempt_at, last_failure_classification, last_safe_error, discord_message_id, operator_hold_reason, created_at, updated_at, delivered_at, state_revision, delivery_lease_token, delivery_started_at FROM terminal_deliveries").map_err(sql_error)?;
        for row in statement
            .query_map([], record_from_row)
            .map_err(sql_error)?
        {
            validate_record(&row.map_err(sql_error)?)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct SemanticSlot {
    record_version: i32,
    openab_inbound_turn_id: String,
    platform: String,
    channel_id: String,
    thread_id_normalized: String,
    response_sequence: String,
    runtime_run_id: String,
    workflow_run_id_normalized: String,
}
impl SemanticSlot {
    fn from_input(input: &NewTerminalDeliveryRecord) -> Self {
        Self {
            record_version: TERMINAL_DELIVERY_RECORD_VERSION,
            openab_inbound_turn_id: input.openab_inbound_turn_id.clone(),
            platform: input.platform.clone(),
            channel_id: input.channel_id.clone(),
            thread_id_normalized: normalize(&input.thread_id),
            response_sequence: input.response_sequence.to_string(),
            runtime_run_id: input.runtime_run_id.clone(),
            workflow_run_id_normalized: normalize(&input.workflow_run_id),
        }
    }
}

#[derive(Serialize)]
struct KeyMaterial<'a> {
    record_version: i32,
    openab_inbound_turn_id: &'a str,
    platform: &'a str,
    channel_id: &'a str,
    thread_id_normalized: String,
    acp_request_id: &'a str,
    runtime_run_id: &'a str,
    workflow_run_id_normalized: String,
    conversation_id: &'a str,
    response_sequence: u64,
    terminal_payload: &'a Value,
}

pub fn terminal_delivery_key(
    input: &NewTerminalDeliveryRecord,
) -> Result<String, TerminalDeliveryError> {
    validate_new(input)?;
    let material = KeyMaterial {
        record_version: TERMINAL_DELIVERY_RECORD_VERSION,
        openab_inbound_turn_id: &input.openab_inbound_turn_id,
        platform: &input.platform,
        channel_id: &input.channel_id,
        thread_id_normalized: normalize(&input.thread_id),
        acp_request_id: &input.acp_request_id,
        runtime_run_id: &input.runtime_run_id,
        workflow_run_id_normalized: normalize(&input.workflow_run_id),
        conversation_id: &input.conversation_id,
        response_sequence: input.response_sequence,
        terminal_payload: &input.terminal_payload,
    };
    Ok(sha256_hex(canonical_json(&material)?.as_bytes()))
}

fn create_schema(tx: &Transaction<'_>) -> Result<(), TerminalDeliveryError> {
    tx.execute_batch("CREATE TABLE terminal_deliveries (record_version INTEGER NOT NULL, terminal_delivery_key TEXT PRIMARY KEY NOT NULL, openab_inbound_turn_id TEXT NOT NULL, platform TEXT NOT NULL, channel_id TEXT NOT NULL, thread_id_normalized TEXT NOT NULL, thread_id TEXT, acp_request_id TEXT NOT NULL, acp_session_id TEXT NOT NULL, runtime_run_id TEXT NOT NULL, workflow_run_id_normalized TEXT NOT NULL, workflow_run_id TEXT, conversation_id TEXT NOT NULL, response_sequence TEXT NOT NULL, terminal_payload TEXT NOT NULL, terminal_payload_digest TEXT NOT NULL, state TEXT NOT NULL, attempt_count TEXT NOT NULL, next_attempt_at TEXT, last_failure_classification TEXT, last_safe_error TEXT, discord_message_id TEXT, operator_hold_reason TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, delivered_at TEXT, state_revision TEXT NOT NULL, delivery_lease_token TEXT, delivery_started_at TEXT, UNIQUE(record_version, openab_inbound_turn_id, platform, channel_id, thread_id_normalized, response_sequence, runtime_run_id, workflow_run_id_normalized)); CREATE UNIQUE INDEX terminal_deliveries_discord_message_id_unique ON terminal_deliveries(platform, discord_message_id) WHERE discord_message_id IS NOT NULL; CREATE TABLE terminal_delivery_events (event_id INTEGER PRIMARY KEY AUTOINCREMENT, terminal_delivery_key TEXT NOT NULL REFERENCES terminal_deliveries(terminal_delivery_key), previous_state TEXT, new_state TEXT NOT NULL, state_revision TEXT NOT NULL, safe_reason_code TEXT NOT NULL, timestamp TEXT NOT NULL); CREATE TRIGGER terminal_delivery_events_no_update BEFORE UPDATE ON terminal_delivery_events BEGIN SELECT RAISE(ABORT, 'terminal_delivery_events append-only'); END; CREATE TRIGGER terminal_delivery_events_no_delete BEFORE DELETE ON terminal_delivery_events BEGIN SELECT RAISE(ABORT, 'terminal_delivery_events append-only'); END; PRAGMA user_version = 1;").map_err(sql_error)
}

fn select_record(
    conn: &Connection,
    key: &str,
) -> Result<Option<TerminalDeliveryRecordV1>, TerminalDeliveryError> {
    conn.query_row("SELECT record_version, terminal_delivery_key, openab_inbound_turn_id, platform, channel_id, thread_id, acp_request_id, acp_session_id, runtime_run_id, workflow_run_id, conversation_id, response_sequence, terminal_payload, terminal_payload_digest, state, attempt_count, next_attempt_at, last_failure_classification, last_safe_error, discord_message_id, operator_hold_reason, created_at, updated_at, delivered_at, state_revision, delivery_lease_token, delivery_started_at FROM terminal_deliveries WHERE terminal_delivery_key=?1", params![key], record_from_row).optional().map_err(sql_error)
}
fn select_slot(
    conn: &Connection,
    slot: &SemanticSlot,
) -> Result<Option<TerminalDeliveryRecordV1>, TerminalDeliveryError> {
    conn.query_row("SELECT record_version, terminal_delivery_key, openab_inbound_turn_id, platform, channel_id, thread_id, acp_request_id, acp_session_id, runtime_run_id, workflow_run_id, conversation_id, response_sequence, terminal_payload, terminal_payload_digest, state, attempt_count, next_attempt_at, last_failure_classification, last_safe_error, discord_message_id, operator_hold_reason, created_at, updated_at, delivered_at, state_revision, delivery_lease_token, delivery_started_at FROM terminal_deliveries WHERE record_version=?1 AND openab_inbound_turn_id=?2 AND platform=?3 AND channel_id=?4 AND thread_id_normalized=?5 AND response_sequence=?6 AND runtime_run_id=?7 AND workflow_run_id_normalized=?8", params![slot.record_version, slot.openab_inbound_turn_id, slot.platform, slot.channel_id, slot.thread_id_normalized, slot.response_sequence, slot.runtime_run_id, slot.workflow_run_id_normalized], record_from_row).optional().map_err(sql_error)
}

fn insert_event(
    tx: &Transaction<'_>,
    key: &str,
    previous: Option<TerminalDeliveryState>,
    new: TerminalDeliveryState,
    revision: u64,
    reason: &str,
    at: DateTime<Utc>,
) -> Result<(), TerminalDeliveryError> {
    tx.execute("INSERT INTO terminal_delivery_events (terminal_delivery_key, previous_state, new_state, state_revision, safe_reason_code, timestamp) VALUES (?1, ?2, ?3, ?4, ?5, ?6)", params![key, previous.map(TerminalDeliveryState::as_str), new.as_str(), revision.to_string(), reason, timestamp(at)]).map_err(sql_error)?;
    Ok(())
}

fn record_from_row(row: &rusqlite::Row<'_>) -> Result<TerminalDeliveryRecordV1, SqlError> {
    let json: String = row.get(12)?;
    let state: String = row.get(14)?;
    Ok(TerminalDeliveryRecordV1 {
        record_version: row.get(0)?,
        terminal_delivery_key: row.get(1)?,
        openab_inbound_turn_id: row.get(2)?,
        platform: row.get(3)?,
        channel_id: row.get(4)?,
        thread_id: row.get(5)?,
        acp_request_id: row.get(6)?,
        acp_session_id: row.get(7)?,
        runtime_run_id: row.get(8)?,
        workflow_run_id: row.get(9)?,
        conversation_id: row.get(10)?,
        response_sequence: parse_u64(row.get::<_, String>(11)?).map_err(to_sql)?,
        terminal_payload: serde_json::from_str(&json).map_err(|_| {
            to_sql(TerminalDeliveryError::MalformedRecord(
                "terminal_payload".into(),
            ))
        })?,
        terminal_payload_digest: row.get(13)?,
        state: TerminalDeliveryState::parse(&state).map_err(to_sql)?,
        attempt_count: parse_u64(row.get::<_, String>(15)?).map_err(to_sql)?,
        next_attempt_at: parse_optional_time(row.get(16)?).map_err(to_sql)?,
        last_failure_classification: row.get(17)?,
        last_safe_error: row.get(18)?,
        discord_message_id: row.get(19)?,
        operator_hold_reason: row.get(20)?,
        created_at: parse_time(row.get(21)?).map_err(to_sql)?,
        updated_at: parse_time(row.get(22)?).map_err(to_sql)?,
        delivered_at: parse_optional_time(row.get(23)?).map_err(to_sql)?,
        state_revision: parse_u64(row.get::<_, String>(24)?).map_err(to_sql)?,
        delivery_lease_token: row.get(25)?,
        delivery_started_at: parse_optional_time(row.get(26)?).map_err(to_sql)?,
    })
}
fn event_from_row(row: &rusqlite::Row<'_>) -> Result<TerminalDeliveryEvent, SqlError> {
    let previous: Option<String> = row.get(1)?;
    let new: String = row.get(2)?;
    Ok(TerminalDeliveryEvent {
        terminal_delivery_key: row.get(0)?,
        previous_state: previous
            .map(|v| TerminalDeliveryState::parse(&v))
            .transpose()
            .map_err(to_sql)?,
        new_state: TerminalDeliveryState::parse(&new).map_err(to_sql)?,
        state_revision: parse_u64(row.get::<_, String>(3)?).map_err(to_sql)?,
        safe_reason_code: row.get(4)?,
        timestamp: parse_time(row.get(5)?).map_err(to_sql)?,
    })
}
fn validate_new(input: &NewTerminalDeliveryRecord) -> Result<(), TerminalDeliveryError> {
    for value in [
        &input.openab_inbound_turn_id,
        &input.platform,
        &input.channel_id,
        &input.acp_request_id,
        &input.acp_session_id,
        &input.runtime_run_id,
        &input.conversation_id,
    ] {
        if value.trim().is_empty() {
            return Err(TerminalDeliveryError::InvalidInput("required identifier"));
        }
    }
    if input
        .thread_id
        .as_deref()
        .is_some_and(|s| s.trim().is_empty())
        || input
            .workflow_run_id
            .as_deref()
            .is_some_and(|s| s.trim().is_empty())
    {
        return Err(TerminalDeliveryError::InvalidInput("optional identifier"));
    }
    Ok(())
}
fn validate_record(record: &TerminalDeliveryRecordV1) -> Result<(), TerminalDeliveryError> {
    if record.record_version != TERMINAL_DELIVERY_RECORD_VERSION {
        return Err(TerminalDeliveryError::MalformedRecord(
            "record_version".into(),
        ));
    }
    let input = NewTerminalDeliveryRecord {
        openab_inbound_turn_id: record.openab_inbound_turn_id.clone(),
        platform: record.platform.clone(),
        channel_id: record.channel_id.clone(),
        thread_id: record.thread_id.clone(),
        acp_request_id: record.acp_request_id.clone(),
        acp_session_id: record.acp_session_id.clone(),
        runtime_run_id: record.runtime_run_id.clone(),
        workflow_run_id: record.workflow_run_id.clone(),
        conversation_id: record.conversation_id.clone(),
        response_sequence: record.response_sequence,
        terminal_payload: record.terminal_payload.clone(),
    };
    validate_new(&input)?;
    if terminal_delivery_key(&input)? != record.terminal_delivery_key {
        return Err(TerminalDeliveryError::MalformedRecord(
            "terminal_delivery_key mismatch".into(),
        ));
    }
    if sha256_hex(canonical_json(&record.terminal_payload)?.as_bytes())
        != record.terminal_payload_digest
    {
        return Err(TerminalDeliveryError::MalformedRecord(
            "terminal_payload_digest mismatch".into(),
        ));
    }
    for value in [
        &record.last_failure_classification,
        &record.last_safe_error,
        &record.operator_hold_reason,
    ]
    .into_iter()
    .flatten()
    {
        validate_safe_field(value, "persisted safe field")?;
    }
    Ok(())
}
fn validate_transition_update(update: &TransitionUpdate) -> Result<(), TerminalDeliveryError> {
    for value in [
        &update.last_failure_classification,
        &update.last_safe_error,
        &update.operator_hold_reason,
    ] {
        if let Some(Some(value)) = value {
            validate_safe_field(value, "safe field")?;
        }
    }
    Ok(())
}
fn validate_safe_field(value: &str, name: &'static str) -> Result<(), TerminalDeliveryError> {
    if value.is_empty()
        || value.len() > 256
        || !value.bytes().all(|b| {
            b.is_ascii_uppercase() || b.is_ascii_digit() || matches!(b, b'_' | b'-' | b'.' | b':')
        })
        || value.to_ascii_lowercase().contains("bearer")
        || value.to_ascii_lowercase().contains("token")
        || value.to_ascii_lowercase().contains("authorization")
    {
        return Err(TerminalDeliveryError::UnsafeAuditField(name));
    }
    Ok(())
}
fn canonical_json<T: Serialize>(value: &T) -> Result<String, TerminalDeliveryError> {
    serde_json::to_string(value)
        .map_err(|_| TerminalDeliveryError::InvalidInput("terminal_payload"))
}
fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn normalize(value: &Option<String>) -> String {
    value.clone().unwrap_or_default()
}
fn timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}
fn opt_timestamp(value: Option<DateTime<Utc>>) -> Option<String> {
    value.map(timestamp)
}
fn parse_time(value: String) -> Result<DateTime<Utc>, TerminalDeliveryError> {
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| TerminalDeliveryError::MalformedRecord("timestamp".into()))
}
fn parse_optional_time(
    value: Option<String>,
) -> Result<Option<DateTime<Utc>>, TerminalDeliveryError> {
    value.map(parse_time).transpose()
}
fn parse_u64(value: String) -> Result<u64, TerminalDeliveryError> {
    value
        .parse()
        .map_err(|_| TerminalDeliveryError::MalformedRecord("integer".into()))
}
fn to_sql(error: TerminalDeliveryError) -> SqlError {
    SqlError::ToSqlConversionFailure(Box::new(error))
}
fn sql_error(error: SqlError) -> TerminalDeliveryError {
    match &error {
        SqlError::SqliteFailure(code, _)
            if code.code == ErrorCode::DatabaseCorrupt || code.code == ErrorCode::NotADatabase =>
        {
            TerminalDeliveryError::Corrupt(error.to_string())
        }
        _ => TerminalDeliveryError::Storage(error.to_string()),
    }
}
fn storage(detail: String) -> TerminalDeliveryError {
    TerminalDeliveryError::Storage(detail)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    fn repo() -> (TempDir, TerminalDeliveryRepository) {
        let temp = TempDir::new().unwrap();
        let repo = TerminalDeliveryRepository::open(temp.path()).unwrap();
        (temp, repo)
    }
    fn input() -> NewTerminalDeliveryRecord {
        NewTerminalDeliveryRecord {
            openab_inbound_turn_id: "turn-1".into(),
            platform: "discord".into(),
            channel_id: "channel-1".into(),
            thread_id: Some("thread-1".into()),
            acp_request_id: "request-1".into(),
            acp_session_id: "session-1".into(),
            runtime_run_id: "runtime-1".into(),
            workflow_run_id: Some("workflow-1".into()),
            conversation_id: "conversation-1".into(),
            response_sequence: 0,
            terminal_payload: serde_json::json!({"content":"already safe"}),
        }
    }
    fn step(
        repo: &TerminalDeliveryRepository,
        record: &TerminalDeliveryRecordV1,
        state: TerminalDeliveryState,
    ) -> TerminalDeliveryRecordV1 {
        repo.transition(
            &record.terminal_delivery_key,
            record.state_revision,
            state,
            TransitionUpdate::default(),
            "TEST_TRANSITION",
        )
        .unwrap()
    }

    #[test]
    fn creates_pending_and_replays_same_key() {
        let (_temp, repo) = repo();
        let first = repo.create_or_reuse(input()).unwrap();
        let second = repo.create_or_reuse(input()).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.state, TerminalDeliveryState::PendingResult);
        assert_eq!(first.state_revision, 0);
    }
    #[test]
    fn semantic_slot_same_payload_reuses_and_different_payload_conflicts() {
        let (_temp, repo) = repo();
        let first = repo.create_or_reuse(input()).unwrap();
        let mut same = input();
        same.acp_request_id = "request-other".into();
        same.acp_session_id = "session-other".into();
        assert_eq!(
            repo.create_or_reuse(same).unwrap().terminal_delivery_key,
            first.terminal_delivery_key
        );
        let mut conflict = input();
        conflict.terminal_payload = serde_json::json!({"content":"different"});
        assert!(matches!(
            repo.create_or_reuse(conflict),
            Err(TerminalDeliveryError::PayloadConflict {
                code: TERMINAL_DELIVERY_PAYLOAD_CONFLICT
            })
        ));
    }
    #[test]
    fn every_legal_transition_and_terminal_immutability() {
        let (_temp, repo) = repo();
        let pending = repo.create_or_reuse(input()).unwrap();
        let hold = step(&repo, &pending, TerminalDeliveryState::OperatorHold);
        let ready = step(&repo, &hold, TerminalDeliveryState::ReadyToDeliver);
        let delivering = step(&repo, &ready, TerminalDeliveryState::Delivering);
        let retry = step(&repo, &delivering, TerminalDeliveryState::RetryScheduled);
        let delivering = step(&repo, &retry, TerminalDeliveryState::Delivering);
        let ambiguous = step(&repo, &delivering, TerminalDeliveryState::Ambiguous);
        let delivered = step(&repo, &ambiguous, TerminalDeliveryState::Delivered);
        assert!(matches!(
            repo.transition(
                &delivered.terminal_delivery_key,
                delivered.state_revision,
                TerminalDeliveryState::OperatorHold,
                TransitionUpdate::default(),
                "TEST"
            ),
            Err(TerminalDeliveryError::ImmutableTerminalState(_))
        ));
        let pending = repo
            .create_or_reuse(NewTerminalDeliveryRecord {
                response_sequence: 1,
                ..input()
            })
            .unwrap();
        let ready = step(&repo, &pending, TerminalDeliveryState::ReadyToDeliver);
        let delivering = step(&repo, &ready, TerminalDeliveryState::Delivering);
        let rejected = step(&repo, &delivering, TerminalDeliveryState::PermanentRejected);
        assert!(matches!(
            repo.transition(
                &rejected.terminal_delivery_key,
                rejected.state_revision,
                TerminalDeliveryState::ReadyToDeliver,
                TransitionUpdate::default(),
                "TEST"
            ),
            Err(TerminalDeliveryError::ImmutableTerminalState(_))
        ));
    }
    #[test]
    fn illegal_transitions_and_ambiguous_retry_are_rejected() {
        let (_temp, repo) = repo();
        let record = repo.create_or_reuse(input()).unwrap();
        assert!(matches!(
            repo.transition(
                &record.terminal_delivery_key,
                0,
                TerminalDeliveryState::Delivering,
                TransitionUpdate::default(),
                "TEST"
            ),
            Err(TerminalDeliveryError::IllegalTransition { .. })
        ));
        let ready = step(&repo, &record, TerminalDeliveryState::ReadyToDeliver);
        let delivering = step(&repo, &ready, TerminalDeliveryState::Delivering);
        let ambiguous = step(&repo, &delivering, TerminalDeliveryState::Ambiguous);
        assert!(matches!(
            repo.transition(
                &ambiguous.terminal_delivery_key,
                ambiguous.state_revision,
                TerminalDeliveryState::RetryScheduled,
                TransitionUpdate::default(),
                "TEST"
            ),
            Err(TerminalDeliveryError::IllegalTransition { .. })
        ));
    }
    #[test]
    fn state_machine_accepts_only_the_approved_edges() {
        let states = [
            TerminalDeliveryState::PendingResult,
            TerminalDeliveryState::ReadyToDeliver,
            TerminalDeliveryState::Delivering,
            TerminalDeliveryState::RetryScheduled,
            TerminalDeliveryState::Ambiguous,
            TerminalDeliveryState::Delivered,
            TerminalDeliveryState::PermanentRejected,
            TerminalDeliveryState::OperatorHold,
        ];
        for from in states {
            for to in states {
                let approved = matches!(
                    (from, to),
                    (
                        TerminalDeliveryState::PendingResult,
                        TerminalDeliveryState::ReadyToDeliver | TerminalDeliveryState::OperatorHold
                    ) | (
                        TerminalDeliveryState::ReadyToDeliver,
                        TerminalDeliveryState::Delivering | TerminalDeliveryState::OperatorHold
                    ) | (
                        TerminalDeliveryState::Delivering,
                        TerminalDeliveryState::Delivered
                            | TerminalDeliveryState::RetryScheduled
                            | TerminalDeliveryState::PermanentRejected
                            | TerminalDeliveryState::Ambiguous
                    ) | (
                        TerminalDeliveryState::RetryScheduled,
                        TerminalDeliveryState::Delivering | TerminalDeliveryState::OperatorHold
                    ) | (
                        TerminalDeliveryState::Ambiguous,
                        TerminalDeliveryState::Delivered | TerminalDeliveryState::OperatorHold
                    ) | (
                        TerminalDeliveryState::OperatorHold,
                        TerminalDeliveryState::ReadyToDeliver
                            | TerminalDeliveryState::Delivered
                            | TerminalDeliveryState::PermanentRejected
                    )
                );
                assert_eq!(from.may_transition_to(to), approved, "{from} -> {to}");
            }
        }
    }
    #[test]
    fn operator_hold_cas_attempts_and_message_id_are_enforced() {
        let (_temp, repo) = repo();
        let record = repo.create_or_reuse(input()).unwrap();
        let hold = step(&repo, &record, TerminalDeliveryState::OperatorHold);
        assert!(matches!(
            repo.transition(
                &hold.terminal_delivery_key,
                0,
                TerminalDeliveryState::ReadyToDeliver,
                TransitionUpdate::default(),
                "TEST"
            ),
            Err(TerminalDeliveryError::RevisionConflict { .. })
        ));
        let mut update = TransitionUpdate {
            attempt_count: Some(2),
            discord_message_id: Some("message-1".into()),
            ..Default::default()
        };
        let ready = repo
            .transition(
                &hold.terminal_delivery_key,
                hold.state_revision,
                TerminalDeliveryState::ReadyToDeliver,
                update.clone(),
                "OPERATOR_RELEASE",
            )
            .unwrap();
        update.attempt_count = Some(1);
        assert!(matches!(
            repo.transition(
                &ready.terminal_delivery_key,
                ready.state_revision,
                TerminalDeliveryState::Delivering,
                update,
                "TEST"
            ),
            Err(TerminalDeliveryError::InvalidInput(_))
        ));
        let delivering = step(&repo, &ready, TerminalDeliveryState::Delivering);
        assert!(matches!(
            repo.transition(
                &delivering.terminal_delivery_key,
                delivering.state_revision,
                TerminalDeliveryState::Delivered,
                TransitionUpdate {
                    discord_message_id: Some("message-2".into()),
                    ..Default::default()
                },
                "TEST"
            ),
            Err(TerminalDeliveryError::DiscordMessageIdWriteOnce)
        ));
    }
    #[test]
    fn partial_unique_message_id_and_event_audit_are_enforced() {
        let (_temp, repo) = repo();
        let first = repo.create_or_reuse(input()).unwrap();
        let first = step(&repo, &first, TerminalDeliveryState::ReadyToDeliver);
        let first = repo
            .transition(
                &first.terminal_delivery_key,
                first.state_revision,
                TerminalDeliveryState::Delivering,
                TransitionUpdate {
                    discord_message_id: Some("message-1".into()),
                    ..Default::default()
                },
                "LEASED",
            )
            .unwrap();
        let second = repo
            .create_or_reuse(NewTerminalDeliveryRecord {
                response_sequence: 1,
                ..input()
            })
            .unwrap();
        let second = step(&repo, &second, TerminalDeliveryState::ReadyToDeliver);
        assert!(matches!(
            repo.transition(
                &second.terminal_delivery_key,
                second.state_revision,
                TerminalDeliveryState::Delivering,
                TransitionUpdate {
                    discord_message_id: Some("message-1".into()),
                    ..Default::default()
                },
                "LEASED"
            ),
            Err(TerminalDeliveryError::Storage(_))
        ));
        let events = repo.events(&first.terminal_delivery_key).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].previous_state, None);
        assert_eq!(events[0].new_state, TerminalDeliveryState::PendingResult);
        let conn = Connection::open(repo.database_path()).unwrap();
        assert!(conn
            .execute("DELETE FROM terminal_delivery_events", [])
            .is_err());
    }
    #[test]
    fn restart_digest_schema_and_corruption_fail_closed() {
        let (temp, repo) = repo();
        let record = repo.create_or_reuse(input()).unwrap();
        let path = repo.database_path().to_path_buf();
        drop(repo);
        assert_eq!(
            TerminalDeliveryRepository::open_path(&path)
                .unwrap()
                .get(&record.terminal_delivery_key)
                .unwrap()
                .unwrap(),
            record
        );
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "UPDATE terminal_deliveries SET terminal_payload_digest='bad'",
            [],
        )
        .unwrap();
        drop(conn);
        assert!(matches!(
            TerminalDeliveryRepository::open_path(&path),
            Err(TerminalDeliveryError::MalformedRecord(_))
        ));
        let version_path = temp.path().join("version.db");
        let conn = Connection::open(&version_path).unwrap();
        conn.pragma_update(None, "user_version", 99).unwrap();
        drop(conn);
        assert!(matches!(
            TerminalDeliveryRepository::open_path(version_path),
            Err(TerminalDeliveryError::SchemaVersion { found: 99 })
        ));
        let corrupt = temp.path().join("corrupt.db");
        fs::write(&corrupt, b"not a sqlite database").unwrap();
        assert!(matches!(
            TerminalDeliveryRepository::open_path(corrupt),
            Err(TerminalDeliveryError::Corrupt(_))
        ));
    }
    #[test]
    fn rejects_unsafe_audit_data() {
        let (_temp, repo) = repo();
        let record = repo.create_or_reuse(input()).unwrap();
        assert!(matches!(
            repo.transition(
                &record.terminal_delivery_key,
                0,
                TerminalDeliveryState::ReadyToDeliver,
                TransitionUpdate {
                    last_safe_error: Some(Some("Bearer secret".into())),
                    ..Default::default()
                },
                "TEST"
            ),
            Err(TerminalDeliveryError::UnsafeAuditField(_))
        ));
        assert!(matches!(
            repo.transition(
                &record.terminal_delivery_key,
                0,
                TerminalDeliveryState::ReadyToDeliver,
                TransitionUpdate::default(),
                "RAW runtime exception"
            ),
            Err(TerminalDeliveryError::UnsafeAuditField(_))
        ));
    }
    #[test]
    fn concurrent_insert_and_transition_have_one_winner() {
        let temp = TempDir::new().unwrap();
        let repo = Arc::new(TerminalDeliveryRepository::open(temp.path()).unwrap());
        let mut joins = Vec::new();
        for _ in 0..8 {
            let repo = Arc::clone(&repo);
            joins.push(thread::spawn(move || {
                repo.create_or_reuse(input()).unwrap()
            }));
        }
        let records: Vec<_> = joins.into_iter().map(|join| join.join().unwrap()).collect();
        assert!(records
            .iter()
            .all(|record| record.terminal_delivery_key == records[0].terminal_delivery_key));
        let record = records[0].clone();
        let mut joins = Vec::new();
        for _ in 0..8 {
            let repo = Arc::clone(&repo);
            let key = record.terminal_delivery_key.clone();
            joins.push(thread::spawn(move || {
                repo.transition(
                    &key,
                    0,
                    TerminalDeliveryState::ReadyToDeliver,
                    TransitionUpdate::default(),
                    "RACE",
                )
            }));
        }
        let results: Vec<_> = joins.into_iter().map(|join| join.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert!(results
            .iter()
            .filter(|result| result.is_err())
            .all(|result| matches!(result, Err(TerminalDeliveryError::RevisionConflict { .. }))));
    }
}
