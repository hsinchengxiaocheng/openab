//! Durable OpenAB acceptance fence for AAP native `agent.work` dispatches.
//!
//! The control socket is the first point at which OpenAB accepts execution
//! responsibility.  Its process-local ledger is useful for waiter
//! coordination, but cannot survive a daemon restart.  This repository is
//! deliberately small: SQLite's `dispatch_id` PRIMARY KEY is the canonical
//! durable fence and `execution_id` is the stable OpenAB job identity returned
//! for every delivery of the same Runtime dispatch.

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, Error as SqlError, ErrorCode, OptionalExtension};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

pub const NATIVE_WORK_ACCEPTANCE_SCHEMA_VERSION: i32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeWorkAcceptance {
    pub dispatch_id: String,
    pub fingerprint: String,
    pub execution_id: String,
    pub accepted_at: DateTime<Utc>,
    pub acknowledgement: Option<String>,
    pub reused: bool,
}

#[derive(Debug)]
pub enum NativeWorkAcceptanceError {
    Storage(String),
    SchemaVersion { found: i32 },
    Conflict,
    InvalidInput(&'static str),
}

impl fmt::Display for NativeWorkAcceptanceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(detail) => write!(f, "native work acceptance storage failure: {detail}"),
            Self::SchemaVersion { found } => write!(
                f,
                "unsupported native work acceptance schema version {found}"
            ),
            Self::Conflict => {
                f.write_str("dispatch_id payload differs from its accepted execution")
            }
            Self::InvalidInput(field) => write!(f, "invalid native work acceptance input: {field}"),
        }
    }
}

impl std::error::Error for NativeWorkAcceptanceError {}

#[derive(Debug, Clone)]
pub struct NativeWorkAcceptanceRepository {
    database_path: PathBuf,
}

impl NativeWorkAcceptanceRepository {
    pub fn open_path(path: impl Into<PathBuf>) -> Result<Self, NativeWorkAcceptanceError> {
        let database_path = path.into();
        if let Some(parent) = database_path.parent() {
            fs::create_dir_all(parent).map_err(|error| storage(error.to_string()))?;
        }
        let repo = Self { database_path };
        let conn = repo.connect()?;
        initialize_and_validate(&conn)?;
        Ok(repo)
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    /// Atomically create or reuse the canonical execution identity for `dispatch_id`.
    ///
    /// `INSERT OR IGNORE` plus the `dispatch_id` PRIMARY KEY is intentional:
    /// distinct control-socket connections and post-restart replays resolve to
    /// this same row without relying on a process-local cache.
    pub fn claim_or_reuse(
        &self,
        dispatch_id: &str,
        fingerprint: &str,
    ) -> Result<NativeWorkAcceptance, NativeWorkAcceptanceError> {
        if dispatch_id.trim().is_empty() {
            return Err(NativeWorkAcceptanceError::InvalidInput("dispatch_id"));
        }
        if fingerprint.trim().is_empty() {
            return Err(NativeWorkAcceptanceError::InvalidInput("fingerprint"));
        }
        let execution_id = format!("openab-native:{dispatch_id}");
        let accepted_at = Utc::now();
        let conn = self.connect()?;
        let inserted = conn
            .execute(
            "INSERT OR IGNORE INTO native_work_acceptances (dispatch_id, fingerprint, execution_id, accepted_at) VALUES (?1, ?2, ?3, ?4)",
            params![dispatch_id, fingerprint, execution_id, accepted_at.to_rfc3339()],
        )
        .map_err(sql_error)?;
        let record = select(&conn, dispatch_id)?
            .ok_or_else(|| NativeWorkAcceptanceError::Storage("accepted row unavailable".into()))?;
        if record.fingerprint != fingerprint {
            return Err(NativeWorkAcceptanceError::Conflict);
        }
        Ok(NativeWorkAcceptance {
            reused: inserted == 0,
            ..record
        })
    }

    /// Persist the successful control-socket acknowledgement so a later
    /// delivery returns the identical OpenAB acceptance envelope.
    pub fn record_acknowledgement(
        &self,
        dispatch_id: &str,
        acknowledgement: &str,
    ) -> Result<(), NativeWorkAcceptanceError> {
        if acknowledgement.trim().is_empty() {
            return Err(NativeWorkAcceptanceError::InvalidInput("acknowledgement"));
        }
        let conn = self.connect()?;
        let changed = conn
            .execute(
                "UPDATE native_work_acceptances SET acknowledgement = CASE WHEN acknowledgement = '' THEN ?1 ELSE acknowledgement END WHERE dispatch_id = ?2",
                params![acknowledgement, dispatch_id],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(NativeWorkAcceptanceError::Storage(
                "accepted row unavailable".into(),
            ));
        }
        Ok(())
    }

    fn connect(&self) -> Result<Connection, NativeWorkAcceptanceError> {
        let conn = Connection::open(&self.database_path).map_err(sql_error)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(sql_error)?;
        Ok(conn)
    }
}

fn initialize_and_validate(conn: &Connection) -> Result<(), NativeWorkAcceptanceError> {
    let version: i32 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(sql_error)?;
    if version == 0 {
        conn.execute_batch(
            "CREATE TABLE native_work_acceptances (dispatch_id TEXT PRIMARY KEY NOT NULL, fingerprint TEXT NOT NULL, execution_id TEXT NOT NULL UNIQUE, accepted_at TEXT NOT NULL, acknowledgement TEXT NOT NULL DEFAULT ''); PRAGMA user_version = 1;",
        )
        .map_err(sql_error)?;
        return Ok(());
    }
    if version != NATIVE_WORK_ACCEPTANCE_SCHEMA_VERSION {
        return Err(NativeWorkAcceptanceError::SchemaVersion { found: version });
    }
    Ok(())
}

fn select(
    conn: &Connection,
    dispatch_id: &str,
) -> Result<Option<NativeWorkAcceptance>, NativeWorkAcceptanceError> {
    conn.query_row(
        "SELECT dispatch_id, fingerprint, execution_id, accepted_at, acknowledgement FROM native_work_acceptances WHERE dispatch_id = ?1",
        params![dispatch_id],
        |row| {
            let accepted_at: String = row.get(3)?;
            Ok(NativeWorkAcceptance {
                dispatch_id: row.get(0)?,
                fingerprint: row.get(1)?,
                execution_id: row.get(2)?,
                accepted_at: DateTime::parse_from_rfc3339(&accepted_at)
                    .map_err(|error| rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(error)))?
                    .with_timezone(&Utc),
                acknowledgement: {
                    let acknowledgement: String = row.get(4)?;
                    (!acknowledgement.is_empty()).then_some(acknowledgement)
                },
                reused: false,
            })
        },
    )
    .optional()
    .map_err(sql_error)
}

fn sql_error(error: SqlError) -> NativeWorkAcceptanceError {
    match error {
        SqlError::SqliteFailure(error, detail)
            if error.code == ErrorCode::DatabaseBusy || error.code == ErrorCode::DatabaseLocked =>
        {
            NativeWorkAcceptanceError::Storage(detail.unwrap_or_else(|| "database busy".into()))
        }
        other => NativeWorkAcceptanceError::Storage(other.to_string()),
    }
}

fn storage(detail: String) -> NativeWorkAcceptanceError {
    NativeWorkAcceptanceError::Storage(detail)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn sqlite_primary_key_reuses_one_execution_across_reopen_and_concurrency() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("native-work.sqlite");
        let first = NativeWorkAcceptanceRepository::open_path(&path).unwrap();
        let accepted = first.claim_or_reuse("dispatch-D", "payload-A").unwrap();
        assert!(!accepted.reused);
        let reopened = NativeWorkAcceptanceRepository::open_path(&path).unwrap();
        let replay = reopened.claim_or_reuse("dispatch-D", "payload-A").unwrap();
        assert!(replay.reused);
        assert_eq!(replay.execution_id, accepted.execution_id);

        let repository = Arc::new(reopened);
        let mut threads = Vec::new();
        for _ in 0..8 {
            let repository = repository.clone();
            threads.push(std::thread::spawn(move || {
                repository
                    .claim_or_reuse("dispatch-concurrent", "payload-A")
                    .unwrap()
            }));
        }
        let ids: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap().execution_id)
            .collect();
        assert!(ids.iter().all(|id| id == &ids[0]));
    }

    #[test]
    fn same_dispatch_with_changed_payload_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let repository =
            NativeWorkAcceptanceRepository::open_path(dir.path().join("native-work.sqlite"))
                .unwrap();
        repository
            .claim_or_reuse("dispatch-D", "payload-A")
            .unwrap();
        assert!(matches!(
            repository.claim_or_reuse("dispatch-D", "payload-B"),
            Err(NativeWorkAcceptanceError::Conflict)
        ));
    }
}
