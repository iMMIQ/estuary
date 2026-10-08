use super::{
    capture::CapturedPayload,
    content, inspect,
    model::{PayloadDetail, RequestDetail, RequestPage, RequestRecord, SessionSummary},
    unix_ms,
};
use crate::config::SessionLogConfig;
use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde_json::Value;
use std::{path::Path, time::Duration};

const APPLICATION_ID: i64 = 0x4553_4c47;

pub(super) fn open(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
        {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    let mut connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_millis(250))?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    let transaction =
        connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let id: i64 = transaction.pragma_query_value(None, "application_id", |r| r.get(0))?;
    let version: i64 = transaction.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if id == 0 {
        let tables: i64 = transaction.query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table'",
            [],
            |r| r.get(0),
        )?;
        if tables != 0 {
            bail!("session log database must be separate and empty");
        }
        transaction.execute_batch(include_str!("schema.sql"))?;
        transaction.pragma_update(None, "application_id", APPLICATION_ID)?;
        transaction.pragma_update(None, "user_version", 1)?;
    } else if id != APPLICATION_ID || version != 1 {
        bail!("unsupported session log database schema");
    }
    transaction.commit()?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "NORMAL")?;
    connection.pragma_update(None, "wal_autocheckpoint", 1000)?;
    connection.pragma_update(None, "journal_size_limit", 16 * 1024 * 1024)?;
    Ok(connection)
}

pub(super) fn write_record(connection: &Connection, record: &RequestRecord) -> Result<()> {
    if let Some(session) = &record.session_id {
        connection.execute(
            "INSERT OR IGNORE INTO sessions(id,source) VALUES(?1,?2)",
            params![session, record.session_source],
        )?;
    }
    let mut metadata = record.clone();
    metadata.attempts.clear();
    metadata.events.clear();
    let mut value = serde_json::to_value(metadata)?;
    inspect::redact(&mut value);
    // A late Start must never overwrite a finalized record.
    connection.execute(
        "INSERT INTO requests(id,session_id,started_at_ms,ended_at_ms,model,outcome,data) VALUES(?1,?2,?3,?4,?5,?6,?7)
         ON CONFLICT(id) DO UPDATE SET session_id=excluded.session_id,ended_at_ms=excluded.ended_at_ms,model=excluded.model,outcome=excluded.outcome,data=excluded.data
         WHERE requests.ended_at_ms IS NULL AND excluded.ended_at_ms IS NOT NULL",
        params![record.id,record.session_id,record.started_at_ms,record.ended_at_ms,record.model,record.outcome,serde_json::to_string(&value)?],
    )?;
    for attempt in &record.attempts {
        connection.execute(
            "INSERT OR REPLACE INTO attempts(request_id,number,node,data) VALUES(?1,?2,?3,?4)",
            params![
                record.id,
                attempt.number,
                attempt.node,
                redacted_json(attempt)?
            ],
        )?;
    }
    for (index, event) in record.events.iter().enumerate() {
        connection.execute(
            "INSERT OR REPLACE INTO request_events(request_id,sequence,data) VALUES(?1,?2,?3)",
            params![record.id, index, serde_json::to_string(event)?],
        )?;
    }
    Ok(())
}

pub(super) fn write_payloads(
    connection: &Connection,
    record: &mut RequestRecord,
    payloads: &[CapturedPayload],
    cache: &mut content::Cache,
) -> Result<()> {
    for payload in payloads {
        if payload.bytes.is_empty() {
            continue;
        }
        let (content, usage, representation) =
            inspect::prepare(&payload.bytes, payload.streaming, payload.anthropic);
        if payload.stage == "upstream_output"
            && !usage.is_null()
            && let Some(attempt) = record
                .attempts
                .iter_mut()
                .find(|a| a.number == payload.attempt)
        {
            if !attempt.usage.is_object() {
                attempt.usage = serde_json::json!({});
            }
            inspect::merge_usage(&mut attempt.usage, &usage);
        }
        let root = content::put(connection, &content, cache)?;
        let state = if payload.state == "partial" {
            "partial"
        } else if representation == "stream_summary" {
            "summary"
        } else if representation == "partial_text" {
            "unparsed"
        } else {
            "complete"
        };
        if connection.execute("INSERT OR IGNORE INTO payloads(request_id,stage,attempt,root_hash,state,bytes_seen,representation,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![record.id,payload.stage,payload.attempt,root,state,payload.bytes_seen,representation,unix_ms()])?==1 {
            connection.execute("UPDATE content_blobs SET refs=refs+1 WHERE hash=?1",[root])?;
        }
    }
    record.usage = record
        .attempts
        .last()
        .map_or(Value::Null, |attempt| attempt.usage.clone());
    Ok(())
}

pub(super) fn maintain(connection: &mut Connection, config: &SessionLogConfig) -> Result<()> {
    let transaction = connection.transaction()?;
    let now = unix_ms();
    let day = 86_400_000u64;
    transaction.execute("DELETE FROM requests WHERE id IN (SELECT id FROM requests WHERE started_at_ms<?1 LIMIT 1000)",[now.saturating_sub(u64::from(config.retention_days)*day)])?;
    transaction.execute("DELETE FROM payloads WHERE id IN (SELECT id FROM payloads WHERE created_at_ms<?1 LIMIT 1000)",[now.saturating_sub(u64::from(config.content_retention_days)*day)])?;
    transaction.execute("DELETE FROM sessions WHERE id IN (SELECT id FROM sessions WHERE NOT EXISTS(SELECT 1 FROM requests WHERE session_id=sessions.id) LIMIT 1000)",[])?;
    content::collect(&transaction)?;
    transaction.commit()?;
    Ok(())
}

fn reader(path: &Path) -> Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    connection.busy_timeout(Duration::from_millis(250))?;
    let id: i64 = connection.pragma_query_value(None, "application_id", |r| r.get(0))?;
    let version: i64 = connection.pragma_query_value(None, "user_version", |r| r.get(0))?;
    if id != APPLICATION_ID || version != 1 {
        bail!("unsupported session log database schema");
    }
    let started = std::time::Instant::now();
    connection.progress_handler(
        1000,
        Some(move || started.elapsed() > Duration::from_secs(2)),
    );
    Ok(connection)
}

pub(super) fn list(
    path: &Path,
    session: Option<&str>,
    cursor: Option<&str>,
    since: u64,
    limit: usize,
) -> Result<RequestPage> {
    let connection = reader(path)?;
    let (time, id) = if let Some(cursor) = cursor {
        let (time, id) = cursor.split_once(':').context("invalid request cursor")?;
        (time.parse::<u64>()?, id.to_owned())
    } else {
        (i64::MAX as u64, String::new())
    };
    let mut statement=connection.prepare("SELECT data FROM requests WHERE started_at_ms>=?1 AND (?2 IS NULL OR session_id=?2) AND (started_at_ms<?3 OR (started_at_ms=?3 AND id<?4)) ORDER BY started_at_ms DESC,id DESC LIMIT ?5")?;
    let rows = statement.query_map(params![since, session, time, id, limit + 1], |r| {
        r.get::<_, String>(0)
    })?;
    let mut requests = rows
        .map(|row| Ok(serde_json::from_str::<RequestRecord>(&row?)?))
        .collect::<Result<Vec<_>>>()?;
    let more = requests.len() > limit;
    requests.truncate(limit);
    let next_cursor = if more {
        requests
            .last()
            .map(|r| format!("{}:{}", r.started_at_ms, r.id))
    } else {
        None
    };
    Ok(RequestPage {
        requests,
        next_cursor,
    })
}

pub(super) fn detail(path: &Path, id: &str) -> Result<Option<RequestDetail>> {
    let mut connection = reader(path)?;
    let transaction = connection.transaction()?;
    let data: Option<String> = transaction
        .query_row("SELECT data FROM requests WHERE id=?1", [id], |r| r.get(0))
        .optional()?;
    let Some(data) = data else {
        return Ok(None);
    };
    let mut request: RequestRecord = serde_json::from_str(&data)?;
    let mut attempts =
        transaction.prepare("SELECT data FROM attempts WHERE request_id=?1 ORDER BY number")?;
    for row in attempts.query_map([id], |r| r.get::<_, String>(0))? {
        request.attempts.push(serde_json::from_str(&row?)?);
    }
    let mut events = transaction
        .prepare("SELECT data FROM request_events WHERE request_id=?1 ORDER BY sequence")?;
    for row in events.query_map([id], |r| r.get::<_, String>(0))? {
        request.events.push(serde_json::from_str(&row?)?);
    }
    let mut statement=transaction.prepare("SELECT stage,attempt,root_hash,state,bytes_seen,representation FROM payloads WHERE request_id=?1 ORDER BY attempt,stage")?;
    let mut payloads = Vec::new();
    let mut budget = content::ReadBudget {
        bytes: 32 * 1024 * 1024,
        nodes: 100_000,
        deadline: std::time::Instant::now() + Duration::from_secs(2),
    };
    for row in statement.query_map([id], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, usize>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, usize>(4)?,
            r.get::<_, String>(5)?,
        ))
    })? {
        let (stage, attempt, hash, capture_state, bytes_seen, representation) = row?;
        payloads.push(PayloadDetail {
            stage,
            attempt,
            state: capture_state,
            bytes_seen,
            representation,
            content: content::get(&transaction, &hash, &mut budget, 0)?,
        });
    }
    if payloads.is_empty() && request.capture_state == "captured" {
        "expired".clone_into(&mut request.capture_state);
    }
    Ok(Some(RequestDetail { request, payloads }))
}

pub(super) fn sessions(path: &Path, since: u64) -> Result<Vec<SessionSummary>> {
    let connection = reader(path)?;
    let mut statement=connection.prepare("SELECT session_id,min(started_at_ms),max(started_at_ms),count(*),sum(outcome='error') FROM requests WHERE session_id IS NOT NULL AND started_at_ms>=?1 GROUP BY session_id ORDER BY max(started_at_ms) DESC LIMIT 100")?;
    Ok(statement
        .query_map([since], |r| {
            Ok(SessionSummary {
                id: r.get(0)?,
                first_seen_at_ms: r.get(1)?,
                last_seen_at_ms: r.get(2)?,
                requests: r.get(3)?,
                errors: r.get(4)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?)
}

pub(super) fn recover(connection: &Connection) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        let mut statement = connection.prepare("SELECT DISTINCT json_extract(data,'$.boot_id'), json_extract(data,'$.process_id'), json_extract(data,'$.process_token') FROM requests WHERE ended_at_ms IS NULL AND outcome='started'")?;
        for row in statement.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<u32>>(1)?,
                r.get::<_, Option<String>>(2)?,
            ))
        })? {
            let (boot, pid, token) = row?;
            if let (Some(pid), Some(token)) = (pid, token) {
                let current = super::process_token(pid);
                if current.as_ref().is_some_and(|value| value != &token)
                    || (current.is_none()
                        && !std::path::Path::new(&format!("/proc/{pid}")).exists())
                {
                    interrupt_boot(connection, &boot)?;
                }
            }
        }
    }
    Ok(())
}

pub(super) fn interrupt_boot(connection: &Connection, boot: &str) -> Result<()> {
    connection.execute("UPDATE requests SET outcome='interrupted',data=json_set(data,'$.outcome','interrupted','$.delivery','unknown') WHERE ended_at_ms IS NULL AND outcome='started' AND json_extract(data,'$.boot_id')=?1",[boot])?;
    Ok(())
}

pub(super) fn redacted_json(value: &impl serde::Serialize) -> Result<String> {
    let mut value = serde_json::to_value(value)?;
    inspect::redact(&mut value);
    Ok(serde_json::to_string(&value)?)
}
