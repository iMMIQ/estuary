use super::{Command, Stats, store, unix_ms};
use crate::config::SessionLogConfig;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::sync::mpsc;
use tracing::warn;

pub(super) fn run(
    mut receiver: mpsc::Receiver<Command>,
    config: &SessionLogConfig,
    stats: &Arc<Stats>,
    boot: &str,
) {
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
    else {
        return;
    };
    runtime.block_on(async {
        let path = config.database.as_ref().expect("enabled writer");
        let mut connection = None;
        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;
        let mut tick = tokio::time::interval(Duration::from_millis(config.flush_interval_ms));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut maintenance_at = std::time::Instant::now();
        let mut retry_at = std::time::Instant::now();
        loop {
            if connection.is_none() && std::time::Instant::now() >= retry_at {
                match store::open(path) {
                    Ok(db) => {
                        if let Err(error) = store::recover(&db) {
                            stats.write_errors.fetch_add(1, Ordering::Relaxed);
                            warn!(error = %error, "session log recovery failed");
                        }
                        connection = Some(db);
                        stats.available.store(true, Ordering::Relaxed);
                    }
                    Err(error) => {
                        stats.write_errors.fetch_add(1, Ordering::Relaxed);
                        warn!(error = %error, "session log unavailable");
                        retry_at = std::time::Instant::now() + Duration::from_secs(5);
                    }
                }
            }
            let command = tokio::select! {
                command = receiver.recv() => command,
                _ = tick.tick() => {
                    flush(&mut connection, &mut batch, stats);
                    batch_bytes = 0;
                    if maintenance_at.elapsed() >= Duration::from_secs(60) {
                        if let Some(db) = &mut connection
                            && let Err(error) = store::maintain(db, config) {
                            stats.write_errors.fetch_add(1, Ordering::Relaxed);
                            warn!(error = %error, "session log retention failed");
                        }
                        maintenance_at = std::time::Instant::now();
                    }
                    continue;
                }
            };
            match command {
                Some(Command::Flush(reply)) => {
                    flush(&mut connection, &mut batch, stats);
                    batch_bytes = 0;
                    let _ =
                        reply.send(connection.is_some() && stats.available.load(Ordering::Relaxed));
                }
                Some(command) => {
                    stats.queued.fetch_sub(1, Ordering::Relaxed);
                    if let Command::Finish(_, payloads) = &command {
                        batch_bytes += payloads.iter().map(|p| p.bytes.len()).sum::<usize>();
                    }
                    batch.push(command);
                    if batch.len() >= 100 || batch_bytes >= 1024 * 1024 {
                        flush(&mut connection, &mut batch, stats);
                        batch_bytes = 0;
                    }
                }
                None => {
                    flush(&mut connection, &mut batch, stats);
                    if let Some(db) = &connection {
                        let _ = store::interrupt_boot(db, boot);
                    }
                    break;
                }
            }
        }
    });
}

fn flush(connection: &mut Option<rusqlite::Connection>, batch: &mut Vec<Command>, stats: &Stats) {
    if batch.is_empty() {
        return;
    }
    let Some(db) = connection else {
        stats
            .dropped
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        batch.clear();
        return;
    };
    let mut result = write_batch(db, batch);
    // WAL writers serialize. Give a short-lived competing batch a bounded
    // chance to commit before dropping content or declaring the logger failed.
    for _ in 1..4 {
        let busy = result.as_ref().err().is_some_and(|error| {
            matches!(error.downcast_ref::<rusqlite::Error>(),
                Some(rusqlite::Error::SqliteFailure(code, _))
                if matches!(code.code, rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked))
        });
        if !busy {
            break;
        }
        result = write_batch(db, batch);
    }
    if let Err(error) = result {
        stats.write_errors.fetch_add(1, Ordering::Relaxed);
        stats.available.store(false, Ordering::Relaxed);
        warn!(error=%error,"session log batch failed");
        // Metadata remains useful when content encoding or storage fails.
        let fallback = (|| -> anyhow::Result<()> {
            let tx = db.transaction()?;
            for command in batch.iter_mut() {
                match command {
                    Command::Start(record) => store::write_record(&tx, record)?,
                    Command::Finish(record, _) => {
                        "storage_error".clone_into(&mut record.capture_state);
                        store::write_record(&tx, record)?;
                    }
                    Command::Flush(_) => {}
                }
            }
            tx.commit()?;
            Ok(())
        })();
        if fallback.is_err() {
            stats
                .dropped
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
        } else {
            record_committed(batch, stats);
        }
    } else {
        stats.available.store(true, Ordering::Relaxed);
        record_committed(batch, stats);
    }
    batch.clear();
}

fn write_batch(db: &mut rusqlite::Connection, batch: &mut [Command]) -> anyhow::Result<()> {
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let mut cache = super::content::Cache::new();
    for command in batch.iter_mut() {
        match command {
            Command::Start(record) => store::write_record(&tx, record)?,
            Command::Finish(record, payloads) => {
                // Request exists before FK-bound payload rows; final metadata
                // is written again after usage extraction.
                store::write_record(&tx, record)?;
                store::write_payloads(&tx, record, payloads, &mut cache)?;
                let mut metadata = record.clone();
                metadata.attempts.clear();
                metadata.events.clear();
                let mut value = serde_json::to_value(metadata)?;
                super::inspect::redact(&mut value);
                tx.execute(
                    "UPDATE requests SET data=?1 WHERE id=?2",
                    rusqlite::params![serde_json::to_string(&value)?, record.id],
                )?;
                for attempt in &record.attempts {
                    tx.execute(
                        "UPDATE attempts SET data=?1 WHERE request_id=?2 AND number=?3",
                        rusqlite::params![
                            store::redacted_json(attempt)?,
                            record.id,
                            attempt.number
                        ],
                    )?;
                }
            }
            Command::Flush(_) => unreachable!(),
        }
    }
    tx.commit()?;
    Ok(())
}

fn record_committed(batch: &[Command], stats: &Stats) {
    stats.committed.fetch_add(
        batch
            .iter()
            .filter(|c| matches!(c, Command::Finish(..)))
            .count() as u64,
        Ordering::Relaxed,
    );
    stats.last_commit_at_ms.store(unix_ms(), Ordering::Relaxed);
}
