use super::{LogSink, content, inspect, model::RequestRecord, store};
use crate::config::SessionLogConfig;
use axum::http::HeaderMap;
use rusqlite::params;
use serde_json::{Value, json};
use std::{path::PathBuf, time::Duration};

struct Database(PathBuf);
impl Database {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("estuary-log-unit-{}.sqlite", uuid::Uuid::now_v7())))
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}
fn read(connection: &rusqlite::Connection, hash: &str) -> Value {
    content::get(
        connection,
        hash,
        &mut content::ReadBudget {
            bytes: 32 * 1024 * 1024,
            nodes: 100_000,
            deadline: std::time::Instant::now() + Duration::from_secs(2),
        },
        0,
    )
    .unwrap()
}
fn payload(
    connection: &rusqlite::Connection,
    id: &str,
    value: &Value,
    time: u64,
    cache: &mut content::Cache,
) {
    let hash = content::put(connection, value, cache).unwrap();
    connection.execute("INSERT INTO payloads(request_id,stage,attempt,root_hash,state,bytes_seen,representation,created_at_ms) VALUES(?1,'client_input',0,?2,'complete',0,'json',?3)",params![id,hash,time]).unwrap();
    connection
        .execute("UPDATE content_blobs SET refs=refs+1 WHERE hash=?1", [hash])
        .unwrap();
}

#[test]
fn retention_preserves_shared_prefixes_and_reclaims_only_unreferenced_content() {
    let db = Database::new();
    let mut connection = store::open(&db.0).unwrap();
    let now = super::unix_ms();
    let old = RequestRecord {
        id: "old".to_owned(),
        started_at_ms: 1,
        ..RequestRecord::default()
    };
    let new = RequestRecord {
        id: "new".to_owned(),
        started_at_ms: now,
        capture_state: "captured".to_owned(),
        ..RequestRecord::default()
    };
    let a = json!({"messages":[{"content":"shared"},{"content":"a"}],"unknown":{"opaque":"signed-data"}});
    let b = json!({"messages":[{"content":"shared"},{"content":"a"},{"content":"b"}],"unknown":{"opaque":"signed-data"}});
    {
        let transaction = connection.transaction().unwrap();
        store::write_record(&transaction, &old).unwrap();
        store::write_record(&transaction, &new).unwrap();
        let mut cache = content::Cache::new();
        payload(&transaction, "old", &a, 1, &mut cache);
        payload(&transaction, "new", &b, now, &mut cache);
        transaction.commit().unwrap();
    }
    store::maintain(&mut connection, &SessionLogConfig::default()).unwrap();
    assert!(store::detail(&db.0, "old").unwrap().is_none());
    assert_eq!(
        store::detail(&db.0, "new").unwrap().unwrap().payloads[0].content,
        b
    );
    connection
        .execute("UPDATE payloads SET created_at_ms=1", [])
        .unwrap();
    store::maintain(&mut connection, &SessionLogConfig::default()).unwrap();
    assert_eq!(
        store::detail(&db.0, "new")
            .unwrap()
            .unwrap()
            .request
            .capture_state,
        "expired"
    );
    for _ in 0..20 {
        content::collect(&connection).unwrap();
    }
    for table in ["content_blobs", "sequence_nodes"] {
        let count: u64 = connection
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "orphaned {table}");
    }
    assert!(
        connection
            .prepare("PRAGMA foreign_key_check")
            .unwrap()
            .query([])
            .unwrap()
            .next()
            .unwrap()
            .is_none()
    );
}

#[test]
fn branches_compaction_and_unknown_fields_reconstruct_exactly() {
    let db = Database::new();
    let mut connection = store::open(&db.0).unwrap();
    let transaction = connection.transaction().unwrap();
    for items in [
        json!([{"role":"system","content":"same"},{"tool_call_id":"a","content":"result"}]),
        json!([{"role":"system","content":"same"},{"tool_call_id":"b","content":"result"}]),
        json!([{"role":"system","content":"compressed summary"}]),
    ] {
        let value = json!({"input":items,"previous_response_id":"r","opaque":["signed",null,{},[]],"unknown":{"kind":"Array","value":"not an internal reference"}});
        let hash = content::put(&transaction, &value, &mut content::Cache::new()).unwrap();
        assert_eq!(read(&transaction, &hash), value);
    }
}

#[test]
fn recovery_distinguishes_dead_boots_from_overlapping_live_workers() {
    let db = Database::new();
    let connection = store::open(&db.0).unwrap();
    let live = RequestRecord {
        id: "live".to_owned(),
        boot_id: "live-boot".to_owned(),
        process_id: std::process::id(),
        process_token: super::process_token(std::process::id()),
        outcome: "started".to_owned(),
        ..RequestRecord::default()
    };
    let dead = RequestRecord {
        id: "dead".to_owned(),
        boot_id: "dead-boot".to_owned(),
        process_id: u32::MAX,
        process_token: Some("dead".to_owned()),
        outcome: "started".to_owned(),
        ..RequestRecord::default()
    };
    store::write_record(&connection, &live).unwrap();
    store::write_record(&connection, &dead).unwrap();
    store::recover(&connection).unwrap();
    assert_eq!(
        store::detail(&db.0, "live")
            .unwrap()
            .unwrap()
            .request
            .outcome,
        "started"
    );
    #[cfg(target_os = "linux")]
    assert_eq!(
        store::detail(&db.0, "dead")
            .unwrap()
            .unwrap()
            .request
            .outcome,
        "interrupted"
    );
}

#[test]
fn redaction_and_anthropic_usage_preserve_unknown_values() {
    let source = json!({"api_key":"secret-value", "messages":[{"content":"Bearer abcdefghijk and sk-abcdefghijkl"}],"opaque":"signed-data","usage":{"input_tokens":4,"output_tokens":2,"cache_read_input_tokens":5,"cache_creation_input_tokens":6}});
    let (value, usage, _) = inspect::prepare(&serde_json::to_vec(&source).unwrap(), false, true);
    assert_eq!(value["api_key"], "[REDACTED]");
    assert_eq!(value["messages"][0]["content"], "[REDACTED] and [REDACTED]");
    assert_eq!(value["opaque"], "signed-data");
    assert_eq!(usage["input_tokens"], 15);
    assert_eq!(usage["cache_write_tokens"], 6);
    let (stream, usage, _) = inspect::prepare(b"data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":4,\"cache_read_input_tokens\":5}}}\r\n\r\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":2}}\r\n\r\ndata: {\"type\":\"message_stop\"}\r\n\r\n", true, true);
    assert_eq!(usage["input_tokens"], 9);
    assert_eq!(usage["output_tokens"], 2);
    assert_eq!(stream["terminal_marker_seen"], true);
}

#[test]
fn truncated_json_and_embedded_tool_arguments_mask_credential_fields() {
    for source in [
        r#"{"api_key":"secret-plain"#,
        r#"tool output: {"Password": "secret-plain", "content":"hello"}"#,
    ] {
        let (value, _, _) = inspect::prepare(source.as_bytes(), false, false);
        assert!(!value.to_string().contains("secret-plain"));
        assert!(value.to_string().contains("[REDACTED]"));
    }
}

#[tokio::test]
async fn bounded_capture_and_full_queue_leave_finalization_capacity() {
    let db = Database::new();
    let sink = LogSink::new(SessionLogConfig {
        database: Some(db.0.clone()),
        queue_capacity: 4,
        max_payload_bytes: 32,
        max_content_bytes: 64,
        ..SessionLogConfig::default()
    });
    assert!(sink.flush(Duration::from_secs(3)).await);
    let mut observations = Vec::new();
    for _ in 0..10 {
        if let Some(log) = sink.begin("/v1/chat/completions", &HeaderMap::new(), "same") {
            log.request_body(&[b'x'; 100], 0);
            observations.push(log);
            assert!(sink.flush(Duration::from_secs(3)).await);
        }
    }
    assert!(sink.status().dropped > 0);
    assert!(sink.status().content_bytes <= 64);
    let accepted = observations.len();
    for log in observations {
        log.downstream_done(true);
    }
    assert!(sink.flush(Duration::from_secs(3)).await);
    assert_eq!(sink.status().content_bytes, 0);
    assert_eq!(sink.status().committed, accepted as u64);
    let records = sink.list(None, None, 0, 100).await.unwrap();
    assert_eq!(records.requests.len(), accepted);
    assert!(
        records
            .requests
            .iter()
            .all(|r| r.capture_state == "partial" && r.outcome == "success")
    );
}

#[tokio::test]
async fn transient_writer_contention_keeps_content_and_logger_available() {
    let db = Database::new();
    let sink = LogSink::new(SessionLogConfig {
        database: Some(db.0.clone()),
        ..SessionLogConfig::default()
    });
    assert!(sink.flush(Duration::from_secs(3)).await);
    let (ready, locked) = std::sync::mpsc::channel();
    let path = db.0.clone();
    let competing = std::thread::spawn(move || {
        let mut connection = store::open(&path).unwrap();
        let tx = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .unwrap();
        ready.send(()).unwrap();
        // Longer than the configured 250 ms busy timeout, within the writer's
        // bounded retry budget. No request task waits on the SQLite lock.
        std::thread::sleep(Duration::from_millis(350));
        tx.commit().unwrap();
    });
    locked.recv_timeout(Duration::from_secs(3)).unwrap();
    let log = sink
        .begin("/v1/chat/completions", &HeaderMap::new(), "locked")
        .unwrap();
    log.request_body(
        br#"{"model":"m","messages":[{"role":"user","content":"retained"}]}"#,
        0,
    );
    log.headers(200, false);
    log.downstream_done(true);
    assert!(sink.flush(Duration::from_secs(3)).await);
    competing.join().unwrap();
    let status = sink.status();
    assert!(status.available);
    assert_eq!(status.write_errors, 0);
    assert_eq!(status.dropped, 0);
    let row = sink
        .list(None, None, 0, 10)
        .await
        .unwrap()
        .requests
        .remove(0);
    let detail = sink.detail(row.id).await.unwrap().unwrap();
    assert_eq!(detail.request.capture_state, "captured");
    assert_eq!(
        detail.payloads[0].content["messages"][0]["content"],
        "retained"
    );
}

#[tokio::test]
async fn overlapping_writers_share_content_without_losing_or_overwriting_records() {
    let db = Database::new();
    let config = SessionLogConfig {
        database: Some(db.0.clone()),
        ..SessionLogConfig::default()
    };
    let first = LogSink::new(config.clone());
    assert!(first.flush(Duration::from_secs(3)).await);
    let second = LogSink::new(config);
    assert!(second.flush(Duration::from_secs(3)).await);
    let mut workers = Vec::new();
    for sink in [&first, &second] {
        let sink = std::sync::Arc::clone(sink);
        workers.push(tokio::spawn(async move {
            for i in 0..50 {
                let log = sink.begin("/v1/chat/completions", &HeaderMap::new(), "reused").unwrap();
                log.request_body(&serde_json::to_vec(&json!({"model":"m","messages":[{"role":"system","content":"shared".repeat(100)},{"role":"user","content":format!("turn {i}")}]})).unwrap(),0);
                log.headers(200,false);
                log.downstream_done(true);
            }
        }));
    }
    for worker in workers {
        worker.await.unwrap();
    }
    assert!(first.flush(Duration::from_secs(5)).await);
    assert!(second.flush(Duration::from_secs(5)).await);
    let rows = first.list(None, None, 0, 100).await.unwrap();
    assert_eq!(rows.requests.len(), 100);
    assert!(rows.requests.iter().all(|r| r.outcome == "success"));
    assert_eq!(first.status().committed, 50);
    assert_eq!(second.status().committed, 50);
    assert_eq!(first.status().write_errors, 0);
    assert_eq!(second.status().write_errors, 0);
    for row in rows.requests {
        let detail = first.detail(row.id).await.unwrap().unwrap();
        assert!(
            detail.payloads[0].content["messages"][1]["content"]
                .as_str()
                .unwrap()
                .starts_with("turn ")
        );
    }
}

#[test]
#[ignore = "1000-turn content storage scale test"]
fn thousand_turn_history_shares_prefixes_and_restores_the_final_request() {
    let db = Database::new();
    let mut connection = store::open(&db.0).unwrap();
    let started = std::time::Instant::now();
    let transaction = connection.transaction().unwrap();
    let mut messages = vec![json!({"role":"system","content":"shared instructions".repeat(1000)})];
    let mut logical_bytes = 0usize;
    let mut cache = content::Cache::new();
    for i in 0..1000 {
        messages.push(json!({"role":"user","content":format!("turn {i}")}));
        let value = json!({"messages":messages,"model":"m"});
        logical_bytes += serde_json::to_vec(&value).unwrap().len();
        let record = RequestRecord {
            id: format!("request-{i}"),
            started_at_ms: super::unix_ms(),
            ..RequestRecord::default()
        };
        store::write_record(&transaction, &record).unwrap();
        payload(
            &transaction,
            &record.id,
            &value,
            super::unix_ms(),
            &mut cache,
        );
    }
    transaction.commit().unwrap();
    assert_eq!(
        store::detail(&db.0, "request-999")
            .unwrap()
            .unwrap()
            .payloads[0]
            .content["messages"],
        Value::Array(messages)
    );
    let nodes: u64 = connection
        .query_row("SELECT count(*) FROM sequence_nodes", [], |r| r.get(0))
        .unwrap();
    let stored: u64 = connection
        .query_row("SELECT sum(stored_bytes) FROM content_blobs", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(nodes, 1001);
    assert!(stored < 1024 * 1024);
    eprintln!(
        "1000 turns: logical JSON {logical_bytes} bytes; compressed blobs {stored} bytes; {nodes} sequence nodes; elapsed {:?}",
        started.elapsed()
    );
}
