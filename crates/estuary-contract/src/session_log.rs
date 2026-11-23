use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RequestRecord {
    pub id: String,
    pub external_request_id: String,
    pub session_id: Option<String>,
    pub session_source: Option<String>,
    pub boot_id: String,
    #[serde(default)]
    pub process_id: u32,
    #[serde(default)]
    pub process_token: Option<String>,
    pub gateway_version: String,
    pub endpoint: String,
    pub protocol: String,
    pub model: Option<String>,
    pub client: Option<String>,
    pub streaming: bool,
    pub started_at_ms: u64,
    pub ended_at_ms: Option<u64>,
    pub http_status: Option<u16>,
    pub outcome: String,
    pub delivery: String,
    pub timings_us: BTreeMap<String, u64>,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub error_phase: Option<String>,
    pub error_class: Option<String>,
    #[cfg_attr(feature = "config-contract", ts(type = "unknown"))]
    pub usage: Value,
    pub attempts: Vec<AttemptRecord>,
    pub events: Vec<LogEvent>,
    pub capture_state: String,
}

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct AttemptRecord {
    pub number: usize,
    pub node: String,
    pub node_instance: u64,
    pub provider: String,
    pub endpoint: String,
    pub model: Option<String>,
    pub adapter: String,
    pub started_at_ms: u64,
    pub http_status: Option<u16>,
    pub upstream_request_id: Option<String>,
    pub outcome: String,
    pub error_class: Option<String>,
    pub retry_reason: Option<String>,
    pub timings_us: BTreeMap<String, u64>,
    #[cfg_attr(feature = "config-contract", ts(type = "Record<string, unknown>"))]
    pub route: Value,
    #[cfg_attr(feature = "config-contract", ts(type = "unknown"))]
    pub usage: Value,
}

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LogEvent {
    pub kind: String,
    pub elapsed_us: u64,
}

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Debug, Serialize)]
pub struct PayloadDetail {
    pub stage: String,
    pub attempt: usize,
    pub state: String,
    pub bytes_seen: usize,
    pub representation: String,
    #[cfg_attr(feature = "config-contract", ts(type = "unknown"))]
    pub content: Value,
}

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Debug, Serialize)]
pub struct RequestDetail {
    pub request: RequestRecord,
    pub payloads: Vec<PayloadDetail>,
}

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Debug, Serialize)]
pub struct RequestPage {
    pub requests: Vec<RequestRecord>,
    pub next_cursor: Option<String>,
}

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Debug, Serialize)]
pub struct SessionSummary {
    pub id: String,
    pub first_seen_at_ms: u64,
    pub last_seen_at_ms: u64,
    pub requests: u64,
    pub errors: u64,
}

#[cfg_attr(feature = "config-contract", derive(ts_rs::TS))]
#[derive(Clone, Debug, Default, Serialize)]
pub struct LogStatus {
    pub enabled: bool,
    pub capture_content: bool,
    pub available: bool,
    pub queued: usize,
    pub content_bytes: usize,
    pub dropped: u64,
    pub truncated: u64,
    pub write_errors: u64,
    pub committed: u64,
    pub last_commit_at_ms: u64,
}
