use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::time::unix_millis;

use super::{AppState, admin::AdminError};

#[derive(Default, Deserialize)]
pub(super) struct Filter {
    session: Option<String>,
    cursor: Option<String>,
    since: Option<u64>,
    limit: Option<usize>,
}

fn since(filter: &Filter) -> u64 {
    filter
        .since
        .unwrap_or_else(|| unix_millis().saturating_sub(7 * 86_400_000))
}

fn unavailable(error: &anyhow::Error) -> AdminError {
    tracing::warn!(error = %error, "session log query failed");
    AdminError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "logs_unavailable",
        "Session logs are unavailable or the query exceeded its limits",
    )
}

pub(super) async fn status(
    State(state): State<Arc<AppState>>,
) -> Json<crate::session_log::LogStatus> {
    Json(state.session_log.status())
}

pub(super) async fn requests(
    State(state): State<Arc<AppState>>,
    Query(filter): Query<Filter>,
) -> Result<Json<crate::session_log::RequestPage>, AdminError> {
    if filter.cursor.as_ref().is_some_and(|s| {
        s.len() > 180
            || !s.split_once(':').is_some_and(|(time, id)| {
                time.parse::<i64>().is_ok_and(|time| time >= 0) && uuid::Uuid::parse_str(id).is_ok()
            })
    }) || filter.session.as_ref().is_some_and(|s| s.len() > 128)
    {
        return Err(AdminError::bad_request(
            "invalid_filter",
            "Invalid log filter",
        ));
    }
    let from = since(&filter);
    state
        .session_log
        .list(
            filter.session,
            filter.cursor,
            from,
            filter.limit.unwrap_or(50),
        )
        .await
        .map(Json)
        .map_err(|error| unavailable(&error))
}

pub(super) async fn request(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<crate::session_log::RequestDetail>, AdminError> {
    if uuid::Uuid::parse_str(&id).is_err() {
        return Err(AdminError::bad_request("invalid_id", "Invalid request ID"));
    }
    state
        .session_log
        .detail(id)
        .await
        .map_err(|error| unavailable(&error))?
        .map(Json)
        .ok_or_else(|| AdminError::not_found("log_not_found", "Request log not found"))
}

pub(super) async fn sessions(
    State(state): State<Arc<AppState>>,
    Query(filter): Query<Filter>,
) -> Result<Json<Value>, AdminError> {
    state
        .session_log
        .sessions(since(&filter))
        .await
        .map(|sessions| Json(json!({"sessions": sessions})))
        .map_err(|error| unavailable(&error))
}
