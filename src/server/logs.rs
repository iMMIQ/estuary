use super::AppState;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Default, Deserialize)]
pub(super) struct Filter {
    session: Option<String>,
    cursor: Option<String>,
    since: Option<u64>,
    limit: Option<usize>,
}

fn since(filter: &Filter) -> u64 {
    filter.since.unwrap_or_else(|| {
        u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX)
        .saturating_sub(7 * 86_400_000)
    })
}
fn failure(error: &anyhow::Error) -> (StatusCode, Json<Value>) {
    tracing::warn!(error=%error,"session log query failed");
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(
            json!({"error":{"code":"logs_unavailable","message":"Session logs are unavailable or the query exceeded its limits"}}),
        ),
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
) -> Result<Json<crate::session_log::RequestPage>, (StatusCode, Json<Value>)> {
    if filter.cursor.as_ref().is_some_and(|s| {
        s.len() > 180
            || !s.split_once(':').is_some_and(|(time, id)| {
                time.parse::<i64>().is_ok_and(|time| time >= 0) && uuid::Uuid::parse_str(id).is_ok()
            })
    }) || filter.session.as_ref().is_some_and(|s| s.len() > 128)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":{"code":"invalid_filter","message":"Invalid log filter"}})),
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
        .map_err(|e| failure(&e))
}

pub(super) async fn request(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<crate::session_log::RequestDetail>, (StatusCode, Json<Value>)> {
    if uuid::Uuid::parse_str(&id).is_err() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":{"code":"invalid_id","message":"Invalid request ID"}})),
        ));
    }
    state
        .session_log
        .detail(id)
        .await
        .map_err(|e| failure(&e))?
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(json!({"error":{"code":"log_not_found","message":"Request log not found"}})),
            )
        })
}

pub(super) async fn sessions(
    State(state): State<Arc<AppState>>,
    Query(filter): Query<Filter>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    state
        .session_log
        .sessions(since(&filter))
        .await
        .map(|sessions| Json(json!({"sessions":sessions})))
        .map_err(|e| failure(&e))
}
