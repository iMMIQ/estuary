use std::{
    sync::{Arc, atomic::Ordering as AtomicOrdering},
    time::Duration,
};

use anyhow::Result;
use tokio::sync::watch;
use tracing::warn;

use super::AppState;
use super::admin::prepare_node;

pub(super) async fn run_control_reconciler(
    state: Arc<AppState>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(
        state.settings.server.control_sync_interval_ms,
    ));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut failure_backoff = Duration::ZERO;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = interval.tick() => {
                if let Err(error) = reconcile_control_plane(&state).await {
                    warn!(error = %error, "failed to reconcile shared node configuration");
                    failure_backoff = if failure_backoff.is_zero() {
                        Duration::from_secs(1)
                    } else {
                        failure_backoff.saturating_mul(2).min(Duration::from_secs(30))
                    };
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                break;
                            }
                        }
                        () = tokio::time::sleep(failure_backoff) => {}
                    }
                } else {
                    failure_backoff = Duration::ZERO;
                }
            }
        }
    }
}

pub(super) async fn reconcile_control_plane(state: &Arc<AppState>) -> Result<()> {
    let observed = state.store.revision_async().await?;
    if observed == state.control_revision.load(AtomicOrdering::Acquire) {
        return Ok(());
    }

    let _mutation = state.admin_mutation.lock().await;
    let before = state.store.revision_async().await?;
    let stored_nodes = state.store.list_async().await?;
    let after = state.store.revision_async().await?;
    if before != after {
        return Ok(());
    }

    let persisted_ids = stored_nodes
        .iter()
        .map(|stored| stored.config.id.clone())
        .collect::<std::collections::HashSet<_>>();
    let mut failures = Vec::new();
    for stored in stored_nodes {
        let current_revision = state
            .runtime_revisions
            .read()
            .get(&stored.config.id)
            .copied();
        if current_revision == Some(stored.revision)
            && state.scheduler.node(&stored.config.id).is_some()
        {
            continue;
        }

        let replacement = match prepare_node(state, &stored.config).await {
            Ok(node) => node,
            Err(error) => {
                if let Some(previous) = state.scheduler.node(&stored.config.id) {
                    previous.set_draining(true);
                    state.scheduler.notify_state_change();
                }
                failures.push(format!("{}: {error:#}", stored.config.id));
                continue;
            }
        };
        if let Some(previous) = state.scheduler.node(&stored.config.id) {
            previous.set_draining(true);
            state.scheduler.notify_state_change();
            let timeout = Duration::from_millis(state.settings.server.node_mutation_timeout_ms);
            if !state.scheduler.wait_for_node_idle(&previous, timeout).await {
                failures.push(format!(
                    "{}: active requests did not drain",
                    stored.config.id
                ));
                continue;
            }
            if let Err(error) = state.scheduler.replace_node(&replacement) {
                failures.push(format!("{}: {error}", stored.config.id));
                continue;
            }
        } else if let Err(error) = state.scheduler.add_node(Arc::clone(&replacement)) {
            failures.push(format!("{}: {error}", stored.config.id));
            continue;
        }
        state
            .runtime_revisions
            .write()
            .insert(stored.config.id, stored.revision);
    }

    for node in state.scheduler.nodes() {
        if persisted_ids.contains(node.id()) {
            continue;
        }
        node.set_draining(true);
        state.scheduler.notify_state_change();
        let timeout = Duration::from_millis(state.settings.server.node_mutation_timeout_ms);
        if !state.scheduler.wait_for_node_idle(&node, timeout).await {
            failures.push(format!(
                "{}: active requests did not drain before removal",
                node.id()
            ));
            continue;
        }
        state.scheduler.remove_node(node.id());
        state.metrics.remove_node(node.id());
        state.runtime_revisions.write().remove(node.id());
    }

    if failures.is_empty() {
        state.control_revision.store(after, AtomicOrdering::Release);
        Ok(())
    } else {
        anyhow::bail!(failures.join("; "))
    }
}
