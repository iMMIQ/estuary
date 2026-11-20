use std::{io::Cursor, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use rmpv::Value;
use tokio::{sync::watch, time::MissedTickBehavior};
use tracing::{info, warn};
use zeromq::{DealerSocket, Socket, SocketRecv, SocketSend, SubSocket, ZmqMessage};

use crate::{
    config::VllmKvEventsConfig,
    kv_cache::{BlockHash, CacheMutation, ExactCacheDirectory},
    node::{HealthState, Node},
    prefix::PrefixDirectory,
};

use super::{KV_HEALTH_POLL_MAX, KvUpstreamUnhealthy};

#[derive(Default)]
pub(super) struct KvSyncState {
    pub(super) last_seq: Option<u64>,
    pub(super) synchronized: bool,
}

impl KvSyncState {
    fn invalidate(
        &mut self,
        node: &Node,
        exact_cache: &ExactCacheDirectory,
        prefix: &PrefixDirectory,
    ) {
        invalidate_cache_state(node, exact_cache, prefix);
        self.last_seq = None;
        self.synchronized = false;
    }

    fn reset_unhealthy(
        &mut self,
        node: &Node,
        exact_cache: &ExactCacheDirectory,
        prefix: &PrefixDirectory,
    ) {
        self.invalidate(node, exact_cache, prefix);
        node.record_kv_event_error(
            "KV cache state invalidated because the vLLM upstream is unhealthy; awaiting full replay"
                .to_owned(),
        );
    }
}

pub(super) async fn run_event_supervisor(
    node: Arc<Node>,
    exact_cache: Arc<ExactCacheDirectory>,
    prefix: Arc<PrefixDirectory>,
    config: VllmKvEventsConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut sync = KvSyncState::default();
    let mut suspended_for_health = false;
    loop {
        if *shutdown.borrow() {
            break;
        }
        if node.health() == HealthState::Unhealthy {
            if !suspended_for_health {
                sync.reset_unhealthy(&node, &exact_cache, &prefix);
                suspended_for_health = true;
                warn!(
                    node = node.id(),
                    "vLLM upstream is unhealthy; KV cache routing suspended"
                );
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                () = tokio::time::sleep(kv_health_poll_interval(&config)) => {}
            }
            continue;
        }
        if suspended_for_health {
            suspended_for_health = false;
            info!(
                node = node.id(),
                "vLLM upstream recovered; rebuilding KV cache state from replay"
            );
        }
        match run_event_session(
            &node,
            &exact_cache,
            &prefix,
            &config,
            &mut sync,
            &mut shutdown,
        )
        .await
        {
            Ok(()) => break,
            Err(error) => {
                let session_detected_unhealthy = error.is::<KvUpstreamUnhealthy>();
                if session_detected_unhealthy || node.health() == HealthState::Unhealthy {
                    if !session_detected_unhealthy && !suspended_for_health {
                        sync.reset_unhealthy(&node, &exact_cache, &prefix);
                    }
                    suspended_for_health = true;
                    warn!(
                        node = node.id(),
                        "vLLM upstream became unhealthy; KV cache routing suspended"
                    );
                } else {
                    exact_cache.suspend_node_owned(node.id(), node.instance_id());
                    node.record_kv_event_error(format!("KV event subscriber failed: {error}"));
                    warn!(node = node.id(), error = %error, "vLLM KV event subscriber disconnected");
                }
            }
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            () = tokio::time::sleep(Duration::from_millis(config.reconnect_ms)) => {}
        }
    }
}

pub(super) async fn run_event_session(
    node: &Node,
    exact_cache: &ExactCacheDirectory,
    prefix: &PrefixDirectory,
    config: &VllmKvEventsConfig,
    sync: &mut KvSyncState,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let mut socket = SubSocket::new();
    tokio::time::timeout(Duration::from_secs(5), socket.connect(&config.endpoint))
        .await
        .context("timed out connecting to KV event publisher")??;
    socket.subscribe(&config.topic).await?;
    info!(node = node.id(), endpoint = %config.endpoint, "subscribed to vLLM KV events");

    let mut replay_high_water = None;
    if let Some(replay_endpoint) = config.replay_endpoint.as_deref() {
        let start = replay_start_sequence(sync.last_seq, sync.synchronized);
        match replay_range(
            node,
            exact_cache,
            prefix,
            config,
            replay_endpoint,
            start,
            None,
        )
        .await
        {
            Ok(Some(replayed_through)) => {
                sync.last_seq = Some(replayed_through);
                sync.synchronized = true;
                replay_high_water = Some(replayed_through);
                exact_cache.resume_node_owned(node.id(), node.instance_id());
                node.record_kv_event_success();
            }
            Ok(None) if sync.synchronized => {
                exact_cache.resume_node_owned(node.id(), node.instance_id());
                node.record_kv_event_success();
            }
            Ok(None) => {
                synchronize_empty_replay(node, exact_cache, prefix, &mut sync.synchronized)?;
                node.record_kv_event_success();
            }
            Err(error) => {
                sync.invalidate(node, exact_cache, prefix);
                node.record_kv_event_error(format!("KV replay synchronization failed: {error}"));
                warn!(node = node.id(), error = %error, "could not synchronize vLLM KV replay buffer");
            }
        }
    }

    let mut health_interval = tokio::time::interval(kv_health_poll_interval(config));
    health_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        if node.is_retired() {
            return Ok(());
        }
        let message = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
                continue;
            }
            _ = health_interval.tick() => {
                if node.health() == HealthState::Unhealthy {
                    sync.reset_unhealthy(node, exact_cache, prefix);
                    return Err(KvUpstreamUnhealthy.into());
                }
                continue;
            }
            message = socket.recv() => message?,
        };
        process_event_frames(
            node,
            exact_cache,
            prefix,
            config,
            sync,
            &mut replay_high_water,
            message.into_vec(),
        )
        .await?;
    }
}

async fn process_event_frames(
    node: &Node,
    exact_cache: &ExactCacheDirectory,
    prefix: &PrefixDirectory,
    config: &VllmKvEventsConfig,
    sync: &mut KvSyncState,
    replay_high_water: &mut Option<u64>,
    frames: Vec<Bytes>,
) -> Result<()> {
    if frames
        .first()
        .is_none_or(|topic| topic.as_ref() != config.topic.as_bytes())
    {
        return Ok(());
    }
    if frames.len() != 3 {
        sync.invalidate(node, exact_cache, prefix);
        bail!("KV publisher sent {} frames instead of 3", frames.len());
    }
    let Ok(seq) = decode_sequence(&frames[1]) else {
        sync.invalidate(node, exact_cache, prefix);
        bail!("KV event sequence is not 8 bytes");
    };
    if replay_high_water.is_some_and(|high_water| seq <= high_water) {
        return Ok(());
    }
    *replay_high_water = None;
    if let Some(previous) = sync.last_seq {
        if seq <= previous {
            if seq == previous {
                return Ok(());
            }
            sync.invalidate(node, exact_cache, prefix);
            warn!(
                node = node.id(),
                previous, seq, "vLLM KV sequence reset; cache state cleared"
            );
            node.record_kv_event_error(format!(
                "KV sequence reset from {previous} to {seq}; awaiting full resynchronization"
            ));
        } else if sync.synchronized && seq > previous.saturating_add(1) {
            let recovered = match config.replay_endpoint.as_deref() {
                Some(endpoint) => replay_range(
                    node,
                    exact_cache,
                    prefix,
                    config,
                    endpoint,
                    previous.saturating_add(1),
                    Some(seq),
                )
                .await
                .map(|_| ()),
                None => Err(anyhow!("replay endpoint is not configured")),
            };
            if let Err(error) = recovered {
                sync.invalidate(node, exact_cache, prefix);
                node.record_kv_event_error(format!("KV replay gap recovery failed: {error}"));
                warn!(node = node.id(), error = %error, "could not replay KV event gap; cache state cleared");
            }
        }
    }
    if sync.last_seq.is_none() && !sync.synchronized {
        if seq == 0 {
            sync.synchronized = true;
        } else if let Some(endpoint) = config.replay_endpoint.as_deref() {
            match replay_range(node, exact_cache, prefix, config, endpoint, 0, Some(seq)).await {
                Ok(_) => sync.synchronized = true,
                Err(error) => {
                    invalidate_cache_state(node, exact_cache, prefix);
                    node.record_kv_event_error(format!(
                        "initial KV replay synchronization failed: {error}"
                    ));
                    warn!(node = node.id(), error = %error, "could not establish initial KV event history");
                }
            }
        }
    }
    match apply_payload(
        node,
        exact_cache,
        prefix,
        config,
        &frames[2],
        sync.synchronized,
    ) {
        Ok(became_synchronized) => sync.synchronized |= became_synchronized,
        Err(error) => {
            sync.last_seq = None;
            sync.synchronized = false;
            return Err(error);
        }
    }
    sync.last_seq = sync.synchronized.then_some(seq);
    Ok(())
}

fn kv_health_poll_interval(config: &VllmKvEventsConfig) -> Duration {
    Duration::from_millis(config.reconnect_ms).min(KV_HEALTH_POLL_MAX)
}

pub(super) fn synchronize_empty_replay(
    node: &Node,
    exact_cache: &ExactCacheDirectory,
    prefix: &PrefixDirectory,
    synchronized: &mut bool,
) -> Result<()> {
    exact_cache.apply_owned(node.id(), node.instance_id(), vec![CacheMutation::Clear])?;
    prefix.clear_node(node.id());
    *synchronized = true;
    Ok(())
}

pub(super) fn replay_start_sequence(last_seq: Option<u64>, synchronized: bool) -> u64 {
    if synchronized {
        last_seq.map_or(0, |seq| seq.saturating_add(1))
    } else {
        0
    }
}

async fn replay_range(
    node: &Node,
    exact_cache: &ExactCacheDirectory,
    prefix: &PrefixDirectory,
    config: &VllmKvEventsConfig,
    endpoint: &str,
    start: u64,
    stop: Option<u64>,
) -> Result<Option<u64>> {
    let mut socket = DealerSocket::new();
    tokio::time::timeout(Duration::from_secs(5), socket.connect(endpoint))
        .await
        .context("timed out connecting to KV replay endpoint")??;
    let request = ZmqMessage::try_from(vec![
        Bytes::new(),
        Bytes::copy_from_slice(&start.to_be_bytes()),
    ])
    .map_err(|error| anyhow!(error.to_string()))?;
    socket.send(request).await?;

    let mut expected = start;
    let mut replayed_through = None;
    loop {
        let message = tokio::time::timeout(
            Duration::from_millis(node.provider().request_timeout_ms),
            socket.recv(),
        )
        .await
        .context("timed out waiting for KV replay")??;
        let mut frames = message.into_vec();
        if frames.first().is_some_and(Bytes::is_empty) {
            frames.remove(0);
        }
        if frames.last().is_some_and(Bytes::is_empty) {
            break;
        }
        let (seq_frame, payload) = match frames.as_slice() {
            [seq, payload] => (seq, payload),
            [topic, seq, payload] if topic.as_ref() == config.topic.as_bytes() => (seq, payload),
            _ => bail!("invalid KV replay frame layout"),
        };
        let seq = decode_sequence(seq_frame)?;
        if seq != expected || stop.is_some_and(|stop| seq >= stop) {
            bail!("KV replay was not contiguous at sequence {expected}");
        }
        apply_payload(node, exact_cache, prefix, config, payload, true)?;
        replayed_through = Some(seq);
        expected = expected
            .checked_add(1)
            .ok_or_else(|| anyhow!("KV replay sequence overflow"))?;
    }
    if let Some(stop) = stop
        && expected != stop
    {
        bail!("KV replay ended at {expected}, expected {stop}");
    }
    Ok(replayed_through)
}

fn decode_sequence(frame: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = frame
        .try_into()
        .map_err(|_| anyhow!("KV event sequence is not 8 bytes"))?;
    Ok(u64::from_be_bytes(bytes))
}

pub(super) fn apply_payload(
    node: &Node,
    exact_cache: &ExactCacheDirectory,
    prefix: &PrefixDirectory,
    config: &VllmKvEventsConfig,
    payload: &[u8],
    synchronized: bool,
) -> Result<bool> {
    if node.is_retired() {
        return Ok(false);
    }
    if payload.len() > config.max_event_bytes {
        bail!("KV event payload exceeds configured size limit");
    }
    let mut mutations = match decode_event_batch(payload) {
        Ok(mutations) => mutations,
        Err(error) => {
            invalidate_cache_state(node, exact_cache, prefix);
            return Err(error);
        }
    };
    let clears = mutations
        .iter()
        .any(|item| matches!(item, CacheMutation::Clear));
    let synchronized = if synchronized {
        true
    } else if let Some(last_clear) = mutations
        .iter()
        .rposition(|item| matches!(item, CacheMutation::Clear))
    {
        mutations.drain(..last_clear);
        true
    } else {
        exact_cache.suspend_node_owned(node.id(), node.instance_id());
        return Ok(false);
    };
    if let Err(error) = exact_cache.apply_owned(node.id(), node.instance_id(), mutations) {
        invalidate_cache_state(node, exact_cache, prefix);
        return Err(error);
    }
    if clears {
        prefix.clear_node(node.id());
    }
    node.record_kv_event_success();
    Ok(synchronized)
}

fn invalidate_cache_state(
    node: &Node,
    exact_cache: &ExactCacheDirectory,
    prefix: &PrefixDirectory,
) {
    exact_cache.invalidate_node_owned(node.id(), node.instance_id());
    prefix.clear_node(node.id());
    node.bump_provider_generation();
}

pub(super) fn decode_event_batch(payload: &[u8]) -> Result<Vec<CacheMutation>> {
    let value = rmpv::decode::read_value_with_max_depth(&mut Cursor::new(payload), 64)
        .context("invalid vLLM KV MessagePack")?;
    let batch = value
        .as_array()
        .ok_or_else(|| anyhow!("vLLM KV batch is not an array"))?;
    let events = batch
        .get(1)
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("vLLM KV batch has no event array"))?;
    events.iter().filter_map(decode_event).collect()
}

pub(super) fn decode_event(event: &Value) -> Option<Result<CacheMutation>> {
    let Some(map) = event.as_map() else {
        return Some(Err(anyhow!("vLLM KV event is not a map")));
    };
    match field(map, "type").and_then(Value::as_str) {
        Some("BlockStored") => decode_stored(map).transpose(),
        Some("BlockRemoved") => decode_removed(map).transpose(),
        Some("AllBlocksCleared") => Some(Ok(CacheMutation::Clear)),
        Some(_) | None => None,
    }
}

fn decode_stored(map: &[(Value, Value)]) -> Result<Option<CacheMutation>> {
    if !is_local_gpu_event(map) || has_special_cache_keys(map) {
        return Ok(None);
    }
    let hashes = hashes_field(map, "block_hashes")?;
    let parent = match field(map, "parent_block_hash") {
        None | Some(Value::Nil) => None,
        Some(value) => Some(decode_hash(value)?),
    };
    let token_ids = integer_array(field_required(map, "token_ids")?)?;
    let block_size = usize::try_from(unsigned(field_required(map, "block_size")?)?)
        .context("KV block size does not fit usize")?;
    let group = optional_group(map)?;
    Ok(Some(CacheMutation::Store {
        hashes,
        parent,
        token_ids,
        block_size,
        group,
    }))
}

fn decode_removed(map: &[(Value, Value)]) -> Result<Option<CacheMutation>> {
    if !is_local_gpu_event(map) {
        return Ok(None);
    }
    Ok(Some(CacheMutation::Remove {
        hashes: hashes_field(map, "block_hashes")?,
        group: optional_group(map)?,
    }))
}

fn is_local_gpu_event(map: &[(Value, Value)]) -> bool {
    let gpu = field(map, "medium").and_then(Value::as_str) == Some("GPU");
    let local = !matches!(
        field(map, "locality").and_then(Value::as_str),
        Some("REMOTE")
    );
    gpu && local
}

fn has_special_cache_keys(map: &[(Value, Value)]) -> bool {
    field(map, "lora_name").is_some_and(|value| !value.is_nil())
        || field(map, "extra_keys")
            .and_then(Value::as_array)
            .is_some_and(|values| values.iter().any(|value| !value.is_nil()))
}

fn hashes_field(map: &[(Value, Value)], name: &str) -> Result<Vec<BlockHash>> {
    field_required(map, name)?
        .as_array()
        .ok_or_else(|| anyhow!("KV {name} is not an array"))?
        .iter()
        .map(decode_hash)
        .collect()
}

fn decode_hash(value: &Value) -> Result<BlockHash> {
    match value {
        Value::Binary(bytes) => Ok(BlockHash::Bytes(bytes.clone())),
        Value::Integer(integer) => integer
            .as_u64()
            .map(BlockHash::Integer)
            .ok_or_else(|| anyhow!("KV block hash integer is negative")),
        _ => bail!("KV block hash is neither bytes nor integer"),
    }
}

fn integer_array(value: &Value) -> Result<Vec<u64>> {
    value
        .as_array()
        .ok_or_else(|| anyhow!("KV token_ids is not an array"))?
        .iter()
        .map(unsigned)
        .collect()
}

fn unsigned(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| anyhow!("KV integer is negative or out of range"))
}

fn optional_group(map: &[(Value, Value)]) -> Result<i64> {
    match field(map, "group_idx") {
        None | Some(Value::Nil) => Ok(0),
        Some(value) => value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
            .ok_or_else(|| anyhow!("KV group_idx is invalid")),
    }
}

fn field_required<'a>(map: &'a [(Value, Value)], name: &str) -> Result<&'a Value> {
    field(map, name).ok_or_else(|| anyhow!("KV event is missing {name}"))
}

fn field<'a>(map: &'a [(Value, Value)], name: &str) -> Option<&'a Value> {
    map.iter()
        .find(|(key, _)| key.as_str() == Some(name))
        .map(|(_, value)| value)
}
