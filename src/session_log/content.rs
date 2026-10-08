//! Immutable JSON objects and persistent ordered arrays, with reference-counted GC.
use std::{
    collections::BTreeMap,
    io::{Read, Write},
};

use anyhow::{Result, bail};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", content = "value")]
enum StoredValue {
    Atom(Value),
    Object(BTreeMap<String, String>),
    Array(Option<String>),
}

pub(super) struct Cache {
    values: lru::LruCache<blake3::Hash, String>,
    sequences: lru::LruCache<blake3::Hash, ()>,
}
impl Cache {
    pub fn new() -> Self {
        let capacity = std::num::NonZeroUsize::new(8192).expect("nonzero cache capacity");
        Self {
            values: lru::LruCache::new(capacity),
            sequences: lru::LruCache::new(capacity),
        }
    }
}

// The cache is transaction-local: another writer's retention must never make a
// cached hash look present after its last reference has been collected.
pub(super) fn put(connection: &Connection, value: &Value, cache: &mut Cache) -> Result<String> {
    let value_key = blake3::hash(&serde_json::to_vec(value)?);
    if let Some(hash) = cache.values.get(&value_key) {
        return Ok(hash.clone());
    }
    let mut blobs = Vec::new();
    let mut sequences = Vec::new();
    let stored = match value {
        Value::Object(object) => {
            let mut fields = BTreeMap::new();
            for (key, value) in object {
                let hash = put(connection, value, cache)?;
                blobs.push(hash.clone());
                fields.insert(key.clone(), hash);
            }
            StoredValue::Object(fields)
        }
        Value::Array(values) => {
            let mut previous: Option<String> = None;
            for (index, value) in values.iter().enumerate() {
                let item = put(connection, value, cache)?;
                let encoded = serde_json::to_vec(&(1u8, &previous, &item))?;
                let sequence_key = blake3::hash(&encoded);
                let hash = sequence_key.to_hex().to_string();
                if !cache.sequences.contains(&sequence_key) && connection.execute(
                    "INSERT OR IGNORE INTO sequence_nodes(hash, previous_hash, item_hash, item_count) VALUES(?1,?2,?3,?4)",
                    params![hash, previous, item, index + 1],
                )? == 1 {
                    connection.execute("UPDATE content_blobs SET refs=refs+1 WHERE hash=?1", [&item])?;
                    if let Some(previous) = &previous {
                        connection.execute("UPDATE sequence_nodes SET refs=refs+1 WHERE hash=?1", [previous])?;
                    }
                }
                cache.sequences.put(sequence_key, ());
                previous = Some(hash);
            }
            sequences.extend(previous.iter().cloned());
            StoredValue::Array(previous)
        }
        _ => StoredValue::Atom(value.clone()),
    };
    let encoded = serde_json::to_vec(&stored)?;
    let hash = blake3::hash(&encoded).to_hex().to_string();
    // Avoid compressing or updating references for already-known content.
    let exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM content_blobs WHERE hash=?1)",
        [&hash],
        |row| row.get(0),
    )?;
    if !exists {
        let (codec, data) = if encoded.len() < 128 {
            ("raw", encoded.clone())
        } else {
            let mut compressor = GzEncoder::new(Vec::new(), Compression::fast());
            compressor.write_all(&encoded)?;
            ("gzip", compressor.finish()?)
        };
        connection.execute(
            "INSERT INTO content_blobs(hash,codec,data,raw_bytes,stored_bytes,blob_refs,sequence_refs) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![hash,codec,data,encoded.len(),data.len(),serde_json::to_string(&blobs)?,serde_json::to_string(&sequences)?],
        )?;
        for child in blobs {
            connection.execute(
                "UPDATE content_blobs SET refs=refs+1 WHERE hash=?1",
                [child],
            )?;
        }
        for child in sequences {
            connection.execute(
                "UPDATE sequence_nodes SET refs=refs+1 WHERE hash=?1",
                [child],
            )?;
        }
    }
    cache.values.put(value_key, hash.clone());
    Ok(hash)
}

pub(super) struct ReadBudget {
    pub bytes: usize,
    pub nodes: usize,
    pub deadline: std::time::Instant,
}

pub(super) fn get(
    connection: &Connection,
    hash: &str,
    budget: &mut ReadBudget,
    depth: usize,
) -> Result<Value> {
    if depth > 128 || budget.nodes == 0 || std::time::Instant::now() > budget.deadline {
        bail!("payload exceeds reconstruction limits");
    }
    budget.nodes -= 1;
    let (codec, data, raw): (String, Vec<u8>, usize) = connection.query_row(
        "SELECT codec,data,raw_bytes FROM content_blobs WHERE hash=?1",
        [hash],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    if raw > budget.bytes {
        bail!("payload exceeds reconstruction byte limit");
    }
    budget.bytes -= raw;
    let decoded = if codec == "gzip" {
        let mut output = Vec::new();
        GzDecoder::new(data.as_slice())
            .take(u64::try_from(raw)?.saturating_add(1))
            .read_to_end(&mut output)?;
        if output.len() != raw {
            bail!("invalid compressed payload length");
        }
        output
    } else {
        data
    };
    match serde_json::from_slice::<StoredValue>(&decoded)? {
        StoredValue::Atom(value) => Ok(value),
        StoredValue::Object(fields) => {
            let mut object = serde_json::Map::new();
            for (key, child) in fields {
                object.insert(key, get(connection, &child, budget, depth + 1)?);
            }
            Ok(Value::Object(object))
        }
        StoredValue::Array(mut tail) => {
            let mut items = Vec::new();
            while let Some(hash) = tail {
                if budget.nodes == 0 || std::time::Instant::now() > budget.deadline {
                    bail!("payload exceeds sequence limits");
                }
                budget.nodes -= 1;
                let (previous, item): (Option<String>, String) = connection.query_row(
                    "SELECT previous_hash,item_hash FROM sequence_nodes WHERE hash=?1",
                    [hash],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                items.push(get(connection, &item, budget, depth + 1)?);
                tail = previous;
            }
            items.reverse();
            Ok(Value::Array(items))
        }
    }
}

pub(super) fn collect(connection: &Connection) -> Result<()> {
    // Child references are released by delete triggers; repeated bounded passes
    // eventually reclaim deep histories without a full graph scan.
    for _ in 0..8 {
        let blobs = connection.execute("DELETE FROM content_blobs WHERE hash IN (SELECT hash FROM content_blobs WHERE refs=0 LIMIT 1000)", [])?;
        let nodes = connection.execute("DELETE FROM sequence_nodes WHERE hash IN (SELECT hash FROM sequence_nodes WHERE refs=0 LIMIT 1000)", [])?;
        if blobs + nodes == 0 {
            break;
        }
    }
    Ok(())
}
