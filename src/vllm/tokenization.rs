use std::{mem::size_of, num::NonZeroUsize};

use anyhow::{Context, Result, bail};
use lru::LruCache;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Map, Value as JsonValue};

use crate::node::Node;

use super::{
    MAX_MANAGEMENT_BODY_BYTES, MAX_TOKENIZATION_CACHE_BYTES, TOKENIZATION_CACHE_ENTRY_OVERHEAD,
    read_bounded_response,
};

pub(super) fn tokenize_payload(endpoint: &str, body: &JsonValue) -> Option<Map<String, JsonValue>> {
    let object = body.as_object()?;
    let mut payload = Map::new();
    match endpoint {
        "chat/completions" => {
            if object
                .get("documents")
                .is_some_and(|value| !value.is_null())
            {
                return None;
            }
            payload.insert("messages".to_owned(), object.get("messages")?.clone());
            for key in [
                "tools",
                "add_generation_prompt",
                "continue_final_message",
                "add_special_tokens",
                "chat_template",
                "chat_template_kwargs",
                "media_io_kwargs",
                "mm_processor_kwargs",
            ] {
                if let Some(value) = object.get(key) {
                    payload.insert(key.to_owned(), value.clone());
                }
            }
        }
        "completions" => {
            payload.insert(
                "prompt".to_owned(),
                JsonValue::String(object.get("prompt")?.as_str()?.to_owned()),
            );
            if let Some(value) = object.get("add_special_tokens") {
                payload.insert("add_special_tokens".to_owned(), value.clone());
            }
        }
        _ => return None,
    }
    Some(payload)
}

pub(super) fn pretokenized_completion(body: &JsonValue) -> Option<Vec<u64>> {
    body.get("prompt")?
        .as_array()?
        .iter()
        .map(JsonValue::as_u64)
        .collect()
}

#[derive(Deserialize)]
pub(super) struct TokenizeResponse {
    pub(super) tokens: Vec<u64>,
}

pub(super) async fn request_tokenization(
    client: &Client,
    node: &Node,
    payload: Map<String, JsonValue>,
) -> Result<Vec<u64>> {
    let url = node.provider_url(&node.provider().tokenize_path)?;
    let mut request = client.post(url).json(&payload);
    for (name, value) in node.headers() {
        request = request.header(name, value);
    }
    let response = request.send().await?.error_for_status()?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_MANAGEMENT_BODY_BYTES as u64)
    {
        bail!("vLLM tokenize response is too large");
    }
    let body = read_bounded_response(response, "vLLM tokenize response").await?;
    Ok(serde_json::from_slice::<TokenizeResponse>(&body)
        .context("invalid /tokenize JSON")?
        .tokens)
}

pub(super) fn tokenization_key(
    endpoint: &str,
    public_model: &str,
    node_id: &str,
    generation: u64,
    payload: &Map<String, JsonValue>,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for value in [endpoint, public_model, node_id] {
        hasher.update(value.as_bytes());
        hasher.update(&[0]);
    }
    hasher.update(&generation.to_le_bytes());
    if let Ok(encoded) = sonic_rs::to_vec(payload) {
        hasher.update(&encoded);
    }
    *hasher.finalize().as_bytes()
}

#[derive(Debug)]
pub(super) struct TokenizationCache {
    pub(super) values: LruCache<[u8; 32], (Vec<u64>, usize)>,
    pub(super) used_bytes: usize,
    pub(super) max_bytes: usize,
}

impl TokenizationCache {
    pub(super) fn new(capacity: usize) -> Self {
        Self::with_max_bytes(capacity, MAX_TOKENIZATION_CACHE_BYTES)
    }

    pub(super) fn with_max_bytes(capacity: usize, max_bytes: usize) -> Self {
        Self {
            values: LruCache::new(NonZeroUsize::new(capacity).expect("cache capacity is positive")),
            used_bytes: 0,
            max_bytes,
        }
    }

    pub(super) fn raise_capacity(&mut self, capacity: usize) {
        if capacity > self.values.cap().get() {
            self.values
                .resize(NonZeroUsize::new(capacity).expect("cache capacity is positive"));
        }
    }

    pub(super) fn get(&mut self, key: &[u8; 32]) -> Option<Vec<u64>> {
        self.values.get(key).map(|(tokens, _)| tokens.clone())
    }

    pub(super) fn insert(&mut self, key: [u8; 32], tokens: Vec<u64>) {
        let bytes = TOKENIZATION_CACHE_ENTRY_OVERHEAD
            .saturating_add(tokens.capacity().saturating_mul(size_of::<u64>()));
        if let Some((_, previous_bytes)) = self.values.pop(&key) {
            self.used_bytes = self.used_bytes.saturating_sub(previous_bytes);
        }
        self.used_bytes = self.used_bytes.saturating_add(bytes);
        if let Some((_, (_, evicted_bytes))) = self.values.push(key, (tokens, bytes)) {
            self.used_bytes = self.used_bytes.saturating_sub(evicted_bytes);
        }
        while self.used_bytes > self.max_bytes {
            let Some((_, (_, evicted_bytes))) = self.values.pop_lru() else {
                break;
            };
            self.used_bytes = self.used_bytes.saturating_sub(evicted_bytes);
        }
    }
}
