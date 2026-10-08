//! Background-only content preparation. No token events are written as rows.
use serde_json::{Value, json};
use std::collections::BTreeMap;

const CREDENTIAL_KEYS: &[&str] = &[
    "authorization",
    "cookie",
    "api_key",
    "apikey",
    "access_token",
    "password",
    "secret",
    "refresh_token",
];

pub(super) fn prepare(
    bytes: &[u8],
    streaming: bool,
    anthropic: bool,
) -> (Value, Value, &'static str) {
    let (mut content, mut usage, representation) = if streaming {
        stream_summary(bytes, anthropic)
    } else if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
        let usage = usage(&value, anthropic);
        (value, usage, "json")
    } else {
        (
            Value::String(String::from_utf8_lossy(bytes).into_owned()),
            Value::Null,
            "partial_text",
        )
    };
    redact(&mut content);
    redact(&mut usage);
    (content, usage, representation)
}

#[allow(
    clippy::too_many_lines,
    clippy::case_sensitive_file_extension_comparisons
)]
fn stream_summary(bytes: &[u8], anthropic: bool) -> (Value, Value, &'static str) {
    let text = String::from_utf8_lossy(bytes)
        .replace("\r\n", "\n")
        .replace('\r', "\n");
    let mut blocks: BTreeMap<String, Value> = BTreeMap::new();
    let mut reported = json!({});
    let mut terminal = false;
    let mut unknown = 0;
    let mut final_response = None;
    for event in text.split("\n\n") {
        let data = event
            .lines()
            .filter_map(|line| line.strip_prefix("data:").map(str::trim_start))
            .collect::<Vec<_>>()
            .join("\n");
        if data.is_empty() {
            continue;
        }
        if data == "[DONE]" {
            terminal = true;
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&data) else {
            unknown += 1;
            continue;
        };
        merge_usage(&mut reported, &usage(&value, anthropic));
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if matches!(kind, "message_stop" | "response.completed") {
            terminal = true;
        }
        if kind == "response.completed" {
            final_response = value.get("response").cloned();
        }
        if let Some(choices) = value.get("choices").and_then(Value::as_array) {
            for choice in choices {
                let index = choice.get("index").and_then(Value::as_u64).unwrap_or(0);
                if let Some(delta) = choice.get("delta") {
                    for name in ["content", "reasoning_content", "reasoning"] {
                        append(
                            &mut blocks,
                            &format!("choice:{index}:{name}"),
                            name,
                            delta.get(name),
                        );
                    }
                    if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
                        for call in calls {
                            let ci = call.get("index").and_then(Value::as_u64).unwrap_or(0);
                            let key = format!("choice:{index}:tool:{ci}");
                            append(
                                &mut blocks,
                                &key,
                                "arguments",
                                call.pointer("/function/arguments"),
                            );
                            let block = blocks
                                .entry(key)
                                .or_insert_with(|| json!({"type":"tool_call"}));
                            for (name, path) in [("id", "/id"), ("name", "/function/name")] {
                                if let Some(value) = call.pointer(path) {
                                    block[name] = value.clone();
                                }
                            }
                        }
                    }
                }
                if let Some(reason) = choice.get("finish_reason").filter(|v| !v.is_null()) {
                    blocks.insert(
                        format!("choice:{index}:finish"),
                        json!({"finish_reason":reason}),
                    );
                }
            }
        } else if kind == "content_block_start" {
            let index = value.get("index").and_then(Value::as_u64).unwrap_or(0);
            if let Some(block) = value.get("content_block") {
                blocks.insert(format!("block:{index}"), block.clone());
            }
        } else if kind == "content_block_delta" {
            let index = value.get("index").and_then(Value::as_u64).unwrap_or(0);
            for name in ["text", "thinking", "partial_json", "signature"] {
                append(
                    &mut blocks,
                    &format!("block:{index}"),
                    name,
                    value.pointer(&format!("/delta/{name}")),
                );
            }
        } else if kind.ends_with(".delta") {
            let item = value
                .get("item_id")
                .and_then(Value::as_str)
                .unwrap_or("output");
            append(
                &mut blocks,
                &format!("{item}:{kind}"),
                "text",
                value.get("delta"),
            );
        } else if let Some(error) = value.get("error") {
            blocks.insert("error".to_owned(), json!({"error":error}));
        } else if !matches!(
            kind,
            "ping"
                | "message_start"
                | "message_delta"
                | "message_stop"
                | "content_block_stop"
                | "response.created"
                | "response.in_progress"
                | "response.completed"
        ) {
            unknown += 1;
        }
    }
    let content = final_response.unwrap_or_else(|| {
        json!({
            "format":"stream_summary","blocks":blocks.into_values().collect::<Vec<_>>(),
            "usage":reported,"terminal_marker_seen":terminal,"unrecognized_events":unknown,
        })
    });
    (content, reported, "stream_summary")
}

fn append(blocks: &mut BTreeMap<String, Value>, key: &str, name: &str, value: Option<&Value>) {
    if let Some(text) = value.and_then(Value::as_str) {
        let block = blocks
            .entry(key.to_owned())
            .or_insert_with(|| json!({"type":name}));
        let field = block
            .as_object_mut()
            .expect("summary block")
            .entry(name.to_owned())
            .or_insert_with(|| Value::String(String::new()));
        if let Value::String(previous) = field {
            previous.push_str(text);
        }
    }
}

pub(super) fn merge_usage(target: &mut Value, next: &Value) {
    if let Some(fields) = next.as_object() {
        for (key, value) in fields {
            if !value.is_null() {
                target[key] = value.clone();
            }
        }
    }
}

fn usage(value: &Value, anthropic: bool) -> Value {
    let Some(raw) = value
        .get("usage")
        .or_else(|| value.pointer("/message/usage"))
        .or_else(|| value.pointer("/response/usage"))
    else {
        return Value::Null;
    };
    let input = raw
        .get("prompt_tokens")
        .or_else(|| raw.get("input_tokens"))
        .and_then(Value::as_u64);
    let read = raw
        .get("cache_read_input_tokens")
        .or_else(|| raw.get("prompt_cache_hit_tokens"))
        .or_else(|| raw.pointer("/prompt_tokens_details/cached_tokens"))
        .or_else(|| raw.pointer("/input_tokens_details/cached_tokens"))
        .and_then(Value::as_u64);
    let write = raw
        .get("cache_creation_input_tokens")
        .and_then(Value::as_u64);
    let total_input = input.map(|n| {
        if anthropic {
            n.saturating_add(read.unwrap_or_default())
                .saturating_add(write.unwrap_or_default())
        } else {
            n
        }
    });
    let raw_preview = if serde_json::to_vec(raw).is_ok_and(|bytes| bytes.len() <= 4096) {
        raw.clone()
    } else {
        json!({"capture_state":"omitted","reason":"usage_too_large"})
    };
    json!({"input_tokens":total_input,"output_tokens":raw.get("completion_tokens").or_else(|| raw.get("output_tokens")).and_then(Value::as_u64),
        "cache_read_tokens":read,"cache_write_tokens":write,"reasoning_tokens":raw.pointer("/completion_tokens_details/reasoning_tokens").or_else(|| raw.pointer("/output_tokens_details/reasoning_tokens")).and_then(Value::as_u64),
        "source":"provider","normalization_version":1,"raw":raw_preview})
}

pub(super) fn redact(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let name = key.to_ascii_lowercase();
                if CREDENTIAL_KEYS.contains(&name.as_str()) {
                    *child = Value::String("[REDACTED]".to_owned());
                } else {
                    redact(child);
                }
            }
        }
        Value::Array(array) => {
            for child in array {
                redact(child);
            }
        }
        Value::String(text) => {
            redact_text_fields(text);
            // Preserve whitespace and code; mask common embedded credential tokens.
            for prefix in ["sk-", "sk_", "Bearer "] {
                let mut offset = 0;
                while let Some(found) = text[offset..].find(prefix) {
                    let start = offset + found;
                    let token_start = start + prefix.len();
                    let end = text[token_start..]
                        .find(|c: char| {
                            c.is_whitespace()
                                || matches!(
                                    c,
                                    '"' | '\'' | '`' | ',' | ';' | '<' | '>' | ')' | '}' | ']'
                                )
                        })
                        .map_or(text.len(), |n| token_start + n);
                    if end - token_start >= 8 {
                        text.replace_range(start..end, "[REDACTED]");
                        offset = start + 10;
                    } else {
                        offset = token_start;
                    }
                    if offset >= text.len() {
                        break;
                    }
                }
            }
        }
        _ => {}
    }
}

// Partial JSON and JSON embedded in tool argument strings still need field
// masking even when the complete envelope cannot be parsed.
fn redact_text_fields(text: &mut String) {
    for key in CREDENTIAL_KEYS {
        let pattern = format!("\"{key}\"");
        let mut offset = 0;
        while let Some(found) = text[offset..].to_ascii_lowercase().find(&pattern) {
            let end_key = offset + found + pattern.len();
            let after = text[end_key..].trim_start();
            let Some(after) = after.strip_prefix(':') else {
                offset = end_key;
                continue;
            };
            let after = after.trim_start();
            let start = text.len() - after.len();
            if after.starts_with('"') {
                let start = start + 1;
                let mut escaped = false;
                let mut end = text.len();
                for (index, character) in text[start..].char_indices() {
                    if character == '"' && !escaped {
                        end = start + index;
                        break;
                    }
                    if character == '\\' {
                        escaped = !escaped;
                    } else {
                        escaped = false;
                    }
                }
                text.replace_range(start..end, "[REDACTED]");
                offset = start + "[REDACTED]".len();
            } else {
                offset = end_key;
            }
        }
    }
}
