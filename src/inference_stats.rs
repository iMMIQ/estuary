//! Bounded, observational SSE inspection. It never changes or rejects upstream bytes.

use serde_json::Value;

const MAX_OBSERVATION_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
#[allow(clippy::struct_field_names)]
pub(crate) struct Usage {
    pub input_tokens: Option<usize>,
    #[serde(rename = "cache_read_tokens")]
    pub cached_tokens: Option<usize>,
    pub cache_write_tokens: Option<usize>,
    pub reasoning_tokens: Option<usize>,
    pub output_tokens: Option<usize>,
}

impl Usage {
    pub fn log_value(self, anthropic: bool) -> Value {
        let mut value = serde_json::to_value(self).unwrap_or_default();
        if anthropic {
            value["input_tokens"] = self
                .input_tokens
                .map(|n| {
                    n.saturating_add(self.cached_tokens.unwrap_or_default())
                        .saturating_add(self.cache_write_tokens.unwrap_or_default())
                })
                .into();
        }
        value["source"] = "provider_observer".into();
        value["normalization_version"] = 1.into();
        value
    }
    pub fn from_response(bytes: &[u8]) -> Self {
        // Ignore response content while deserializing, avoiding a second large response allocation.
        let mut usage = Self::default();
        if let Ok(envelope) = serde_json::from_slice::<UsageEnvelope>(bytes) {
            for reported in [
                envelope.usage,
                envelope.message.and_then(|nested| nested.usage),
                envelope.response.and_then(|nested| nested.usage),
            ]
            .into_iter()
            .flatten()
            {
                merge(
                    &mut usage.input_tokens,
                    reported.prompt_tokens.or(reported.input_tokens),
                );
                merge(
                    &mut usage.output_tokens,
                    reported.completion_tokens.or(reported.output_tokens),
                );
                merge(
                    &mut usage.cache_write_tokens,
                    reported.cache_creation_input_tokens,
                );
                merge(
                    &mut usage.reasoning_tokens,
                    reported
                        .completion_tokens_details
                        .or(reported.output_tokens_details)
                        .and_then(|details| details.reasoning_tokens),
                );
                merge(
                    &mut usage.cached_tokens,
                    reported
                        .prompt_tokens_details
                        .or(reported.input_tokens_details)
                        .and_then(|details| details.cached_tokens)
                        .or(reported.cache_read_input_tokens)
                        .or(reported.prompt_cache_hit_tokens),
                );
            }
        }
        usage
    }
    pub fn observe(&mut self, value: &Value) {
        for usage in [
            value.get("usage"),
            value.pointer("/message/usage"),
            value.pointer("/response/usage"),
        ]
        .into_iter()
        .flatten()
        {
            merge(
                &mut self.input_tokens,
                count(usage, "prompt_tokens").or_else(|| count(usage, "input_tokens")),
            );
            merge(
                &mut self.output_tokens,
                count(usage, "completion_tokens").or_else(|| count(usage, "output_tokens")),
            );
            merge(
                &mut self.cache_write_tokens,
                count(usage, "cache_creation_input_tokens"),
            );
            merge(
                &mut self.reasoning_tokens,
                usage
                    .pointer("/completion_tokens_details/reasoning_tokens")
                    .or_else(|| usage.pointer("/output_tokens_details/reasoning_tokens"))
                    .and_then(Value::as_u64)
                    .and_then(|n| usize::try_from(n).ok()),
            );
            merge(
                &mut self.cached_tokens,
                usage
                    .pointer("/prompt_tokens_details/cached_tokens")
                    .or_else(|| usage.pointer("/input_tokens_details/cached_tokens"))
                    .or_else(|| usage.get("cache_read_input_tokens"))
                    .or_else(|| usage.get("prompt_cache_hit_tokens"))
                    .and_then(Value::as_u64)
                    .and_then(|n| usize::try_from(n).ok()),
            );
        }
    }
}

#[derive(serde::Deserialize)]
struct UsageEnvelope {
    usage: Option<ReportedUsage>,
    message: Option<NestedUsage>,
    response: Option<NestedUsage>,
}

#[derive(serde::Deserialize)]
struct NestedUsage {
    usage: Option<ReportedUsage>,
}

#[derive(serde::Deserialize)]
struct ReportedUsage {
    prompt_tokens: Option<usize>,
    input_tokens: Option<usize>,
    completion_tokens: Option<usize>,
    output_tokens: Option<usize>,
    prompt_tokens_details: Option<CacheUsage>,
    input_tokens_details: Option<CacheUsage>,
    cache_read_input_tokens: Option<usize>,
    prompt_cache_hit_tokens: Option<usize>,
    cache_creation_input_tokens: Option<usize>,
    completion_tokens_details: Option<ReasoningUsage>,
    output_tokens_details: Option<ReasoningUsage>,
}
#[derive(serde::Deserialize)]
struct ReasoningUsage {
    reasoning_tokens: Option<usize>,
}

#[derive(serde::Deserialize)]
struct CacheUsage {
    cached_tokens: Option<usize>,
}

fn count(value: &Value, key: &str) -> Option<usize> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
}

fn merge(target: &mut Option<usize>, value: Option<usize>) {
    if let Some(value) = value {
        *target = Some(target.unwrap_or(0).max(value));
    }
}

// These independent observations are not mutually exclusive lifecycle states.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Default)]
pub(crate) struct StreamObservation {
    line: Vec<u8>,
    data: Vec<u8>,
    oversized: bool,
    previous_cr: bool,
    pub has_output: bool,
    pub terminal_marker_seen: bool,
    pub incomplete: bool,
    pub error_seen: bool,
    pub has_visible_text: bool,
    pub usage: Usage,
}

impl StreamObservation {
    pub fn observe_json(&mut self, data: &[u8]) {
        if data.iter().all(u8::is_ascii_whitespace) {
            return;
        }
        if data == b"[DONE]" {
            self.terminal_marker_seen = true;
            return;
        }
        if data.len() > MAX_OBSERVATION_BYTES {
            self.incomplete = true;
            return;
        }
        if let Ok(value) = serde_json::from_slice::<Value>(data) {
            self.terminal_marker_seen |= matches!(
                value.get("type").and_then(Value::as_str),
                Some("message_stop" | "response.completed")
            );
            self.error_seen |= value.get("error").is_some_and(|error| !error.is_null())
                || matches!(
                    value.get("type").and_then(Value::as_str),
                    Some("error" | "response.failed" | "response.incomplete")
                );
            self.has_visible_text |= visible_text(&value);
            self.usage.observe(&value);
            self.has_output |= has_generation(&value);
        } else {
            self.incomplete = true;
        }
    }

    pub fn observe_bytes(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            if byte == b'\r' {
                self.finish_line();
                self.previous_cr = true;
            } else if byte == b'\n' {
                if !self.previous_cr {
                    self.finish_line();
                }
                self.previous_cr = false;
            } else if !self.oversized && self.line.len() + self.data.len() < MAX_OBSERVATION_BYTES {
                self.previous_cr = false;
                self.line.push(byte);
            } else {
                self.previous_cr = false;
                self.oversized = true;
                self.incomplete = true;
                // Retain only whether this line is blank so we can recover at the next event.
                if self.line.is_empty() {
                    self.line.push(b'x');
                }
            }
        }
    }

    fn finish_line(&mut self) {
        if self.line.last() == Some(&b'\r') {
            self.line.pop();
        }
        if self.line.is_empty() {
            if !self.oversized {
                let data = std::mem::take(&mut self.data);
                self.observe_json(&data);
                self.data = data;
            }
            self.data.clear();
            self.oversized = false;
        } else if !self.oversized
            && let Some(data) = self.line.strip_prefix(b"data:")
        {
            if !self.data.is_empty() {
                self.data.push(b'\n');
            }
            self.data
                .extend_from_slice(data.strip_prefix(b" ").unwrap_or(data));
        }
        self.line.clear();
    }
}

fn nonempty(value: Option<&Value>) -> bool {
    value
        .and_then(Value::as_str)
        .is_some_and(|text| !text.is_empty())
}

fn has_generation(value: &Value) -> bool {
    if let Some(choices) = value.get("choices").and_then(Value::as_array)
        && choices.iter().any(|choice| {
            nonempty(choice.get("text"))
                || choice.get("delta").is_some_and(|delta| {
                    ["content", "reasoning_content", "reasoning"]
                        .iter()
                        .any(|key| nonempty(delta.get(key)))
                        || delta
                            .get("tool_calls")
                            .and_then(Value::as_array)
                            .is_some_and(|calls| {
                                calls.iter().any(|call| {
                                    nonempty(call.pointer("/function/name"))
                                        || nonempty(call.pointer("/function/arguments"))
                                })
                            })
                        || nonempty(delta.pointer("/function_call/name"))
                        || nonempty(delta.pointer("/function_call/arguments"))
                })
        })
    {
        return true;
    }
    match value.get("type").and_then(Value::as_str) {
        Some("content_block_delta") => ["text", "thinking", "partial_json"]
            .iter()
            .any(|key| nonempty(value.get("delta").and_then(|delta| delta.get(key)))),
        Some("content_block_start") => {
            nonempty(value.pointer("/content_block/text"))
                || nonempty(value.pointer("/content_block/thinking"))
                || nonempty(value.pointer("/content_block/name"))
        }
        Some(
            "response.output_text.delta"
            | "response.reasoning_text.delta"
            | "response.reasoning_summary_text.delta"
            | "response.function_call_arguments.delta",
        ) => nonempty(value.get("delta")),
        _ => false,
    }
}

fn visible_text(value: &Value) -> bool {
    value
        .get("choices")
        .and_then(Value::as_array)
        .is_some_and(|choices| {
            choices.iter().any(|choice| {
                nonempty(choice.pointer("/delta/content")) || nonempty(choice.get("text"))
            })
        })
        || (value.get("type").and_then(Value::as_str) == Some("content_block_delta")
            && nonempty(value.pointer("/delta/text")))
        || (value.get("type").and_then(Value::as_str) == Some("response.output_text.delta")
            && nonempty(value.get("delta")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragmented_sse_skips_metadata_and_observes_generation_and_usage() {
        let mut observation = StreamObservation::default();
        let stream = concat!(
            ": heartbeat\r\n\r\n",
            "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\r\n\r\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"你好\"}}]}\r\n\r\n",
            "data: {\"usage\":{\"prompt_tokens\":42,\"completion_tokens\":7,\"prompt_tokens_details\":{\"cached_tokens\":32}}}\n\n",
            "data: [DONE]\n\n",
        );
        for byte in stream.as_bytes() {
            observation.observe_bytes(&[*byte]);
        }
        assert!(observation.has_output);
        assert_eq!(observation.usage.input_tokens, Some(42));
        assert_eq!(observation.usage.cached_tokens, Some(32));
        assert_eq!(observation.usage.output_tokens, Some(7));
    }

    #[test]
    fn metadata_and_keepalives_are_not_generation() {
        for data in [
            r#"{"type":"ping"}"#,
            r#"{"type":"message_start","message":{"usage":{"input_tokens":9,"output_tokens":0}}}"#,
            r#"{"type":"response.created"}"#,
            r#"{"choices":[{"delta":{"role":"assistant","content":""}}]}"#,
        ] {
            let mut observation = StreamObservation::default();
            observation.observe_json(data.as_bytes());
            assert!(!observation.has_output, "{data}");
        }
    }

    #[test]
    fn reasoning_tools_and_native_protocols_count_as_generation() {
        for data in [
            r#"{"choices":[{"delta":{"reasoning_content":"think"}}]}"#,
            r#"{"choices":[{"delta":{"tool_calls":[{"function":{"arguments":"{"}}]}}]}"#,
            r#"{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"think"}}"#,
            r#"{"type":"response.output_text.delta","delta":"answer"}"#,
        ] {
            let mut observation = StreamObservation::default();
            observation.observe_json(data.as_bytes());
            assert!(observation.has_output, "{data}");
        }
    }

    #[test]
    fn oversized_or_invalid_events_do_not_prevent_later_observation() {
        let mut observation = StreamObservation::default();
        observation.observe_bytes(b"data: ");
        observation.observe_bytes(&vec![b'x'; MAX_OBSERVATION_BYTES * 2]);
        assert!(observation.line.len() + observation.data.len() <= MAX_OBSERVATION_BYTES);
        observation
            .observe_bytes(b"\n\ndata: invalid\n\ndata: {\"choices\":[{\"text\":\"ok\"}]}\n\n");
        assert!(observation.has_output);
    }

    #[test]
    fn cr_line_endings_and_multiline_json_are_observed() {
        let mut observation = StreamObservation::default();
        observation.observe_bytes(b"data: {\rdata: \"choices\": [{\"text\": \"answer\"}]}\r\r");
        assert!(observation.has_output);
    }

    #[test]
    fn buffered_usage_ignores_content_and_extracts_native_and_responses_fields() {
        let usage = Usage::from_response(br#"{"content":[{"text":"answer"}],"usage":{"input_tokens":12,"cache_read_input_tokens":8,"output_tokens":4}}"#);
        assert_eq!(usage.input_tokens, Some(12));
        assert_eq!(usage.cached_tokens, Some(8));
        assert_eq!(usage.output_tokens, Some(4));
        let usage = Usage::from_response(br#"{"response":{"usage":{"input_tokens":20,"input_tokens_details":{"cached_tokens":16},"output_tokens":5}}}"#);
        assert_eq!(usage.cached_tokens, Some(16));
        assert_eq!(usage.output_tokens, Some(5));
        assert_eq!(Usage::from_response(b"invalid").output_tokens, None);
    }
}
