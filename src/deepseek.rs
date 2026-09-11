//! Bridge recipe's shared conversation and output events to Chat Completions.
//! No model-specific prompt rendering or raw-token decoding happens here.
use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use deepseek_recipe::{
    anthropic::{
        MessagesRequest,
        messages::response::{MessagesChunkGenerator, MessagesResponse},
    },
    openai::{
        ResponsesRequest,
        responses::response::{ResponsesChunkGenerator, ResponsesResponse},
    },
    request::{ConversionOptions, ProtocolRequest, WebSearchBehavior},
    response::ProtocolResponse,
    stream::{ChunkGenerator, CompletionUsage, FinishReason, OutputChunk, PromptUsage},
    util::append_delta::AppendDelta,
};
use deepseek_recipe_core::{
    conversation::ResponseFormat,
    messages::InputMessage,
    multimodal::{IMAGE_SPECIAL_TOKEN, ImageSource},
    tools::ToolChoice,
};
use serde_json::{Value, json};

use crate::{error::GatewayError, sse};

const MAX_ADAPTER_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct Prepared {
    source: Value,
    messages: bool,
    // Chat function names cannot contain recipe's namespace separator.
    tool_names: HashMap<String, String>,
}

impl Prepared {
    pub(crate) fn is_messages(&self) -> bool {
        self.messages
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn new(endpoint: &str, source: &Value) -> Result<(Value, Self), GatewayError> {
        validate_content(source)?;
        let options = ConversionOptions::default()
            .with_default_thinking_mode(false)
            .with_responses_web_search(WebSearchBehavior::Reject);
        let converted = match endpoint {
            "messages" => serde_json::from_value::<MessagesRequest>(source.clone())
                .map_err(|error| invalid(&error))?
                .convert(options),
            "responses" => serde_json::from_value::<ResponsesRequest>(source.clone())
                .map_err(|error| invalid(&error))?
                .convert(options),
            _ => return Err(GatewayError::Internal),
        }
        .map_err(|error| match error {
            deepseek_recipe::request::ConversionError::BadRequest { detail } => {
                GatewayError::InvalidRequest(detail)
            }
            deepseek_recipe::request::ConversionError::Internal { .. } => GatewayError::Internal,
        })?;
        // Recipe intentionally drops some unsupported fields; the gateway must
        // reject them instead of silently promising unavailable semantics.
        if source.pointer("/text/format/type").and_then(Value::as_str) == Some("json_schema")
            || source
                .pointer("/output_config/format")
                .is_some_and(|v| !v.is_null())
            || source
                .get("logprobs")
                .is_some_and(|v| !v.is_null() && v != false)
        {
            return Err(GatewayError::UnsupportedFeature(
                "recipe constrained decoding/logprobs",
            ));
        }
        let conversation = converted.conversation;
        let mut tool_names = HashMap::new();
        let mut reverse_names = HashMap::new();
        for (index, tool) in conversation.tools.iter().enumerate() {
            let wire = format!("recipe_tool_{index}");
            tool_names.insert(wire.clone(), tool.name.clone());
            reverse_names.insert(tool.name.clone(), wire);
        }
        // Historical calls can refer to a tool no longer in the current list.
        for message in &conversation.messages {
            if let InputMessage::Assistant {
                tool_calls: Some(calls),
                ..
            } = message
            {
                for call in calls {
                    if !reverse_names.contains_key(&call.name) {
                        let wire = format!("recipe_tool_{}", reverse_names.len());
                        tool_names.insert(wire.clone(), call.name.clone());
                        reverse_names.insert(call.name.clone(), wire);
                    }
                }
            }
        }
        let messages = conversation.messages.iter().map(|message| {
            Ok(match message {
                InputMessage::System { content } => json!({"role":"system", "content":content}),
                InputMessage::LatestReminder { content } => json!({"role":"user", "content":content}),
                InputMessage::User { content, image_sources } => json!({"role":"user", "content": content_parts(content, image_sources)?}),
                InputMessage::Tool { content, image_sources, tool_call_id } => json!({"role":"tool", "tool_call_id":tool_call_id, "content":content_parts(content, image_sources)?}),
                InputMessage::Assistant { content, reasoning_content, tool_calls } => {
                    let mut message = json!({"role":"assistant", "content":content});
                    if let Some(reasoning) = reasoning_content { message["reasoning_content"] = json!(reasoning); }
                    if let Some(calls) = tool_calls {
                        message["tool_calls"] = json!(calls.iter().map(|call| json!({"id":call.id,"type":"function", "function":{"name":reverse_names[&call.name],"arguments":call.arguments}})).collect::<Vec<_>>());
                    }
                    message
                }
            })
        }).collect::<Result<Vec<_>, GatewayError>>()?;
        let mut body = json!({"model":source["model"], "messages":messages, "stream":converted.stream,
            "thinking":{"type":if conversation.thinking_mode {"enabled"} else {"disabled"}}});
        if converted.stream {
            body["stream_options"] = json!({"include_usage":true});
        }
        if let Some(effort) = conversation.reasoning_effort {
            body["reasoning_effort"] = json!(effort);
        }
        for (key, value) in [
            ("max_tokens", json!(converted.inference_options.max_tokens)),
            (
                "temperature",
                json!(converted.inference_options.temperature),
            ),
            ("top_p", json!(converted.inference_options.top_p)),
        ] {
            if !value.is_null() {
                body[key] = value;
            }
        }
        if let Some(disabled) = converted.inference_options.disable_parallel_tool_use {
            body["parallel_tool_calls"] = json!(!disabled);
        } else if let Some(parallel) = source.get("parallel_tool_calls") {
            body["parallel_tool_calls"] = parallel.clone();
        }
        let stops = &converted.parsing_options.stop_sequences;
        if !stops.is_empty() {
            body["stop"] = json!(stops);
        }
        if matches!(conversation.response_format, ResponseFormat::JsonObject) {
            body["response_format"] = json!({"type":"json_object"});
        }
        if !conversation.tools.is_empty() {
            body["tools"] = json!(
                conversation
                    .tools
                    .iter()
                    .map(|tool| {
                        let mut function =
                            json!({"name":reverse_names[&tool.name],"parameters":tool.parameters});
                        if let Some(description) = &tool.description {
                            function["description"] = json!(description);
                        }
                        if let Some(strict) = tool.strict {
                            function["strict"] = json!(strict);
                        }
                        json!({"type":"function", "function":function})
                    })
                    .collect::<Vec<_>>()
            );
            body["tool_choice"] = json!(match conversation.tool_choice {
                ToolChoice::Auto => "auto",
                ToolChoice::None => "none",
                ToolChoice::Required => "required",
            });
        }
        for key in [
            "cache_salt",
            "seed",
            "frequency_penalty",
            "presence_penalty",
        ] {
            if let Some(value) = source.get(key) {
                body[key] = value.clone();
            }
        }
        Ok((
            body,
            Self {
                source: source.clone(),
                messages: endpoint == "messages",
                tool_names,
            },
        ))
    }

    pub(crate) fn converter(
        &self,
        model: &str,
        accumulate: bool,
    ) -> Result<Converter, GatewayError> {
        let id = format!("recipe_{}", uuid::Uuid::now_v7().simple());
        let generator = if self.messages {
            Generator::Messages(MessagesChunkGenerator::new(&id, model, false))
        } else {
            let request: ResponsesRequest =
                serde_json::from_value(self.source.clone()).map_err(|error| invalid(&error))?;
            // Use the library's normalized parsing and custom-tool metadata.
            let custom = request.custom_tool_names();
            let converted = request
                .convert(
                    ConversionOptions::default()
                        .with_default_thinking_mode(false)
                        .with_responses_web_search(WebSearchBehavior::Reject),
                )
                .map_err(|e| GatewayError::InvalidRequest(e.to_string()))?;
            Generator::Responses(Box::new(
                ResponsesRequest::chunk_generator(&converted, id.clone(), model.to_owned())
                    .with_custom_tool_names(custom),
            ))
        };
        let response = accumulate.then(|| {
            if self.messages {
                Accumulator::Messages(MessagesResponse::new(id.clone(), model.to_owned(), 0, 0, 0))
            } else {
                Accumulator::Responses(ResponsesResponse::new(id, model.to_owned(), 0, 0, 0))
            }
        });
        Ok(Converter {
            generator,
            response,
            tool_names: self.tool_names.clone(),
            tools: BTreeMap::new(),
            started: false,
            finished: false,
            finish_reason: None,
            prompt_usage: PromptUsage::default(),
            completion_tokens: 0,
            bytes: 0,
            expose_thinking: !self.messages || crate::anthropic::thinking_requested(&self.source),
        })
    }

    pub(crate) async fn complete(&self, bytes: &[u8], model: &str) -> Result<Bytes, GatewayError> {
        let mut value: Value =
            serde_json::from_slice(bytes).map_err(|_| GatewayError::InvalidUpstreamResponse)?;
        let choice = value
            .pointer_mut("/choices/0")
            .ok_or(GatewayError::InvalidUpstreamResponse)?;
        choice["delta"] = choice["message"].take();
        let mut converter = self.converter(model, true)?;
        converter.push(&value).await?;
        converter.finish().await?;
        let result = match converter.response.expect("buffered accumulator") {
            Accumulator::Messages(response) => serde_json::to_vec(&response),
            Accumulator::Responses(response) => serde_json::to_vec(&response),
        };
        result.map(Bytes::from).map_err(|_| GatewayError::Internal)
    }
}

fn validate_content(value: &Value) -> Result<(), GatewayError> {
    match value {
        Value::Array(values) => {
            for value in values {
                validate_content(value)?;
            }
        }
        Value::Object(object) => {
            let kind = object.get("type").and_then(Value::as_str);
            if matches!(kind, Some("document" | "input_file" | "redacted_thinking"))
                || (kind == Some("reasoning")
                    && object
                        .get("encrypted_content")
                        .is_some_and(|value| !value.is_null()))
            {
                return Err(GatewayError::UnsupportedFeature(
                    "recipe document or encrypted reasoning content",
                ));
            }
            for key in ["input", "messages", "content", "system", "output"] {
                if let Some(value) = object.get(key) {
                    validate_content(value)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn invalid(error: &serde_json::Error) -> GatewayError {
    GatewayError::InvalidRequest(error.to_string())
}
fn content_parts(content: &str, images: &[ImageSource]) -> Result<Value, GatewayError> {
    if images.is_empty() {
        return Ok(json!(content));
    }
    let segments = content.split(IMAGE_SPECIAL_TOKEN).collect::<Vec<_>>();
    if segments.len() != images.len() + 1 {
        return Err(GatewayError::Internal);
    }
    let mut parts = Vec::new();
    for (index, segment) in segments.iter().enumerate() {
        if !segment.is_empty() {
            parts.push(json!({"type":"text","text":segment}));
        }
        if let Some(image) = images.get(index) {
            let url = match image {
                ImageSource::DataUrl { data_url, .. } => data_url,
                ImageSource::Url { url, .. } => url,
                ImageSource::Bytes { .. } => {
                    return Err(GatewayError::UnsupportedFeature("untyped image bytes"));
                }
            };
            parts.push(json!({"type":"image_url","image_url":{"url":url,"detail":image.detail()}}));
        }
    }
    Ok(json!(parts))
}

enum Generator {
    Messages(MessagesChunkGenerator),
    Responses(Box<ResponsesChunkGenerator>),
}
enum Accumulator {
    Messages(MessagesResponse),
    Responses(ResponsesResponse),
}
#[derive(Default)]
struct Tool {
    name: String,
    arguments: String,
}

pub(crate) struct Converter {
    generator: Generator,
    response: Option<Accumulator>,
    tool_names: HashMap<String, String>,
    tools: BTreeMap<u64, Tool>,
    started: bool,
    finished: bool,
    finish_reason: Option<FinishReason>,
    prompt_usage: PromptUsage,
    completion_tokens: usize,
    bytes: usize,
    expose_thinking: bool,
}

impl Converter {
    async fn emit(&mut self, chunk: OutputChunk) -> Result<Vec<sse::Event>, GatewayError> {
        match &mut self.generator {
            Generator::Messages(generator) => {
                let mut events = generator.generate(chunk).await;
                for event in &mut events {
                    if let deepseek_recipe::anthropic::messages::response::MessagesStreamEvent::MessageDelta { usage, .. } = event {
                        *usage = (self.prompt_usage, CompletionUsage { completion_tokens: self.completion_tokens }).into();
                    }
                }
                let output = events
                    .iter()
                    .map(|event| {
                        serde_json::to_value(event).map(|value| {
                            sse::Event::json(
                                MessagesResponse::chunk_event_type(event).unwrap_or("message"),
                                &value,
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>();
                if let Some(Accumulator::Messages(response)) = &mut self.response {
                    for event in events {
                        response.append(event);
                    }
                }
                output.map_err(|_| GatewayError::Internal)
            }
            Generator::Responses(generator) => {
                let mut events = generator.generate(chunk).await;
                for event in &mut events {
                    use deepseek_recipe::openai::responses::response::ResponsesStreamEvent;
                    if let ResponsesStreamEvent::Completed { response, .. }
                    | ResponsesStreamEvent::Incomplete { response, .. } = event
                    {
                        response.usage = Some(
                            (
                                self.prompt_usage,
                                CompletionUsage {
                                    completion_tokens: self.completion_tokens,
                                },
                            )
                                .into(),
                        );
                    }
                }
                let output = events
                    .iter()
                    .map(|event| {
                        serde_json::to_value(event).map(|value| {
                            sse::Event::json(
                                ResponsesResponse::chunk_event_type(event).unwrap_or("message"),
                                &value,
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>();
                if let Some(Accumulator::Responses(response)) = &mut self.response {
                    for event in events {
                        response.append(event);
                    }
                }
                output.map_err(|_| GatewayError::Internal)
            }
        }
    }

    pub(crate) async fn push_event(
        &mut self,
        event: &sse::Event,
    ) -> Result<Vec<sse::Event>, GatewayError> {
        if event.data() == "[DONE]" {
            return self.finish().await;
        }
        let value = serde_json::from_str(event.data())
            .map_err(|_| GatewayError::InvalidUpstreamResponse)?;
        self.push(&value).await
    }

    async fn push(&mut self, value: &Value) -> Result<Vec<sse::Event>, GatewayError> {
        if self.finished {
            return Ok(Vec::new());
        }
        self.bytes = self.bytes.saturating_add(value.to_string().len());
        if self.bytes > MAX_ADAPTER_BYTES || value.get("error").is_some() {
            return Err(GatewayError::InvalidUpstreamResponse);
        }
        if let Some(usage) = value.get("usage").filter(|v| !v.is_null()) {
            self.prompt_usage = PromptUsage {
                prompt_tokens: count(usage, "prompt_tokens"),
                prompt_cache_hit_tokens: usage
                    .pointer("/prompt_tokens_details/cached_tokens")
                    .and_then(Value::as_u64)
                    .and_then(|n| usize::try_from(n).ok())
                    .unwrap_or_else(|| count(usage, "prompt_cache_hit_tokens")),
            };
            self.completion_tokens = count(usage, "completion_tokens");
        }
        let choices = value["choices"]
            .as_array()
            .ok_or(GatewayError::InvalidUpstreamResponse)?;
        if choices.len() > 1 {
            return Err(GatewayError::InvalidUpstreamResponse);
        }
        let mut events = Vec::new();
        if !self.started {
            events.extend(
                self.emit(OutputChunk::Start {
                    system_fingerprint: value["system_fingerprint"].as_str().map(str::to_owned),
                    usage: self.prompt_usage,
                })
                .await?,
            );
            self.started = true;
        }
        let Some(choice) = choices.first() else {
            return Ok(events);
        };
        if choice["index"].as_u64().is_some_and(|index| index != 0) {
            return Err(GatewayError::InvalidUpstreamResponse);
        }
        if let Some(reason) = choice["finish_reason"].as_str() {
            self.finish_reason = Some(match reason {
                "stop" => FinishReason::Stop,
                "length" => FinishReason::Length,
                "tool_calls" => FinishReason::ToolCalls,
                "content_filter" => FinishReason::ContentFilter,
                _ => return Err(GatewayError::InvalidUpstreamResponse),
            });
        }
        let delta = choice["delta"]
            .as_object()
            .ok_or(GatewayError::InvalidUpstreamResponse)?;
        if self.expose_thinking
            && let Some(content) = delta
                .get("reasoning_content")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
        {
            events.extend(
                self.emit(OutputChunk::Reasoning {
                    content: content.to_owned(),
                })
                .await?,
            );
        }
        if let Some(content) = delta
            .get("content")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            events.extend(
                self.emit(OutputChunk::Raw {
                    content: content.to_owned(),
                })
                .await?,
            );
        }
        if let Some(calls) = delta.get("tool_calls") {
            for (position, call) in calls
                .as_array()
                .ok_or(GatewayError::InvalidUpstreamResponse)?
                .iter()
                .enumerate()
            {
                let index = call["index"].as_u64().unwrap_or(position as u64);
                let tool = self.tools.entry(index).or_default();
                if let Some(name) = call.pointer("/function/name").and_then(Value::as_str) {
                    tool.name.push_str(name);
                }
                if let Some(arguments) = call.pointer("/function/arguments").and_then(Value::as_str)
                {
                    tool.arguments.push_str(arguments);
                }
            }
        }
        Ok(events)
    }

    pub(crate) async fn finish(&mut self) -> Result<Vec<sse::Event>, GatewayError> {
        if self.finished {
            return Ok(Vec::new());
        }
        let reason = self
            .finish_reason
            .ok_or(GatewayError::InvalidUpstreamResponse)?;
        let mut events = Vec::new();
        for (_, tool) in std::mem::take(&mut self.tools) {
            let name = self
                .tool_names
                .get(&tool.name)
                .ok_or(GatewayError::InvalidUpstreamResponse)?
                .clone();
            if !serde_json::from_str::<Value>(&tool.arguments).is_ok_and(|value| value.is_object())
            {
                return Err(GatewayError::InvalidUpstreamResponse);
            }
            events.extend(
                self.emit(OutputChunk::ToolCall {
                    tool_name: name,
                    arguments: tool.arguments,
                })
                .await?,
            );
        }
        events.extend(
            self.emit(OutputChunk::Finish {
                reason,
                stop_sequence: None,
                usage: CompletionUsage {
                    completion_tokens: self.completion_tokens,
                },
            })
            .await?,
        );
        self.finished = true;
        Ok(events)
    }
}
fn count(value: &Value, key: &str) -> usize {
    value[key]
        .as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_images_and_tool_history_without_prompt_encoding() {
        let source = json!({"model":"ds","input":[
            {"role":"user","content":[{"type":"input_text","text":"look"},{"type":"input_image","image_url":"https://example.com/test.png"}]},
            {"type":"function_call","call_id":"call_1","name":"read","arguments":"{}"},
            {"type":"function_call_output","call_id":"call_1","output":"done"}
        ],"tools":[{"type":"function","name":"read","parameters":{"type":"object"}}]});
        let (body, _) = Prepared::new("responses", &source).unwrap();
        assert!(body.get("prompt").is_none());
        assert_eq!(
            body["messages"][0]["content"][1]["image_url"]["url"],
            "https://example.com/test.png"
        );
        assert_eq!(body["messages"][2]["tool_call_id"], "call_1");
        assert_eq!(
            body["messages"][1]["tool_calls"][0]["function"]["name"],
            body["tools"][0]["function"]["name"]
        );
    }

    #[tokio::test]
    async fn truncated_stream_does_not_emit_success() {
        let (_, prepared) =
            Prepared::new("responses", &json!({"model":"ds","input":"hello"})).unwrap();
        let mut converter = prepared.converter("ds", false).unwrap();
        converter
            .push(&json!({"choices":[{"delta":{"content":"partial"},"finish_reason":null}]}))
            .await
            .unwrap();
        assert!(converter.finish().await.is_err());
    }

    #[tokio::test]
    async fn custom_apply_patch_remains_a_custom_call() {
        let (body, prepared) = Prepared::new(
            "responses",
            &json!({"model":"ds","input":"patch","tools":[{"type":"custom","name":"apply_patch"}]}),
        )
        .unwrap();
        let arguments = json!({"input":"*** Begin Patch\n*** End Patch"}).to_string();
        let response = json!({"choices":[{"message":{"content":null,"tool_calls":[{"function":{"name":body["tools"][0]["function"]["name"],"arguments":arguments}}]},"finish_reason":"tool_calls"}]});
        let result: Value = serde_json::from_slice(
            &prepared
                .complete(response.to_string().as_bytes(), "ds")
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(result["output"][0]["type"], "custom_tool_call");
        assert_eq!(result["output"][0]["name"], "apply_patch");
        assert_eq!(
            result["output"][0]["input"],
            "*** Begin Patch\n*** End Patch"
        );
    }

    #[test]
    fn rejects_hosted_tools_and_encrypted_reasoning() {
        for request in [
            json!({"model":"ds","input":"hello","tools":[{"type":"web_search"}]}),
            json!({"model":"ds","input":[{"type":"reasoning","encrypted_content":"opaque"}]}),
        ] {
            assert!(Prepared::new("responses", &request).is_err());
        }
    }
}
