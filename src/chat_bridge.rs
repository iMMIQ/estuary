//! Client protocol adapters. All generation protocols use the Chat backend.
use std::collections::{BTreeMap, HashMap, HashSet};

use bytes::Bytes;
use deepseek_recipe::{
    anthropic::messages::{
        request::MessagesRequest,
        response::{MessagesChunkGenerator, MessagesResponse},
    },
    openai::responses::{
        request::ResponsesRequest,
        response::{ResponsesChunkGenerator, ResponsesResponse},
    },
    request::{ConversationRequest, ConversionOptions, ProtocolRequest, WebSearchBehavior},
    response::ProtocolResponse,
    stream::{ChunkGenerator, CompletionUsage, FinishReason, OutputChunk, PromptUsage},
    util::append_delta::AppendDelta,
};
use deepseek_recipe_core::{
    messages::InputMessage,
    multimodal::{IMAGE_SPECIAL_TOKEN, ImageSource},
};
use serde_json::{Value, json};

use crate::{error::GatewayError, sse};

#[derive(Clone, Debug)]
pub(crate) struct Context {
    pub(crate) messages: bool,
    thinking: bool,
    custom_tools: HashSet<String>,
    tool_names: HashMap<String, String>,
}

fn invalid(error: impl std::fmt::Display) -> GatewayError {
    GatewayError::InvalidRequest(error.to_string())
}

/// Convert once, before prefix lookup and node selection. Chat callers keep
/// their model-specific options unchanged; protocol adapters use shared types.
pub(crate) fn prepare(
    endpoint: &str,
    source: &Value,
) -> Result<(Value, Option<Context>), GatewayError> {
    if !matches!(endpoint, "messages" | "messages/count_tokens" | "responses") {
        return Ok((source.clone(), None));
    }
    reject_lossy_input(source)?;
    if source["tool_choice"]["type"]
        .as_str()
        .is_some_and(|kind| kind.starts_with("web_search"))
    {
        return Err(invalid(
            "server search tool selection is not supported by the Chat backend",
        ));
    }
    let options = ConversionOptions::new()
        .with_default_thinking_mode(false)
        .with_responses_web_search(WebSearchBehavior::Reject);
    let messages = endpoint != "responses";
    if endpoint == "messages" && source.get("max_tokens").and_then(Value::as_u64).is_none() {
        return Err(invalid("max_tokens is required for Messages"));
    }
    let (request, custom_tools) = if messages {
        let mut request: MessagesRequest =
            serde_json::from_value(source.clone()).map_err(invalid)?;
        // Tool selection belongs to Chat/vLLM; recipe applies DeepSeek prompt policies.
        request.tool_choice = None;
        (request.convert(options).map_err(invalid)?, HashSet::new())
    } else {
        let mut request: ResponsesRequest =
            serde_json::from_value(source.clone()).map_err(invalid)?;
        request.tool_choice = None;
        let names = request.custom_tool_names();
        (request.convert(options).map_err(invalid)?, names)
    };
    let choice = &source["tool_choice"];
    if request.conversation.tools.is_empty()
        && (choice == "required" || choice["type"] == "any" || choice.get("name").is_some())
    {
        return Err(invalid(
            "required or named tool selection needs an available tool",
        ));
    }
    let mut context = Context {
        messages,
        thinking: request.conversation.thinking_mode,
        custom_tools,
        tool_names: HashMap::new(),
    };
    let chat = serialize_chat(&request, source, &mut context)?;
    if source["thinking"]["display"] == "omitted" {
        context.thinking = false;
    }
    Ok((chat, Some(context)))
}

fn reject_lossy_input(value: &Value) -> Result<(), GatewayError> {
    match value {
        Value::Array(items) => {
            for item in items {
                reject_lossy_input(item)?;
            }
        }
        Value::Object(object) => {
            if matches!(
                object.get("type").and_then(Value::as_str),
                Some("document" | "input_file" | "web_search_call" | "redacted_thinking")
            ) {
                return Err(invalid(
                    "document, file, server search and redacted thinking blocks cannot be represented by the Chat backend",
                ));
            }
            if object
                .get("encrypted_content")
                .is_some_and(|v| !v.is_null())
                && object
                    .get("content")
                    .and_then(Value::as_array)
                    .is_none_or(Vec::is_empty)
            {
                return Err(invalid(
                    "encrypted-only reasoning requires the original provider; supply plaintext reasoning history",
                ));
            }
            for (key, item) in object {
                // Tool JSON schemas and arbitrary tool arguments are user data.
                if !matches!(
                    key.as_str(),
                    "input" | "parameters" | "input_schema" | "schema" | "metadata"
                ) || item.is_array()
                {
                    reject_lossy_input(item)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn content(text: &str, images: &[ImageSource]) -> Result<Value, GatewayError> {
    if images.is_empty() {
        return Ok(Value::String(text.into()));
    }
    let parts: Vec<_> = text.split(IMAGE_SPECIAL_TOKEN).collect();
    if parts.len() != images.len() + 1 {
        return Err(invalid("image placeholder count mismatch"));
    }
    let mut blocks = Vec::new();
    for (index, part) in parts.iter().enumerate() {
        // The shared representation joins content blocks with two newlines.
        // Remove those separators at image boundaries, preserving user text.
        let part = if index > 0 {
            part.strip_prefix("\n\n").unwrap_or(part)
        } else {
            part
        };
        let part = if index < images.len() {
            part.strip_suffix("\n\n").unwrap_or(part)
        } else {
            part
        };
        if !part.is_empty() {
            blocks.push(json!({"type":"text","text":part}));
        }
        if let Some(image) = images.get(index) {
            let url = match image {
                ImageSource::DataUrl { data_url, .. } => data_url,
                ImageSource::Url { url, .. } => url,
                ImageSource::Bytes { .. } => {
                    return Err(invalid("raw image bytes require a declared media type"));
                }
            };
            blocks
                .push(json!({"type":"image_url","image_url":{"url":url,"detail":image.detail()}}));
        }
    }
    Ok(Value::Array(blocks))
}

fn tool_name(name: &str, context: &mut Context) -> String {
    // Hash namespace names to avoid invalid Chat names and flattening collisions.
    if let Some((safe, _)) = context
        .tool_names
        .iter()
        .find(|(_, original)| original.as_str() == name)
    {
        return safe.clone();
    }
    let base = format!("estuary_{}", &blake3::hash(name.as_bytes()).to_hex()[..32]);
    let mut safe = if name.contains("::") {
        base.clone()
    } else {
        name.to_owned()
    };
    let mut suffix = 0;
    while context.tool_names.contains_key(&safe) {
        suffix += 1;
        safe = format!("{base}_{suffix}");
    }

    context.tool_names.insert(safe.clone(), name.to_owned());
    safe
}

// Messages content blocks can become adjacent User entries in the shared
// representation. Keep them in one Chat turn for templates requiring alternation.
fn coalesce_users(messages: Vec<Value>) -> Vec<Value> {
    fn parts(value: Value) -> Vec<Value> {
        match value {
            Value::Array(parts) => parts,
            value => vec![json!({"type":"text","text":value})],
        }
    }
    let mut result: Vec<Value> = Vec::new();
    for mut message in messages {
        if message["role"] == "user"
            && let Some(previous) = result.last_mut().filter(|v| v["role"] == "user")
        {
            let mut content = parts(previous["content"].take());
            content.extend(parts(message["content"].take()));
            previous["content"] = content.into();
        } else {
            result.push(message);
        }
    }
    result
}

#[allow(clippy::too_many_lines)]
fn serialize_chat(
    request: &ConversationRequest,
    source: &Value,
    context: &mut Context,
) -> Result<Value, GatewayError> {
    let mut messages = Vec::new();
    for message in &request.conversation.messages {
        messages.push(match message {
            InputMessage::System { content } => json!({"role":"system","content":content}),
            InputMessage::User { content: text, image_sources } => json!({"role":"user","content":content(text,image_sources)?}),
            InputMessage::Tool { content: text, image_sources, tool_call_id } => json!({"role":"tool","content":content(text,image_sources)?,"tool_call_id":tool_call_id}),
            InputMessage::LatestReminder { content } => json!({"role":"user","content":format!("<system-reminder>\n{content}\n</system-reminder>")}),
            InputMessage::Assistant { content, reasoning_content, tool_calls } => {
                let mut value = json!({"role":"assistant","content":content});
                if let Some(reasoning) = reasoning_content { value["reasoning_content"] = reasoning.clone().into(); }
                if let Some(calls) = tool_calls {
                    value["tool_calls"] = calls.iter().map(|call| json!({"id":call.id,"type":"function","function":{"name":tool_name(&call.name,context),"arguments":call.arguments}})).collect();
                }
                value
            }
        });
    }
    let mut chat = json!({"model":source["model"],"messages":coalesce_users(messages),"stream":request.stream});
    let options = &request.inference_options;
    if let Some(max) = options.max_tokens {
        chat["max_tokens"] = max.into();
    }
    if let Some(value) = source.get("temperature") {
        chat["temperature"] = value.clone();
    }
    if let Some(value) = source.get("top_p") {
        chat["top_p"] = value.clone();
    }
    if !request.conversation.tools.is_empty() {
        chat["tools"] = request
            .conversation
            .tools
            .iter()
            .map(|tool| {
                let mut function =
                    json!({"name":tool_name(&tool.name,context),"parameters":tool.parameters});
                if let Some(value) = &tool.description {
                    function["description"] = value.clone().into();
                }
                if let Some(value) = tool.strict {
                    function["strict"] = value.into();
                }
                json!({"type":"function","function":function})
            })
            .collect();
        chat["tool_choice"] = "auto".into();
        // Preserve API semantics where recipe's DeepSeek policies relax them.
        let choice = &source["tool_choice"];
        if choice == "required" || (context.messages && choice["type"] == "any") {
            chat["tool_choice"] = "required".into();
        }
        if choice == "none" || choice["type"] == "none" {
            chat["tool_choice"] = "none".into();
        }
        if let Some(name) = choice["name"].as_str() {
            let name = choice["namespace"].as_str().map_or_else(
                || name.to_owned(),
                |namespace| format!("{namespace}::{name}"),
            );
            if !request
                .conversation
                .tools
                .iter()
                .any(|tool| tool.name == name)
            {
                return Err(invalid(format!(
                    "tool_choice: no tool named '{name}' was specified"
                )));
            }
            chat["tool_choice"] =
                json!({"type":"function","function":{"name":tool_name(&name,context)}});
        }
        if let Some(disabled) = source["tool_choice"]["disable_parallel_tool_use"].as_bool() {
            chat["parallel_tool_calls"] = (!disabled).into();
        }
        if let Some(parallel) = source.get("parallel_tool_calls") {
            chat["parallel_tool_calls"] = parallel.clone();
        }
    }
    if let Some(stop) = source.get("stop_sequences") {
        chat["stop"] = stop.clone();
    }
    let explicit_thinking = source.get("thinking").is_some_and(|v| !v.is_null())
        || source.get("reasoning").is_some_and(|v| !v.is_null())
        || source["output_config"].get("effort").is_some();
    if let Some(kwargs) = source.get("chat_template_kwargs") {
        chat["chat_template_kwargs"] = kwargs.clone();
    }
    if explicit_thinking {
        chat["chat_template_kwargs"]["enable_thinking"] = context.thinking.into();
    }
    if let Some(budget) = options.thinking_budget_tokens {
        chat["chat_template_kwargs"]["thinking_budget"] = budget.into();
    }
    if let Some(effort) = source["reasoning"]
        .get("effort")
        .or_else(|| source["output_config"].get("effort"))
    {
        chat["reasoning_effort"] = effort.clone();
    }
    if let Some(format) = source["text"].get("format") {
        chat["response_format"] = if format["type"] == "json_schema" {
            let mut schema = format.clone();
            schema
                .as_object_mut()
                .expect("format is an object")
                .remove("type");
            json!({"type":"json_schema","json_schema":schema})
        } else {
            format.clone()
        };
    }
    if let Some(format) = source["output_config"].get("format") {
        chat["response_format"] = if format["type"] == "json_schema" {
            json!({"type":"json_schema","json_schema":{"name":"response","schema":format["schema"],"strict":true}})
        } else {
            format.clone()
        };
    }
    for key in [
        "seed",
        "top_k",
        "min_p",
        "frequency_penalty",
        "presence_penalty",
        "repetition_penalty",
        "user",
        "priority",
        "cache_salt",
        "add_generation_prompt",
        "continue_final_message",
    ] {
        if let Some(value) = source.get(key) {
            chat[key] = value.clone();
        }
    }
    if request.stream {
        chat["stream_options"] = json!({"include_usage":true});
    }
    Ok(chat)
}

#[derive(Default)]
struct Usage {
    input: usize,
    cached: usize,
    created: usize,
    output: usize,
    reasoning: usize,
}
impl Usage {
    fn observe(&mut self, value: &Value) {
        if let Some(n) = value["prompt_tokens"].as_u64() {
            self.input = usize::try_from(n).unwrap_or(usize::MAX);
        }
        if let Some(n) = value["prompt_tokens_details"]["cached_tokens"]
            .as_u64()
            .or_else(|| value["prompt_cache_hit_tokens"].as_u64())
        {
            self.cached = usize::try_from(n).unwrap_or(usize::MAX);
        }
        if let Some(n) = value["prompt_tokens_details"]["created_cache_tokens"]
            .as_u64()
            .or_else(|| value["prompt_tokens_details"]["cache_write_tokens"].as_u64())
        {
            self.created = usize::try_from(n).unwrap_or(usize::MAX);
        }
        if let Some(n) = value["completion_tokens"].as_u64() {
            self.output = usize::try_from(n).unwrap_or(usize::MAX);
        }
        if let Some(n) = value["completion_tokens_details"]["reasoning_tokens"].as_u64() {
            self.reasoning = usize::try_from(n).unwrap_or(usize::MAX);
        }
    }
    fn value(&self, messages: bool) -> Value {
        if messages {
            json!({"input_tokens":self.input.saturating_sub(self.cached).saturating_sub(self.created),"cache_read_input_tokens":self.cached,"cache_creation_input_tokens":self.created,"output_tokens":self.output})
        } else {
            json!({"input_tokens":self.input,"input_tokens_details":{"cached_tokens":self.cached,"cache_write_tokens":self.created},"output_tokens":self.output,"output_tokens_details":{"reasoning_tokens":self.reasoning},"total_tokens":self.input.saturating_add(self.output)})
        }
    }
}

enum Generator {
    Messages {
        generator: MessagesChunkGenerator,
        response: MessagesResponse,
    },
    Responses {
        generator: Box<ResponsesChunkGenerator>,
        response: ResponsesResponse,
    },
}
#[derive(Default)]
struct Tool {
    name: String,
    arguments: String,
}

pub(crate) struct StreamConverter {
    generator: Generator,
    context: Context,
    usage: Usage,
    tools: BTreeMap<usize, Tool>,
    started: bool,
    finished: bool,
    reason: Option<FinishReason>,
    buffered_bytes: usize,
    provider_metrics: Option<Value>,
}
impl StreamConverter {
    pub(crate) fn new(context: Context, model: String) -> Self {
        let id = format!(
            "{}{}",
            if context.messages { "msg_" } else { "resp_" },
            uuid::Uuid::now_v7().simple()
        );
        let generator = if context.messages {
            Generator::Messages {
                generator: MessagesChunkGenerator::new(id.clone(), model.clone(), false),
                response: MessagesResponse::new(id, model, 0, 0, 0),
            }
        } else {
            Generator::Responses {
                generator: Box::new(
                    ResponsesChunkGenerator::new(id.clone(), model.clone())
                        .with_custom_tool_names(context.custom_tools.clone()),
                ),
                response: ResponsesResponse::new(id, model, 0, 0, 0),
            }
        };
        Self {
            generator,
            context,
            usage: Usage::default(),
            tools: BTreeMap::new(),
            started: false,
            finished: false,
            reason: None,
            buffered_bytes: 0,
            provider_metrics: None,
        }
    }
    async fn emit(&mut self, chunk: OutputChunk) -> Result<Vec<sse::Event>, GatewayError> {
        let mut values = Vec::new();
        match &mut self.generator {
            Generator::Messages {
                generator,
                response,
            } => {
                for event in generator.generate(chunk).await {
                    let name = MessagesResponse::chunk_event_type(&event).unwrap_or_default();
                    let value = serde_json::to_value(&event).map_err(|_| GatewayError::Internal)?;
                    response.append(event);
                    values.push((name, value));
                }
            }
            Generator::Responses {
                generator,
                response,
            } => {
                for event in generator.generate(chunk).await {
                    let name = ResponsesResponse::chunk_event_type(&event).unwrap_or_default();
                    let value = serde_json::to_value(&event).map_err(|_| GatewayError::Internal)?;
                    response.append(event);
                    values.push((name, value));
                }
            }
        }
        for (_, value) in &mut values {
            match value["type"].as_str() {
                Some("message_delta") => value["usage"] = self.usage.value(true),
                Some("response.completed" | "response.incomplete") => {
                    value["response"]["usage"] = self.usage.value(false);
                    if let Some(metrics) = &self.provider_metrics {
                        value["response"]["metrics"] = metrics.clone();
                    }
                }
                _ => {}
            }
        }
        Ok(values
            .iter()
            .map(|(name, value)| sse::Event::json(*name, value))
            .collect())
    }
    async fn start(&mut self) -> Result<Vec<sse::Event>, GatewayError> {
        if self.started {
            return Ok(Vec::new());
        }
        self.started = true;
        self.emit(OutputChunk::Start {
            system_fingerprint: None,
            usage: PromptUsage {
                prompt_tokens: self.usage.input,
                prompt_cache_hit_tokens: self.usage.cached,
            },
        })
        .await
    }
    pub(crate) async fn push_event(
        &mut self,
        event: &sse::Event,
    ) -> Result<Vec<sse::Event>, GatewayError> {
        if event.data() == "[DONE]" {
            return self.finish().await;
        }
        let value: Value = serde_json::from_str(event.data())
            .map_err(|_| GatewayError::InvalidUpstreamResponse)?;
        self.push(&value).await
    }
    async fn push(&mut self, value: &Value) -> Result<Vec<sse::Event>, GatewayError> {
        if self.finished || value.get("error").is_some() {
            return Err(GatewayError::InvalidUpstreamResponse);
        }
        self.usage.observe(&value["usage"]);
        if let Some(metrics) = value.get("metrics") {
            self.provider_metrics = Some(metrics.clone());
        }
        let choices = value["choices"]
            .as_array()
            .ok_or(GatewayError::InvalidUpstreamResponse)?;
        if choices.len() > 1 {
            return Err(GatewayError::InvalidUpstreamResponse);
        }
        let mut events = self.start().await?;
        for choice in choices {
            if choice["index"].as_u64().unwrap_or(0) != 0 {
                return Err(GatewayError::InvalidUpstreamResponse);
            }
            let delta = choice
                .get("delta")
                .or_else(|| choice.get("message"))
                .ok_or(GatewayError::InvalidUpstreamResponse)?;
            for (key, reasoning) in [
                ("reasoning_content", true),
                ("reasoning", true),
                ("content", false),
            ] {
                if let Some(text) = delta[key].as_str() {
                    self.buffered_bytes = self.buffered_bytes.saturating_add(text.len());
                    if self.buffered_bytes > 16 * 1024 * 1024 {
                        return Err(GatewayError::InvalidUpstreamResponse);
                    }
                    if !reasoning || self.context.thinking || !self.context.messages {
                        events.extend(
                            self.emit(if reasoning {
                                OutputChunk::Reasoning {
                                    content: text.into(),
                                }
                            } else {
                                OutputChunk::Raw {
                                    content: text.into(),
                                }
                            })
                            .await?,
                        );
                    }
                }
            }
            if let Some(calls) = delta["tool_calls"].as_array() {
                for (offset, call) in calls.iter().enumerate() {
                    let index = usize::try_from(call["index"].as_u64().unwrap_or(offset as u64))
                        .map_err(|_| GatewayError::InvalidUpstreamResponse)?;
                    if index > 1024 || self.tools.len() > 1024 {
                        return Err(GatewayError::InvalidUpstreamResponse);
                    }
                    let tool = self.tools.entry(index).or_default();
                    if let Some(name) = call["function"]["name"].as_str() {
                        tool.name.push_str(name);
                        self.buffered_bytes += name.len();
                    }
                    if let Some(args) = call["function"]["arguments"].as_str() {
                        tool.arguments.push_str(args);
                        self.buffered_bytes += args.len();
                    }
                    if self.buffered_bytes > 16 * 1024 * 1024 {
                        return Err(GatewayError::InvalidUpstreamResponse);
                    }
                }
            }
            if let Some(reason) = choice["finish_reason"].as_str() {
                self.reason = Some(match reason {
                    "stop" => FinishReason::Stop,
                    "length" => FinishReason::Length,
                    "tool_calls" => FinishReason::ToolCalls,
                    "content_filter" => FinishReason::ContentFilter,
                    _ => return Err(GatewayError::InvalidUpstreamResponse),
                });
            }
        }
        Ok(events)
    }
    pub(crate) async fn finish(&mut self) -> Result<Vec<sse::Event>, GatewayError> {
        if self.finished {
            return Ok(Vec::new());
        }
        let mut reason = self.reason.ok_or(GatewayError::InvalidUpstreamResponse)?;
        let mut events = self.start().await?;
        for (_, tool) in std::mem::take(&mut self.tools) {
            if tool.name.is_empty() || serde_json::from_str::<Value>(&tool.arguments).is_err() {
                return Err(GatewayError::InvalidUpstreamResponse);
            }
            let name = self
                .context
                .tool_names
                .get(&tool.name)
                .cloned()
                .unwrap_or(tool.name);
            events.extend(
                self.emit(OutputChunk::ToolCall {
                    tool_name: name,
                    arguments: tool.arguments,
                })
                .await?,
            );
            if reason == FinishReason::Stop {
                reason = FinishReason::ToolCalls;
            }
        }
        events.extend(
            self.emit(OutputChunk::Finish {
                reason,
                stop_sequence: None,
                usage: CompletionUsage {
                    completion_tokens: self.usage.output,
                },
            })
            .await?,
        );
        self.finished = true;
        Ok(events)
    }
    fn response(&self) -> Result<Bytes, GatewayError> {
        let mut value = match &self.generator {
            Generator::Messages { response, .. } => serde_json::to_value(response),
            Generator::Responses { response, .. } => serde_json::to_value(response),
        }
        .map_err(|_| GatewayError::Internal)?;
        value["usage"] = self.usage.value(self.context.messages);
        if let Some(metrics) = &self.provider_metrics {
            value["metrics"] = metrics.clone();
        }
        serde_json::to_vec(&value)
            .map(Bytes::from)
            .map_err(|_| GatewayError::Internal)
    }
}

pub(crate) async fn convert_response(
    body: &[u8],
    context: &Context,
    model: &str,
) -> Result<Bytes, GatewayError> {
    let value: Value =
        serde_json::from_slice(body).map_err(|_| GatewayError::InvalidUpstreamResponse)?;
    let mut converter = StreamConverter::new(context.clone(), model.into());
    converter.push(&value).await?;
    converter.finish().await?;
    converter.response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_normalization_preserves_history_and_discards_cache_markers() {
        let source = json!({"model":"any-model","max_tokens":64,"thinking":{"type":"enabled","budget_tokens":32},
            "system":[{"type":"text","text":"stable","cache_control":{"type":"ephemeral"}}],
            "messages":[{"role":"user","content":"question"},{"role":"assistant","content":[{"type":"thinking","thinking":"reason","signature":"opaque"},{"type":"text","text":"answer"}]},{"role":"system","content":"new reminder"},{"role":"user","content":"continue"}]});
        let (chat, _) = prepare("messages", &source).unwrap();
        assert_eq!(
            chat["messages"][0],
            json!({"role":"system","content":"stable"})
        );
        assert_eq!(chat["messages"][2]["reasoning_content"], "reason");
        assert_eq!(chat["messages"][3]["role"], "user");
        assert!(
            chat["messages"][3]["content"]
                .to_string()
                .contains("new reminder")
        );
        assert_eq!(chat["chat_template_kwargs"]["enable_thinking"], true);
        assert!(!chat.to_string().contains("cache_control"));
        let mut other = source.clone();
        other["system"][0]
            .as_object_mut()
            .unwrap()
            .remove("cache_control");
        assert_eq!(prepare("messages", &other).unwrap().0, chat);
    }

    #[test]
    fn preserves_sampling_stop_and_required_tool_semantics() {
        let source = json!({"model":"m","max_tokens":32,"temperature":0.4,"stop_sequences":["END"],"tool_choice":{"type":"any","disable_parallel_tool_use":true},"tools":[{"name":"run","input_schema":{"type":"object","properties":{}}}],"messages":[{"role":"user","content":"go"}]});
        let (chat, _) = prepare("messages", &source).unwrap();
        assert_eq!(chat["tool_choice"], "required");
        assert_eq!(chat["parallel_tool_calls"], false);
        assert_eq!(chat["stop"], json!(["END"]));
        assert_eq!(chat["temperature"], source["temperature"]);
    }

    #[test]
    fn preserves_generic_reasoning_effort_and_json_schema() {
        let source = json!({"model":"qwen","input":"hi","reasoning":{"effort":"medium"},"text":{"format":{"type":"json_schema","name":"result","strict":true,"schema":{"type":"object"}}}});
        let (chat, _) = prepare("responses", &source).unwrap();
        assert_eq!(chat["reasoning_effort"], "medium");
        assert_eq!(chat["response_format"]["json_schema"]["name"], "result");
        assert_eq!(
            chat["response_format"]["json_schema"]["schema"],
            json!({"type":"object"})
        );
    }

    #[test]
    fn unsupported_documents_and_encrypted_only_history_fail_explicitly() {
        for input in [
            json!([{"type":"reasoning","encrypted_content":"cipher","summary":[]}]),
            json!([{"role":"user","content":[{"type":"input_file","file_id":"f"}]}]),
        ] {
            assert!(prepare("responses", &json!({"model":"m","input":input})).is_err());
        }
    }

    fn chat_reply() -> Value {
        json!({"choices":[{"index":0,"message":{"content":"hello","reasoning_content":"plan","tool_calls":[{"id":"one","type":"function","function":{"name":"run","arguments":"{\"x\":1}"}},{"id":"two","type":"function","function":{"name":"run","arguments":"{\"x\":2}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":1000,"prompt_tokens_details":{"cached_tokens":880},"completion_tokens":20,"completion_tokens_details":{"reasoning_tokens":5}}})
    }

    #[tokio::test]
    async fn buffered_responses_and_messages_use_upstream_usage_and_parallel_tools() {
        for messages in [true, false] {
            let ctx = Context {
                messages,
                thinking: true,
                custom_tools: HashSet::new(),
                tool_names: HashMap::new(),
            };
            let body =
                convert_response(&serde_json::to_vec(&chat_reply()).unwrap(), &ctx, "public")
                    .await
                    .unwrap();
            let value: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["model"], "public");
            assert_eq!(value["usage"]["output_tokens"], 20);
            if messages {
                assert_eq!(value["usage"]["input_tokens"], 120);
                assert_eq!(value["usage"]["cache_read_input_tokens"], 880);
                let blocks = value["content"].as_array().unwrap();
                assert_eq!(blocks.iter().filter(|v| v["type"] == "tool_use").count(), 2);
                assert_eq!(blocks.last().unwrap()["input"], json!({"x":2}));
            } else {
                assert_eq!(value["usage"]["input_tokens"], 1000);
                assert_eq!(value["usage"]["input_tokens_details"]["cached_tokens"], 880);
                assert_eq!(
                    value["usage"]["output_tokens_details"]["reasoning_tokens"],
                    5
                );
                assert_eq!(
                    value["output"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|v| v["type"] == "function_call")
                        .count(),
                    2
                );
            }
        }
    }

    #[tokio::test]
    async fn stream_waits_for_usage_after_finish_reason_and_rejects_truncation() {
        for messages in [true, false] {
            let ctx = Context {
                messages,
                thinking: false,
                custom_tools: HashSet::new(),
                tool_names: HashMap::new(),
            };
            let mut converter = StreamConverter::new(ctx.clone(), "public".into());
            converter
                .push_event(&sse::Event::json(
                    "",
                    &json!({"choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}),
                ))
                .await
                .unwrap();
            let terminal = converter
                .push_event(&sse::Event::json(
                    "",
                    &json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
                ))
                .await
                .unwrap();
            assert!(terminal.is_empty());
            converter.push_event(&sse::Event::json("",&json!({"choices":[],"usage":{"prompt_tokens":1000,"prompt_tokens_details":{"cached_tokens":880},"completion_tokens":4}}))).await.unwrap();
            let events = converter.finish().await.unwrap();
            let values: Vec<Value> = events
                .iter()
                .map(|v| serde_json::from_str(v.data()).unwrap())
                .collect();
            let final_usage = if messages {
                &values
                    .iter()
                    .find(|v| v["type"] == "message_delta")
                    .unwrap()["usage"]
            } else {
                &values
                    .iter()
                    .find(|v| v["type"] == "response.completed")
                    .unwrap()["response"]["usage"]
            };
            assert_eq!(final_usage["output_tokens"], 4);
            assert_eq!(
                final_usage["input_tokens"],
                if messages { 120 } else { 1000 }
            );
            assert!(
                StreamConverter::new(ctx, "public".into())
                    .finish()
                    .await
                    .is_err()
            );
        }
    }
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    #[tokio::test]
    async fn split_sse_roundtrip_keeps_the_protocol_terminal_event() {
        let chunks = [
            b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},".as_slice(),
            b"\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n".as_slice(),
        ];
        let input = sse::parse_chunks(chunks.iter().map(|v| Bytes::copy_from_slice(v)).collect())
            .await
            .unwrap();
        let context = Context {
            messages: true,
            thinking: false,
            custom_tools: HashSet::new(),
            tool_names: HashMap::new(),
        };
        let mut converter = StreamConverter::new(context, "m".into());
        let mut output = Vec::new();
        for event in input {
            output.extend(converter.push_event(&event).await.unwrap());
        }
        let bytes = sse::encode(output).await;
        assert!(String::from_utf8_lossy(&bytes).contains("event: message_stop"));
    }
}

#[cfg(test)]
mod compatibility_tests {
    use super::*;

    #[tokio::test]
    async fn namespace_tools_restore_names_and_replay_through_chat() {
        let mut source = json!({"model":"generic","tools":[{"type":"namespace","name":"terminal","tools":[{"type":"function","name":"run","parameters":{"type":"object"}}]}],"input":[{"role":"user","content":"run"}]});
        let (chat, context) = prepare("responses", &source).unwrap();
        let name = chat["tools"][0]["function"]["name"].as_str().unwrap();
        assert!(!name.contains(':'));
        let reply = json!({"choices":[{"message":{"tool_calls":[{"function":{"name":name,"arguments":"{}"}}]},"finish_reason":"tool_calls"}]});
        let bytes = convert_response(
            &serde_json::to_vec(&reply).unwrap(),
            &context.unwrap(),
            "generic",
        )
        .await
        .unwrap();
        let response: Value = serde_json::from_slice(&bytes).unwrap();
        let call = response["output"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["type"] == "function_call")
            .unwrap();
        assert_eq!(call["name"], "run");
        assert_eq!(call["namespace"], "terminal");
        source["input"].as_array_mut().unwrap().extend([
            call.clone(),
            json!({"type":"function_call_output","call_id":call["call_id"],"output":"done"}),
        ]);
        let (replayed, _) = prepare("responses", &source).unwrap();
        assert_eq!(
            replayed["messages"][1]["tool_calls"][0]["function"]["name"],
            name
        );
        assert_eq!(replayed["messages"][2]["tool_call_id"], call["call_id"]);
    }

    #[tokio::test]
    async fn custom_apply_patch_keeps_raw_input_and_images_keep_their_position() {
        let source = json!({"model":"m","tools":[{"type":"custom","name":"apply_patch","format":{"type":"text"}}],"input":"edit"});
        let (chat, context) = prepare("responses", &source).unwrap();
        assert_eq!(chat["tools"][0]["type"], "function");
        let patch = "*** Begin Patch\n*** End Patch";
        let reply = json!({"choices":[{"message":{"tool_calls":[{"function":{"name":"apply_patch","arguments":json!({"input":patch}).to_string()}}]},"finish_reason":"tool_calls"}]});
        let response =
            convert_response(&serde_json::to_vec(&reply).unwrap(), &context.unwrap(), "m")
                .await
                .unwrap();
        let response: Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(response["output"][0]["type"], "custom_tool_call");
        assert_eq!(response["output"][0]["input"], patch);
        let source = json!({"model":"m","input":[{"role":"user","content":[{"type":"input_text","text":"before"},{"type":"input_image","image_url":"https://example.com/a.png"},{"type":"input_text","text":"after"}]}]});
        let (chat, _) = prepare("responses", &source).unwrap();
        assert_eq!(chat["messages"][0]["content"][0]["text"], "before");
        assert_eq!(
            chat["messages"][0]["content"][1]["image_url"]["url"],
            "https://example.com/a.png"
        );
        assert_eq!(chat["messages"][0]["content"][2]["text"], "after");
    }

    #[tokio::test]
    async fn interleaved_parallel_tool_fragments_complete_without_argument_loss() {
        let (_, context) = prepare("responses", &json!({"model":"m","input":"go"})).unwrap();
        let mut converter = StreamConverter::new(context.unwrap(), "m".into());
        for delta in [
            json!({"tool_calls":[{"index":0,"function":{"name":"first","arguments":"{\"a\":"}},{"index":1,"function":{"name":"second","arguments":"{\"b\":"}}]}),
            json!({"tool_calls":[{"index":1,"function":{"arguments":"2}"}},{"index":0,"function":{"arguments":"1}"}}]}),
        ] {
            converter
                .push(&json!({"choices":[{"delta":delta,"finish_reason":null}]}))
                .await
                .unwrap();
        }
        converter
            .push(&json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]}))
            .await
            .unwrap();
        converter.finish().await.unwrap();
        let value: Value = serde_json::from_slice(&converter.response().unwrap()).unwrap();
        let output = value["output"].as_array().unwrap();
        assert_eq!(output[0]["arguments"], "{\"a\":1}");
        assert_eq!(output[1]["arguments"], "{\"b\":2}");
    }
}

#[cfg(test)]
mod selection_tests {
    use super::*;
    #[test]
    fn generic_chat_keeps_all_tool_definitions_for_none_and_named_selection() {
        let source = json!({"model":"m","input":"hi","reasoning":{"effort":"high"},"tools":[{"type":"function","name":"one","parameters":{"type":"object"}},{"type":"function","name":"two","parameters":{"type":"object"}}]});
        for choice in [
            json!("none"),
            json!("required"),
            json!({"type":"function","name":"one"}),
        ] {
            let mut source = source.clone();
            source["tool_choice"] = choice.clone();
            let (chat, _) = prepare("responses", &source).unwrap();
            assert_eq!(chat["tools"].as_array().unwrap().len(), 2);
            if choice.is_string() {
                assert_eq!(chat["tool_choice"], choice);
            } else {
                assert_eq!(chat["tool_choice"]["function"]["name"], "one");
            }
        }
    }
    #[test]
    fn unavailable_tool_selection_is_rejected_before_scheduling() {
        for choice in [
            json!("required"),
            json!({"type":"web_search"}),
            json!({"type":"function","name":"missing"}),
        ] {
            assert!(
                prepare(
                    "responses",
                    &json!({"model":"m","input":"hi","tool_choice":choice})
                )
                .is_err()
            );
        }
    }
    #[test]
    fn message_text_and_image_blocks_stay_in_one_user_turn() {
        let source = json!({"model":"m","max_tokens":64,"messages":[{"role":"user","content":[{"type":"text","text":"before"},{"type":"image","source":{"type":"url","url":"https://example.com/a.png"}},{"type":"text","text":"after"}]}]});
        let (chat, _) = prepare("messages", &source).unwrap();
        assert_eq!(chat["messages"].as_array().unwrap().len(), 1);
        assert_eq!(chat["messages"][0]["content"][0]["text"], "before");
        assert_eq!(chat["messages"][0]["content"][1]["type"], "image_url");
        assert_eq!(chat["messages"][0]["content"][2]["text"], "after");
    }
}
