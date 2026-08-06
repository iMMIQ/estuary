use std::collections::BTreeMap;

use bytes::Bytes;
use serde_json::{Map, Value, json};

use crate::{error::GatewayError, sse};

const REASONING_SIGNATURE_PREFIX: &str = "estuary_responses:";

pub(crate) fn convert_request(body: &Value) -> Result<Value, GatewayError> {
    let source = object(body, "Anthropic request body")?;
    reject_unknown_top_level_features(source)?;
    let model = required_string(source, "model")?;
    let max_tokens = source
        .get("max_tokens")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            GatewayError::InvalidRequest(
                "Anthropic request body requires a positive 'max_tokens' integer".to_owned(),
            )
        })?;

    let mut output = Map::new();
    output.insert("model".to_owned(), Value::String(model.to_owned()));
    output.insert("store".to_owned(), Value::Bool(false));
    output.insert("max_output_tokens".to_owned(), max_tokens.into());
    output.insert(
        "stream".to_owned(),
        Value::Bool(
            source
                .get("stream")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        ),
    );
    if let Some(system) = source.get("system") {
        output.insert("instructions".to_owned(), convert_system(system)?);
    }
    output.insert(
        "input".to_owned(),
        Value::Array(convert_messages(
            source
                .get("messages")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    GatewayError::InvalidRequest(
                        "Anthropic request body requires a 'messages' array".to_owned(),
                    )
                })?,
        )?),
    );
    copy_fields(source, &mut output, &["temperature", "top_p"]);
    if let Some(stop) = source.get("stop_sequences") {
        output.insert("stop".to_owned(), stop.clone());
    }
    if let Some(tools) = source.get("tools") {
        output.insert("tools".to_owned(), convert_tools(tools)?);
    }
    if let Some(choice) = source.get("tool_choice") {
        let (choice, parallel) = convert_tool_choice(choice)?;
        output.insert("tool_choice".to_owned(), choice);
        if let Some(parallel) = parallel {
            output.insert("parallel_tool_calls".to_owned(), Value::Bool(parallel));
        }
    }
    if let Some(context) = source.get("context_management") {
        output.insert("context_management".to_owned(), context.clone());
    }
    apply_output_config(source, &mut output)?;
    apply_thinking(source, &mut output)?;
    Ok(Value::Object(output))
}

fn reject_unknown_top_level_features(source: &Map<String, Value>) -> Result<(), GatewayError> {
    for field in ["mcp_servers", "container"] {
        if source.get(field).is_some_and(|value| !value.is_null()) {
            return Err(GatewayError::UnsupportedFeature(
                "Anthropic MCP and code-execution features require a native Messages upstream",
            ));
        }
    }
    Ok(())
}

fn convert_system(value: &Value) -> Result<Value, GatewayError> {
    if let Some(text) = value.as_str() {
        return Ok(Value::String(text.to_owned()));
    }
    let blocks = value.as_array().ok_or_else(|| {
        GatewayError::InvalidRequest(
            "Anthropic 'system' must be a string or content block array".to_owned(),
        )
    })?;
    let mut content = Vec::new();
    for block in blocks {
        if block.get("type").and_then(Value::as_str) != Some("text") {
            return Err(GatewayError::UnsupportedFeature(
                "non-text Anthropic system blocks require a native Messages upstream",
            ));
        }
        content.push(required_block_string(block, "text")?);
    }
    Ok(Value::String(content.join("\n\n")))
}

fn convert_messages(messages: &[Value]) -> Result<Vec<Value>, GatewayError> {
    let mut output = Vec::new();
    for message in messages {
        let object = object(message, "Anthropic message")?;
        let role = required_string(object, "role")?;
        let content = object.get("content").ok_or_else(|| {
            GatewayError::InvalidRequest("Anthropic message is missing 'content'".to_owned())
        })?;
        if let Some(text) = content.as_str() {
            output.push(json!({
                "type": "message", "role": role,
                "content": [{"type": if role == "assistant" { "output_text" } else { "input_text" }, "text": text}]
            }));
            continue;
        }
        let blocks = content.as_array().ok_or_else(|| {
            GatewayError::InvalidRequest(
                "Anthropic message content must be a string or block array".to_owned(),
            )
        })?;
        if role == "assistant" {
            convert_assistant_blocks(blocks, &mut output)?;
        } else if role == "user" {
            convert_user_blocks(blocks, &mut output)?;
        } else {
            return Err(GatewayError::InvalidRequest(format!(
                "unsupported Anthropic message role '{role}'"
            )));
        }
    }
    Ok(output)
}

fn convert_assistant_blocks(blocks: &[Value], output: &mut Vec<Value>) -> Result<(), GatewayError> {
    let mut content = Vec::new();
    for block in blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => content.push(json!({
                "type": "output_text", "text": required_block_string(block, "text")?
            })),
            Some("tool_use") => {
                flush_message("assistant", &mut content, output);
                let input = block
                    .get("input")
                    .filter(|value| value.is_object())
                    .ok_or_else(|| {
                        GatewayError::InvalidRequest(
                            "Anthropic tool_use requires object input".to_owned(),
                        )
                    })?;
                output.push(json!({
                    "type": "function_call",
                    "call_id": required_block_string(block, "id")?,
                    "name": required_block_string(block, "name")?,
                    "arguments": serde_json::to_string(input).map_err(|_| GatewayError::Internal)?
                }));
            }
            Some("thinking") => {
                flush_message("assistant", &mut content, output);
                output.push(convert_reasoning_input(block)?);
            }
            Some("redacted_thinking") => {
                return Err(GatewayError::UnsupportedFeature(
                    "Anthropic redacted thinking requires a native Messages upstream",
                ));
            }
            Some(_) => {
                return Err(GatewayError::UnsupportedFeature(
                    "Anthropic assistant content block requires a native Messages upstream",
                ));
            }
            None => {
                return Err(GatewayError::InvalidRequest(
                    "Anthropic content block is missing 'type'".to_owned(),
                ));
            }
        }
    }
    flush_message("assistant", &mut content, output);
    Ok(())
}

fn convert_reasoning_input(block: &Value) -> Result<Value, GatewayError> {
    let signature = required_block_string(block, "signature")?;
    let encoded = signature.strip_prefix(REASONING_SIGNATURE_PREFIX).ok_or(
        GatewayError::UnsupportedFeature(
            "Anthropic thinking signature did not originate from this Responses adapter",
        ),
    )?;
    let provenance: Value = serde_json::from_str(encoded).map_err(|_| {
        GatewayError::InvalidRequest("invalid Estuary Responses thinking signature".to_owned())
    })?;
    let id = provenance
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            GatewayError::InvalidRequest(
                "Estuary Responses thinking signature is missing an id".to_owned(),
            )
        })?;
    let encrypted_content = provenance
        .get("encrypted_content")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            GatewayError::InvalidRequest(
                "Estuary Responses thinking signature is missing encrypted content".to_owned(),
            )
        })?;
    Ok(json!({
        "type": "reasoning",
        "id": id,
        "encrypted_content": encrypted_content,
        "summary": [{"type": "summary_text", "text": required_block_string(block, "thinking")?}]
    }))
}

fn convert_user_blocks(blocks: &[Value], output: &mut Vec<Value>) -> Result<(), GatewayError> {
    let mut content = Vec::new();
    for block in blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => content.push(json!({
                "type": "input_text", "text": required_block_string(block, "text")?
            })),
            Some("image") => content.push(convert_image(block)?),
            Some("tool_result") => {
                flush_message("user", &mut content, output);
                let call_id = required_block_string(block, "tool_use_id")?;
                let result = convert_tool_result(block.get("content"))?;
                output.push(
                    json!({"type": "function_call_output", "call_id": call_id, "output": result}),
                );
            }
            Some(_) => {
                return Err(GatewayError::UnsupportedFeature(
                    "Anthropic user content block requires a native Messages upstream",
                ));
            }
            None => {
                return Err(GatewayError::InvalidRequest(
                    "Anthropic content block is missing 'type'".to_owned(),
                ));
            }
        }
    }
    flush_message("user", &mut content, output);
    Ok(())
}

fn flush_message(role: &str, content: &mut Vec<Value>, output: &mut Vec<Value>) {
    if !content.is_empty() {
        output.push(json!({"type": "message", "role": role, "content": std::mem::take(content)}));
    }
}

fn convert_tool_result(value: Option<&Value>) -> Result<Value, GatewayError> {
    let Some(value) = value else {
        return Ok(Value::String(String::new()));
    };
    if let Some(text) = value.as_str() {
        return Ok(Value::String(text.to_owned()));
    }
    let blocks = value.as_array().ok_or_else(|| {
        GatewayError::InvalidRequest(
            "Anthropic tool result content must be a string or block array".to_owned(),
        )
    })?;
    blocks
        .iter()
        .map(|block| match block.get("type").and_then(Value::as_str) {
            Some("text") => Ok(json!({
                "type": "input_text", "text": required_block_string(block, "text")?
            })),
            Some("image") => convert_image(block),
            _ => Err(GatewayError::UnsupportedFeature(
                "Anthropic tool result block requires a native Messages upstream",
            )),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

fn convert_image(block: &Value) -> Result<Value, GatewayError> {
    let source = object(
        block.get("source").ok_or_else(|| {
            GatewayError::InvalidRequest("Anthropic image block is missing source".to_owned())
        })?,
        "Anthropic image source",
    )?;
    let image_url = match required_string(source, "type")? {
        "url" => required_string(source, "url")?.to_owned(),
        "base64" => format!(
            "data:{};base64,{}",
            required_string(source, "media_type")?,
            required_string(source, "data")?
        ),
        _ => {
            return Err(GatewayError::UnsupportedFeature(
                "Anthropic image source requires a native Messages upstream",
            ));
        }
    };
    Ok(json!({"type": "input_image", "image_url": image_url}))
}

fn convert_tools(value: &Value) -> Result<Value, GatewayError> {
    let tools = value.as_array().ok_or_else(|| {
        GatewayError::InvalidRequest("Anthropic 'tools' must be an array".to_owned())
    })?;
    tools
        .iter()
        .map(|tool| {
            let tool = object(tool, "Anthropic tool")?;
            if tool
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind != "custom")
            {
                return Err(GatewayError::UnsupportedFeature(
                    "Anthropic server tools require a native Messages upstream",
                ));
            }
            let name = required_string(tool, "name")?;
            let parameters = tool
                .get("input_schema")
                .filter(|value| value.is_object())
                .ok_or_else(|| {
                    GatewayError::InvalidRequest(format!(
                        "Anthropic tool '{name}' is missing input_schema"
                    ))
                })?;
            let mut converted = json!({"type": "function", "name": name, "parameters": parameters});
            if let Some(description) = tool.get("description") {
                converted["description"] = description.clone();
            }
            if let Some(strict) = tool.get("strict") {
                converted["strict"] = strict.clone();
            }
            Ok(converted)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

fn convert_tool_choice(value: &Value) -> Result<(Value, Option<bool>), GatewayError> {
    let choice = object(value, "Anthropic tool_choice")?;
    let converted = match required_string(choice, "type")? {
        "auto" => Value::String("auto".to_owned()),
        "any" => Value::String("required".to_owned()),
        "none" => Value::String("none".to_owned()),
        "tool" => json!({"type": "function", "name": required_string(choice, "name")?}),
        kind => {
            return Err(GatewayError::InvalidRequest(format!(
                "unsupported Anthropic tool_choice type '{kind}'"
            )));
        }
    };
    let parallel = choice
        .get("disable_parallel_tool_use")
        .and_then(Value::as_bool)
        .map(|disabled| !disabled);
    Ok((converted, parallel))
}

fn apply_output_config(
    source: &Map<String, Value>,
    output: &mut Map<String, Value>,
) -> Result<(), GatewayError> {
    let Some(config) = source.get("output_config").and_then(Value::as_object) else {
        return Ok(());
    };
    if config.get("effort").is_some() {
        return Err(GatewayError::UnsupportedFeature(
            "Anthropic output effort cannot be represented losslessly by Responses",
        ));
    }
    if let Some(format) = config.get("format").filter(|value| !value.is_null()) {
        let format = object(format, "Anthropic output format")?;
        if required_string(format, "type")? != "json_schema" {
            return Err(GatewayError::InvalidRequest(
                "only Anthropic json_schema output format is supported".to_owned(),
            ));
        }
        let schema = format
            .get("schema")
            .or_else(|| format.get("json_schema"))
            .filter(|value| value.is_object())
            .ok_or_else(|| {
                GatewayError::InvalidRequest(
                    "Anthropic json_schema output format requires schema".to_owned(),
                )
            })?;
        output.insert("text".to_owned(), json!({"format": {"type": "json_schema", "name": "anthropic_output", "schema": schema}}));
    }
    Ok(())
}

fn apply_thinking(
    source: &Map<String, Value>,
    output: &mut Map<String, Value>,
) -> Result<(), GatewayError> {
    let Some(thinking) = source.get("thinking").and_then(Value::as_object) else {
        return Ok(());
    };
    let kind = required_string(thinking, "type")?;
    match kind {
        "disabled" => {
            output.insert("reasoning".to_owned(), json!({"effort": "none"}));
        }
        "adaptive" => {
            output.insert("reasoning".to_owned(), json!({"effort": "high"}));
        }
        "enabled" => {
            return Err(GatewayError::UnsupportedFeature(
                "Anthropic exact thinking budgets require a native Messages upstream",
            ));
        }
        _ => {
            return Err(GatewayError::InvalidRequest(format!(
                "unsupported Anthropic thinking type '{kind}'"
            )));
        }
    }
    output.insert("include".to_owned(), json!(["reasoning.encrypted_content"]));
    Ok(())
}

fn copy_fields(source: &Map<String, Value>, output: &mut Map<String, Value>, fields: &[&str]) {
    for field in fields {
        if let Some(value) = source.get(*field) {
            output.insert((*field).to_owned(), value.clone());
        }
    }
}

fn object<'a>(value: &'a Value, name: &str) -> Result<&'a Map<String, Value>, GatewayError> {
    value
        .as_object()
        .ok_or_else(|| GatewayError::InvalidRequest(format!("{name} must be an object")))
}

fn required_string<'a>(
    value: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, GatewayError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| GatewayError::InvalidRequest(format!("missing non-empty '{field}' string")))
}

fn required_block_string<'a>(value: &'a Value, field: &str) -> Result<&'a str, GatewayError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| GatewayError::InvalidRequest(format!("content block is missing '{field}'")))
}

pub(crate) fn convert_response(
    body: &[u8],
    public_model: &str,
    expose_thinking: bool,
) -> Result<Bytes, GatewayError> {
    let response: Value =
        serde_json::from_slice(body).map_err(|_| GatewayError::InvalidUpstreamResponse)?;
    if !matches!(
        response.get("status").and_then(Value::as_str),
        Some("completed" | "incomplete")
    ) {
        return Err(GatewayError::InvalidUpstreamResponse);
    }
    let mut content = Vec::new();
    let mut has_tools = false;
    for item in response
        .get("output")
        .and_then(Value::as_array)
        .ok_or(GatewayError::InvalidUpstreamResponse)?
    {
        match item.get("type").and_then(Value::as_str) {
            Some("message") => {
                for part in item
                    .get("content")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if part.get("type").and_then(Value::as_str) == Some("output_text") {
                        content.push(json!({"type": "text", "text": part.get("text").and_then(Value::as_str).unwrap_or("")}));
                    } else if part.get("type").and_then(Value::as_str) == Some("refusal") {
                        content.push(json!({"type": "text", "text": part.get("refusal").and_then(Value::as_str).unwrap_or("")}));
                    }
                }
            }
            Some("function_call") => {
                let arguments = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .unwrap_or("{}");
                let input: Value = serde_json::from_str(arguments)
                    .map_err(|_| GatewayError::InvalidUpstreamResponse)?;
                if !input.is_object() {
                    return Err(GatewayError::InvalidUpstreamResponse);
                }
                content.push(json!({
                    "type": "tool_use",
                    "id": item.get("call_id").or_else(|| item.get("id")).and_then(Value::as_str).unwrap_or("toolu_unknown"),
                    "name": item.get("name").and_then(Value::as_str).unwrap_or("unknown"),
                    "input": input
                }));
                has_tools = true;
            }
            Some("reasoning") if expose_thinking => {
                let text = reasoning_text(item);
                if !text.is_empty()
                    && item
                        .get("encrypted_content")
                        .and_then(Value::as_str)
                        .is_some()
                {
                    content.push(json!({"type": "thinking", "thinking": text, "signature": reasoning_signature(item)?}));
                }
            }
            _ => {}
        }
    }
    let stop = if has_tools {
        "tool_use"
    } else if response.get("status").and_then(Value::as_str) == Some("incomplete")
        && response
            .pointer("/incomplete_details/reason")
            .and_then(Value::as_str)
            == Some("max_output_tokens")
    {
        "max_tokens"
    } else {
        "end_turn"
    };
    let converted = json!({
        "id": anthropic_id(response.get("id").and_then(Value::as_str)), "type": "message", "role": "assistant",
        "model": public_model, "content": content, "stop_reason": stop, "stop_sequence": Value::Null,
        "usage": response_usage(response.get("usage"))
    });
    serde_json::to_vec(&converted)
        .map(Bytes::from)
        .map_err(|_| GatewayError::Internal)
}

fn reasoning_text(item: &Value) -> String {
    item.get("summary")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .chain(
            item.get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten(),
        )
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("")
}

fn response_usage(value: Option<&Value>) -> Value {
    let total = value
        .and_then(|value| value.get("input_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cached = value
        .and_then(|value| value.pointer("/input_tokens_details/cached_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output = value
        .and_then(|value| value.get("output_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    json!({"input_tokens": total.saturating_sub(cached), "output_tokens": output, "cache_creation_input_tokens": 0, "cache_read_input_tokens": cached})
}

fn anthropic_id(id: Option<&str>) -> String {
    let id = id.unwrap_or("unknown");
    if id.starts_with("msg_") {
        id.to_owned()
    } else {
        format!("msg_{id}")
    }
}

fn reasoning_signature(item: &Value) -> Result<String, GatewayError> {
    let id = item
        .get("id")
        .and_then(Value::as_str)
        .ok_or(GatewayError::InvalidUpstreamResponse)?;
    let encrypted_content = item
        .get("encrypted_content")
        .and_then(Value::as_str)
        .ok_or(GatewayError::InvalidUpstreamResponse)?;
    let encoded = serde_json::to_string(&json!({
        "id": id,
        "encrypted_content": encrypted_content
    }))
    .map_err(|_| GatewayError::Internal)?;
    Ok(format!("{REASONING_SIGNATURE_PREFIX}{encoded}"))
}

pub(crate) struct StreamConverter {
    state: ResponseStreamState,
}

impl StreamConverter {
    pub(crate) fn new(public_model: String, expose_thinking: bool) -> Self {
        Self {
            state: ResponseStreamState::new(public_model, expose_thinking),
        }
    }
    pub(crate) fn push_event(
        &mut self,
        event: &sse::Event,
    ) -> Result<Vec<sse::Event>, GatewayError> {
        let mut output = Vec::new();
        let value: Value = serde_json::from_str(event.data())
            .map_err(|_| GatewayError::InvalidUpstreamResponse)?;
        self.state
            .event(event.name().unwrap_or_default(), &value, &mut output)?;
        Ok(output)
    }
    pub(crate) fn finish(&mut self) -> Result<Vec<sse::Event>, GatewayError> {
        let mut output = Vec::new();
        self.state.finish(&mut output)?;
        Ok(output)
    }
}

#[derive(Default)]
struct ResponseStreamState {
    public_model: String,
    id: String,
    lifecycle: StreamLifecycle,
    next_index: u64,
    active: BTreeMap<u64, ActiveBlock>,
    output_to_block: BTreeMap<u64, u64>,
    stop_reason: ResponseStop,
    usage: Option<Value>,
    expose_thinking: bool,
}

#[derive(Default, Eq, PartialEq)]
enum StreamLifecycle {
    #[default]
    Initial,
    Streaming,
    Finished,
}

#[derive(Default)]
enum ResponseStop {
    #[default]
    EndTurn,
    MaxTokens,
    ToolUse,
}

#[derive(Clone)]
enum ActiveBlock {
    Text,
    Thinking {
        id: String,
        encrypted_content: Option<String>,
    },
    Tool {
        emitted_arguments: bool,
    },
}

impl ResponseStreamState {
    fn new(public_model: String, expose_thinking: bool) -> Self {
        Self {
            public_model,
            expose_thinking,
            ..Self::default()
        }
    }
    #[allow(clippy::too_many_lines)]
    fn event(
        &mut self,
        name: &str,
        value: &Value,
        output: &mut Vec<sse::Event>,
    ) -> Result<(), GatewayError> {
        if self.lifecycle == StreamLifecycle::Finished {
            return Ok(());
        }
        match name {
            "response.created" | "response.in_progress" => {
                let response = value.get("response").unwrap_or(value);
                if self.id.is_empty() {
                    self.id = anthropic_id(response.get("id").and_then(Value::as_str));
                }
                self.start(output)?;
            }
            "response.output_item.added" => {
                self.start(output)?;
                let item = value
                    .get("item")
                    .ok_or(GatewayError::InvalidUpstreamResponse)?;
                let output_index = value
                    .get("output_index")
                    .and_then(Value::as_u64)
                    .unwrap_or(self.next_index);
                match item.get("type").and_then(Value::as_str) {
                    Some("function_call") => {
                        let index = self.allocate(
                            output_index,
                            ActiveBlock::Tool {
                                emitted_arguments: false,
                            },
                        );
                        self.stop_reason = ResponseStop::ToolUse;
                        emit(
                            output,
                            "content_block_start",
                            json!({"type":"content_block_start","index":index,"content_block":{"type":"tool_use","id":item.get("call_id").or_else(|| item.get("id")).and_then(Value::as_str).unwrap_or("toolu_unknown"),"name":item.get("name").and_then(Value::as_str).unwrap_or("unknown"),"input":{}}}),
                        )?;
                    }
                    Some("reasoning") => {
                        if !self.expose_thinking {
                            return Ok(());
                        }
                        let id = item
                            .get("id")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown")
                            .to_owned();
                        let encrypted_content = item
                            .get("encrypted_content")
                            .and_then(Value::as_str)
                            .map(str::to_owned);
                        let index = self.allocate(
                            output_index,
                            ActiveBlock::Thinking {
                                id,
                                encrypted_content,
                            },
                        );
                        emit(
                            output,
                            "content_block_start",
                            json!({"type":"content_block_start","index":index,"content_block":{"type":"thinking","thinking":""}}),
                        )?;
                    }
                    _ => {}
                }
            }
            "response.output_text.delta" | "response.refusal.delta" => {
                self.start(output)?;
                let index = self.ensure_text(value, output)?;
                emit(
                    output,
                    "content_block_delta",
                    json!({"type":"content_block_delta","index":index,"delta":{"type":"text_delta","text":value.get("delta").and_then(Value::as_str).unwrap_or("")}}),
                )?;
            }
            "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
                if !self.expose_thinking {
                    return Ok(());
                }
                self.start(output)?;
                let index = self.ensure_thinking(value, output)?;
                emit(
                    output,
                    "content_block_delta",
                    json!({"type":"content_block_delta","index":index,"delta":{"type":"thinking_delta","thinking":value.get("delta").and_then(Value::as_str).unwrap_or("")}}),
                )?;
            }
            "response.function_call_arguments.delta" => {
                self.start(output)?;
                let index = self.ensure_tool(value, output)?;
                if let Some(ActiveBlock::Tool { emitted_arguments }) = self.active.get_mut(&index) {
                    *emitted_arguments = true;
                }
                emit(
                    output,
                    "content_block_delta",
                    json!({"type":"content_block_delta","index":index,"delta":{"type":"input_json_delta","partial_json":value.get("delta").and_then(Value::as_str).unwrap_or("")}}),
                )?;
            }
            "response.output_item.done" => {
                let output_index = value
                    .get("output_index")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                if let Some(index) = self.output_to_block.get(&output_index).copied() {
                    if let Some(ActiveBlock::Thinking {
                        encrypted_content, ..
                    }) = self.active.get_mut(&index)
                    {
                        if encrypted_content.is_none() {
                            *encrypted_content = value
                                .get("item")
                                .and_then(|item| item.get("encrypted_content"))
                                .and_then(Value::as_str)
                                .map(str::to_owned);
                        }
                    }
                    let arguments_missing = matches!(
                        self.active.get(&index),
                        Some(ActiveBlock::Tool {
                            emitted_arguments: false
                        })
                    );
                    if arguments_missing {
                        if let Some(arguments) = value
                            .get("item")
                            .and_then(|item| item.get("arguments"))
                            .and_then(Value::as_str)
                            .filter(|arguments| !arguments.is_empty())
                        {
                            emit(
                                output,
                                "content_block_delta",
                                json!({"type":"content_block_delta","index":index,"delta":{"type":"input_json_delta","partial_json":arguments}}),
                            )?;
                        }
                    }
                }
                self.close_output(output_index, output)?;
            }
            "response.completed" | "response.incomplete" => {
                let response = value.get("response").unwrap_or(value);
                self.usage = response.get("usage").cloned();
                if !matches!(self.stop_reason, ResponseStop::ToolUse)
                    && (name == "response.incomplete"
                        || response.get("status").and_then(Value::as_str) == Some("incomplete"))
                {
                    self.stop_reason = ResponseStop::MaxTokens;
                }
                self.finish(output)?;
            }
            "response.failed" | "error" => return Err(GatewayError::InvalidUpstreamResponse),
            _ => {}
        }
        Ok(())
    }
    fn start(&mut self, output: &mut Vec<sse::Event>) -> Result<(), GatewayError> {
        if self.lifecycle != StreamLifecycle::Initial {
            return Ok(());
        }
        self.lifecycle = StreamLifecycle::Streaming;
        emit(
            output,
            "message_start",
            json!({"type":"message_start","message":{"id":if self.id.is_empty(){"msg_unknown"}else{&self.id},"type":"message","role":"assistant","model":self.public_model,"content":[],"stop_reason":Value::Null,"stop_sequence":Value::Null,"usage":{"input_tokens":0,"output_tokens":0,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}),
        )
    }
    fn allocate(&mut self, output_index: u64, block: ActiveBlock) -> u64 {
        if let Some(index) = self.output_to_block.get(&output_index) {
            return *index;
        }
        let index = self.next_index;
        self.next_index += 1;
        self.output_to_block.insert(output_index, index);
        self.active.insert(index, block);
        index
    }
    fn ensure_text(
        &mut self,
        value: &Value,
        output: &mut Vec<sse::Event>,
    ) -> Result<u64, GatewayError> {
        let upstream = value
            .get("output_index")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if let Some(index) = self.output_to_block.get(&upstream) {
            return Ok(*index);
        }
        let index = self.allocate(upstream, ActiveBlock::Text);
        emit(
            output,
            "content_block_start",
            json!({"type":"content_block_start","index":index,"content_block":{"type":"text","text":""}}),
        )?;
        Ok(index)
    }
    fn ensure_thinking(
        &mut self,
        value: &Value,
        output: &mut Vec<sse::Event>,
    ) -> Result<u64, GatewayError> {
        let upstream = value
            .get("output_index")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if let Some(index) = self.output_to_block.get(&upstream) {
            return Ok(*index);
        }
        let id = value
            .get("item_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let index = self.allocate(
            upstream,
            ActiveBlock::Thinking {
                id,
                encrypted_content: None,
            },
        );
        emit(
            output,
            "content_block_start",
            json!({"type":"content_block_start","index":index,"content_block":{"type":"thinking","thinking":""}}),
        )?;
        Ok(index)
    }
    fn ensure_tool(
        &mut self,
        value: &Value,
        output: &mut Vec<sse::Event>,
    ) -> Result<u64, GatewayError> {
        let upstream = value
            .get("output_index")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if let Some(index) = self.output_to_block.get(&upstream) {
            return Ok(*index);
        }
        self.stop_reason = ResponseStop::ToolUse;
        let index = self.allocate(
            upstream,
            ActiveBlock::Tool {
                emitted_arguments: false,
            },
        );
        emit(
            output,
            "content_block_start",
            json!({"type":"content_block_start","index":index,"content_block":{"type":"tool_use","id":value.get("item_id").and_then(Value::as_str).unwrap_or("toolu_unknown"),"name":"unknown","input":{}}}),
        )?;
        Ok(index)
    }
    fn close_output(
        &mut self,
        output_index: u64,
        output: &mut Vec<sse::Event>,
    ) -> Result<(), GatewayError> {
        let Some(index) = self.output_to_block.remove(&output_index) else {
            return Ok(());
        };
        match self.active.remove(&index) {
            Some(ActiveBlock::Thinking {
                id,
                encrypted_content: Some(encrypted_content),
            }) => {
                let signature = reasoning_signature(&json!({
                    "id": id,
                    "encrypted_content": encrypted_content
                }))?;
                emit(
                    output,
                    "content_block_delta",
                    json!({"type":"content_block_delta","index":index,"delta":{"type":"signature_delta","signature":signature}}),
                )?;
            }
            Some(ActiveBlock::Thinking { .. }) => {
                return Err(GatewayError::InvalidUpstreamResponse);
            }
            _ => {}
        }
        emit(
            output,
            "content_block_stop",
            json!({"type":"content_block_stop","index":index}),
        )
    }
    fn finish(&mut self, output: &mut Vec<sse::Event>) -> Result<(), GatewayError> {
        if self.lifecycle == StreamLifecycle::Finished {
            return Ok(());
        }
        if self.lifecycle == StreamLifecycle::Initial {
            return Err(GatewayError::InvalidUpstreamResponse);
        }
        for upstream in self.output_to_block.keys().copied().collect::<Vec<_>>() {
            self.close_output(upstream, output)?;
        }
        let usage = response_usage(self.usage.as_ref());
        let stop = match self.stop_reason {
            ResponseStop::EndTurn => "end_turn",
            ResponseStop::MaxTokens => "max_tokens",
            ResponseStop::ToolUse => "tool_use",
        };
        emit(
            output,
            "message_delta",
            json!({"type":"message_delta","delta":{"stop_reason":stop,"stop_sequence":Value::Null},"usage":usage}),
        )?;
        emit(output, "message_stop", json!({"type":"message_stop"}))?;
        self.lifecycle = StreamLifecycle::Finished;
        Ok(())
    }
}

#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn emit(output: &mut Vec<sse::Event>, name: &str, value: Value) -> Result<(), GatewayError> {
    output.push(sse::Event::json(name, &value));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn responses_request_preserves_multimodal_tool_results() {
        let converted = convert_request(&json!({
            "model":"claude", "max_tokens":1024,
            "messages":[
                {"role":"assistant","content":[{"type":"tool_use","id":"tool_1","name":"inspect","input":{"path":"a"}}]},
                {"role":"user","content":[{"type":"tool_result","tool_use_id":"tool_1","content":[{"type":"text","text":"ok"},{"type":"image","source":{"type":"url","url":"https://example.test/a.png"}}]}]}
            ]
        })).unwrap();
        assert_eq!(converted["store"], false);
        assert_eq!(converted["input"][0]["type"], "function_call");
        assert_eq!(converted["input"][1]["output"][1]["type"], "input_image");
    }

    #[test]
    fn responses_usage_excludes_cached_tokens_from_input() {
        assert_eq!(
            response_usage(Some(
                &json!({"input_tokens":100,"output_tokens":7,"input_tokens_details":{"cached_tokens":40}})
            ))["input_tokens"],
            60
        );
    }

    #[test]
    fn responses_refusal_is_preserved_as_text() {
        let response = convert_response(
            br#"{"id":"resp_1","status":"completed","output":[{"type":"message","content":[{"type":"refusal","refusal":"cannot comply"}]}]}"#,
            "claude",
            false,
        )
        .unwrap();
        let response: Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(response["content"][0]["text"], "cannot comply");
    }

    #[test]
    fn responses_reasoning_signature_round_trips_encrypted_state() {
        let response = convert_response(
            serde_json::to_string(&json!({
                "id":"resp_1", "status":"completed",
                "output":[{"type":"reasoning","id":"rs_1","encrypted_content":"opaque-token","summary":[{"type":"summary_text","text":"worked"}]}],
                "usage":{"input_tokens":1,"output_tokens":2}
            }))
            .unwrap()
            .as_bytes(),
            "claude",
            true,
        )
        .unwrap();
        let response: Value = serde_json::from_slice(&response).unwrap();
        let signature = response["content"][0]["signature"].as_str().unwrap();
        assert!(signature.starts_with(REASONING_SIGNATURE_PREFIX));

        let request = convert_request(&json!({
            "model":"claude", "max_tokens":1024,
            "messages":[{"role":"assistant","content":[{
                "type":"thinking", "thinking":"worked", "signature":signature
            }]}, {"role":"user","content":"continue"}]
        }))
        .unwrap();
        assert_eq!(request["input"][0]["type"], "reasoning");
        assert_eq!(request["input"][0]["id"], "rs_1");
        assert_eq!(request["input"][0]["encrypted_content"], "opaque-token");
    }

    #[tokio::test]
    async fn responses_stream_converts_parallel_tools_and_usage() {
        let source = concat!(
            "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n",
            "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"call_id\":\"call_1\",\"name\":\"Read\"}}\n\n",
            "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_2\",\"call_id\":\"call_2\",\"name\":\"Glob\"}}\n\n",
            "event: response.function_call_arguments.delta\ndata: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":\"{\\\"path\\\":\\\"a\\\"}\"}\n\n",
            "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"arguments\":\"{\\\"path\\\":\\\"a\\\"}\"}}\n\n",
            "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"arguments\":\"{\\\"pattern\\\":\\\"*.rs\\\"}\"}}\n\n",
            "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":50,\"output_tokens\":9,\"input_tokens_details\":{\"cached_tokens\":20}}}}\n\n"
        );
        let mut converter = StreamConverter::new("claude-public".to_owned(), false);
        let split = source.len() / 2;
        let events = sse::parse_chunks(vec![
            Bytes::copy_from_slice(&source.as_bytes()[..split]),
            Bytes::copy_from_slice(&source.as_bytes()[split..]),
        ])
        .await
        .unwrap();
        let mut output = Vec::new();
        for event in events {
            output.extend(converter.push_event(&event).unwrap());
        }
        output.extend(converter.finish().unwrap());
        let output = String::from_utf8(sse::encode(output).await.to_vec()).unwrap();
        assert_eq!(output.matches(r#""type":"tool_use""#).count(), 2);
        assert!(output.contains(r#""id":"call_1""#));
        assert!(output.contains(r#""id":"call_2""#));
        assert!(output.contains(r#""partial_json":"{\"pattern\":\"*.rs\"}""#));
        assert!(output.contains(r#""input_tokens":30"#));
        assert!(output.contains(r#""cache_read_input_tokens":20"#));
        assert!(output.contains(r#""stop_reason":"tool_use""#));
        assert!(output.ends_with("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"));
    }
}
