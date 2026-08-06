use std::collections::BTreeMap;

use axum::{
    Json,
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use serde_json::{Map, Value, json};

use crate::{error::GatewayError, sse};

pub(crate) fn convert_request(body: &Value) -> Result<Value, GatewayError> {
    convert_request_inner(body, true)
}

pub(crate) fn convert_count_request(body: &Value) -> Result<Value, GatewayError> {
    convert_request_inner(body, false)
}

pub(crate) fn validate_request_shape(body: &Value, generation: bool) -> Result<(), GatewayError> {
    let object = body.as_object().ok_or_else(|| {
        GatewayError::InvalidRequest("JSON request body must be an object".to_owned())
    })?;
    required_string(object, "model")?;
    if !object.get("messages").is_some_and(Value::is_array) {
        return Err(GatewayError::InvalidRequest(
            "Anthropic request body requires a 'messages' array".to_owned(),
        ));
    }
    if generation
        && object
            .get("max_tokens")
            .and_then(Value::as_u64)
            .is_none_or(|tokens| tokens == 0)
    {
        return Err(GatewayError::InvalidRequest(
            "Anthropic request body requires a positive 'max_tokens' integer".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn thinking_requested(body: &Value) -> bool {
    body.get("thinking")
        .and_then(Value::as_object)
        .is_some_and(|thinking| {
            matches!(
                thinking.get("type").and_then(Value::as_str),
                Some("adaptive" | "enabled")
            ) && thinking.get("display").and_then(Value::as_str) != Some("omitted")
        })
}

fn convert_request_inner(body: &Value, generation: bool) -> Result<Value, GatewayError> {
    let object = body.as_object().ok_or_else(|| {
        GatewayError::InvalidRequest("JSON request body must be an object".to_owned())
    })?;
    let model = required_string(object, "model")?;
    if object
        .get("context_management")
        .is_some_and(|value| !value.is_null())
    {
        return Err(GatewayError::UnsupportedFeature(
            "Anthropic context management requires a native Messages or Responses upstream",
        ));
    }
    let max_tokens = generation
        .then(|| {
            object
                .get("max_tokens")
                .and_then(Value::as_u64)
                .filter(|tokens| *tokens > 0)
                .ok_or_else(|| {
                    GatewayError::InvalidRequest(
                        "Anthropic request body requires a positive 'max_tokens' integer"
                            .to_owned(),
                    )
                })
        })
        .transpose()?;

    let mut messages = Vec::new();
    if let Some(system) = object.get("system") {
        let text = text_content(system, "system")?;
        if !text.is_empty() {
            messages.push(json!({"role": "system", "content": text}));
        }
    }
    let input_messages = object
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            GatewayError::InvalidRequest(
                "Anthropic request body requires a 'messages' array".to_owned(),
            )
        })?;
    for message in input_messages {
        convert_message(message, &mut messages)?;
    }

    let mut output = Map::new();
    output.insert("model".to_owned(), Value::String(model.to_owned()));
    output.insert("messages".to_owned(), Value::Array(messages));
    if let Some(max_tokens) = max_tokens {
        output.insert(
            "max_completion_tokens".to_owned(),
            Value::Number(max_tokens.into()),
        );
    }
    copy_fields(object, &mut output, &["temperature", "top_p", "seed"]);
    if let Some(stop) = object.get("stop_sequences") {
        output.insert("stop".to_owned(), stop.clone());
    }
    let streaming = generation
        && object
            .get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    output.insert("stream".to_owned(), Value::Bool(streaming));
    if streaming {
        output.insert("stream_options".to_owned(), json!({"include_usage": true}));
    }
    if let Some(tools) = object.get("tools") {
        output.insert("tools".to_owned(), convert_tools(tools)?);
    }
    if let Some(choice) = object.get("tool_choice") {
        let (choice, parallel) = convert_tool_choice(choice)?;
        output.insert("tool_choice".to_owned(), choice);
        if let Some(parallel) = parallel {
            output.insert("parallel_tool_calls".to_owned(), Value::Bool(parallel));
        }
    }
    apply_output_config(object, &mut output)?;
    apply_thinking(object, &mut output)?;
    Ok(Value::Object(output))
}

fn apply_output_config(
    source: &Map<String, Value>,
    destination: &mut Map<String, Value>,
) -> Result<(), GatewayError> {
    let Some(config) = source.get("output_config").and_then(Value::as_object) else {
        return Ok(());
    };
    if let Some(effort) = config.get("effort").and_then(Value::as_str) {
        if !matches!(effort, "low" | "medium" | "high" | "xhigh" | "max") {
            return Err(GatewayError::InvalidRequest(format!(
                "unsupported Anthropic output effort '{effort}'"
            )));
        }
        destination.insert(
            "reasoning_effort".to_owned(),
            Value::String(effort.to_owned()),
        );
    }
    if let Some(format) = config.get("format").filter(|value| !value.is_null()) {
        let format = format.as_object().ok_or_else(|| {
            GatewayError::InvalidRequest("Anthropic output format must be an object".to_owned())
        })?;
        if format.get("type").and_then(Value::as_str) != Some("json_schema") {
            return Err(GatewayError::InvalidRequest(
                "only Anthropic json_schema output format is supported".to_owned(),
            ));
        }
        let schema = format
            .get("schema")
            .or_else(|| format.get("json_schema"))
            .filter(|schema| schema.is_object())
            .ok_or_else(|| {
                GatewayError::InvalidRequest(
                    "Anthropic json_schema output format requires an object 'schema'".to_owned(),
                )
            })?;
        destination.insert(
            "response_format".to_owned(),
            json!({
                "type": "json_schema",
                "json_schema": {"name": "anthropic_output", "schema": schema}
            }),
        );
    }
    Ok(())
}

fn apply_thinking(
    source: &Map<String, Value>,
    destination: &mut Map<String, Value>,
) -> Result<(), GatewayError> {
    let Some(thinking) = source.get("thinking").and_then(Value::as_object) else {
        return Ok(());
    };
    let kind = required_string(thinking, "type")?;
    match kind {
        "disabled" => {
            destination.insert(
                "reasoning_effort".to_owned(),
                Value::String("none".to_owned()),
            );
        }
        "adaptive" | "enabled" => {
            return Err(GatewayError::UnsupportedFeature(
                "Anthropic thinking requires a native Messages or Responses upstream",
            ));
        }
        _ => {
            return Err(GatewayError::InvalidRequest(format!(
                "unsupported Anthropic thinking type '{kind}'"
            )));
        }
    }
    Ok(())
}

fn required_string<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a str, GatewayError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            GatewayError::InvalidRequest(format!(
                "Anthropic request body requires a non-empty '{key}' string"
            ))
        })
}

fn copy_fields(source: &Map<String, Value>, destination: &mut Map<String, Value>, keys: &[&str]) {
    for key in keys {
        if let Some(value) = source.get(*key) {
            destination.insert((*key).to_owned(), value.clone());
        }
    }
}

fn text_content(value: &Value, field: &str) -> Result<String, GatewayError> {
    if let Some(text) = value.as_str() {
        return Ok(text.to_owned());
    }
    let blocks = value.as_array().ok_or_else(|| {
        GatewayError::InvalidRequest(format!(
            "Anthropic '{field}' must be a string or content block array"
        ))
    })?;
    let mut parts = Vec::new();
    for block in blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => parts.push(
                block
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        GatewayError::InvalidRequest(format!(
                            "Anthropic '{field}' text block is missing 'text'"
                        ))
                    })?
                    .to_owned(),
            ),
            Some(kind) => {
                return Err(GatewayError::InvalidRequest(format!(
                    "unsupported Anthropic '{field}' content block '{kind}'"
                )));
            }
            None => {
                return Err(GatewayError::InvalidRequest(format!(
                    "Anthropic '{field}' content block is missing 'type'"
                )));
            }
        }
    }
    Ok(parts.join("\n\n"))
}

fn convert_message(message: &Value, output: &mut Vec<Value>) -> Result<(), GatewayError> {
    let object = message.as_object().ok_or_else(|| {
        GatewayError::InvalidRequest("Anthropic messages must be objects".to_owned())
    })?;
    let role = required_string(object, "role")?;
    if !matches!(role, "user" | "assistant") {
        return Err(GatewayError::InvalidRequest(format!(
            "unsupported Anthropic message role '{role}'"
        )));
    }
    let content = object.get("content").ok_or_else(|| {
        GatewayError::InvalidRequest("Anthropic message is missing 'content'".to_owned())
    })?;
    if let Some(text) = content.as_str() {
        output.push(json!({"role": role, "content": text}));
        return Ok(());
    }
    let blocks = content.as_array().ok_or_else(|| {
        GatewayError::InvalidRequest(
            "Anthropic message content must be a string or block array".to_owned(),
        )
    })?;
    if role == "assistant" {
        convert_assistant_blocks(blocks, output)
    } else {
        convert_user_blocks(blocks, output)
    }
}

fn convert_assistant_blocks(blocks: &[Value], output: &mut Vec<Value>) -> Result<(), GatewayError> {
    let mut text = Vec::new();
    let mut tool_calls = Vec::new();
    for block in blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => text.push(required_block_string(block, "text")?.to_owned()),
            Some("tool_use") => {
                let id = required_block_string(block, "id")?;
                let name = required_block_string(block, "name")?;
                let input = block
                    .get("input")
                    .filter(|input| input.is_object())
                    .ok_or_else(|| {
                        GatewayError::InvalidRequest(format!(
                            "Anthropic tool use '{id}' requires an object 'input'"
                        ))
                    })?;
                let arguments = serde_json::to_string(input).map_err(|_| GatewayError::Internal)?;
                tool_calls.push(json!({
                    "id": id,
                    "type": "function",
                    "function": {"name": name, "arguments": arguments}
                }));
            }
            Some("thinking") => {
                return Err(GatewayError::UnsupportedFeature(
                    "Anthropic thinking history requires a native Messages or Responses upstream",
                ));
            }
            Some("redacted_thinking") => {
                return Err(GatewayError::UnsupportedFeature(
                    "Anthropic redacted thinking requires a native Messages upstream",
                ));
            }
            Some(kind) => {
                return Err(GatewayError::InvalidRequest(format!(
                    "unsupported Anthropic assistant content block '{kind}'"
                )));
            }
            None => {
                return Err(GatewayError::InvalidRequest(
                    "Anthropic content block is missing 'type'".to_owned(),
                ));
            }
        }
    }
    let mut message = Map::new();
    message.insert("role".to_owned(), Value::String("assistant".to_owned()));
    message.insert("content".to_owned(), Value::String(text.join("\n")));
    if !tool_calls.is_empty() {
        message.insert("tool_calls".to_owned(), Value::Array(tool_calls));
    }
    output.push(Value::Object(message));
    Ok(())
}

fn convert_user_blocks(blocks: &[Value], output: &mut Vec<Value>) -> Result<(), GatewayError> {
    let mut user_parts = Vec::new();
    for block in blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => user_parts.push(json!({
                "type": "text",
                "text": required_block_string(block, "text")?
            })),
            Some("image") => user_parts.push(convert_image(block)?),
            Some("tool_result") => {
                push_user_message(&mut user_parts, output);
                let id = required_block_string(block, "tool_use_id")?;
                let mut content = block
                    .get("content")
                    .map(|value| tool_result_content(value, id))
                    .transpose()?
                    .unwrap_or_default();
                if block.get("is_error").and_then(Value::as_bool) == Some(true) {
                    content = format!("Error: {content}");
                }
                output.push(json!({
                    "role": "tool",
                    "tool_call_id": id,
                    "content": content
                }));
            }
            Some(kind) => {
                return Err(GatewayError::InvalidRequest(format!(
                    "unsupported Anthropic user content block '{kind}'"
                )));
            }
            None => {
                return Err(GatewayError::InvalidRequest(
                    "Anthropic content block is missing 'type'".to_owned(),
                ));
            }
        }
    }
    push_user_message(&mut user_parts, output);
    Ok(())
}

fn push_user_message(user_parts: &mut Vec<Value>, output: &mut Vec<Value>) {
    if !user_parts.is_empty() {
        let content = if user_parts.len() == 1 && user_parts[0]["type"] == "text" {
            user_parts.remove(0)["text"].clone()
        } else {
            Value::Array(std::mem::take(user_parts))
        };
        output.push(json!({"role": "user", "content": content}));
    }
}

fn required_block_string<'a>(block: &'a Value, key: &str) -> Result<&'a str, GatewayError> {
    block
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| GatewayError::InvalidRequest(format!("content block is missing '{key}'")))
}

fn convert_image(block: &Value) -> Result<Value, GatewayError> {
    let source = block
        .get("source")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            GatewayError::InvalidRequest("Anthropic image block is missing 'source'".to_owned())
        })?;
    let url = match source.get("type").and_then(Value::as_str) {
        Some("base64") => {
            let media_type = required_string(source, "media_type")?;
            let data = required_string(source, "data")?;
            format!("data:{media_type};base64,{data}")
        }
        Some("url") => required_string(source, "url")?.to_owned(),
        Some(kind) => {
            return Err(GatewayError::InvalidRequest(format!(
                "unsupported Anthropic image source '{kind}'"
            )));
        }
        None => {
            return Err(GatewayError::InvalidRequest(
                "Anthropic image source is missing 'type'".to_owned(),
            ));
        }
    };
    Ok(json!({"type": "image_url", "image_url": {"url": url}}))
}

fn tool_result_content(value: &Value, id: &str) -> Result<String, GatewayError> {
    if let Some(text) = value.as_str() {
        return Ok(text.to_owned());
    }
    let blocks = value.as_array().ok_or_else(|| {
        GatewayError::InvalidRequest(format!(
            "Anthropic tool result '{id}' content must be a string or block array"
        ))
    })?;
    let mut text = Vec::new();
    for block in blocks {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => text.push(required_block_string(block, "text")?.to_owned()),
            Some("image") => {
                return Err(GatewayError::UnsupportedFeature(
                    "image tool results require a native Messages or Responses upstream",
                ));
            }
            Some(kind) => {
                return Err(GatewayError::InvalidRequest(format!(
                    "unsupported Anthropic tool result block '{kind}'"
                )));
            }
            None => {}
        }
    }
    Ok(text.join("\n"))
}

fn convert_tools(tools: &Value) -> Result<Value, GatewayError> {
    let tools = tools.as_array().ok_or_else(|| {
        GatewayError::InvalidRequest("Anthropic 'tools' must be an array".to_owned())
    })?;
    tools
        .iter()
        .map(|tool| {
            let object = tool.as_object().ok_or_else(|| {
                GatewayError::InvalidRequest("Anthropic tools must be objects".to_owned())
            })?;
            if object
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind != "custom")
            {
                return Err(GatewayError::UnsupportedFeature(
                    "Anthropic server tools require a native Messages upstream",
                ));
            }
            if object.get("cache_control").is_some() {
                return Err(GatewayError::UnsupportedFeature(
                    "Anthropic tool cache_control requires a native Messages upstream",
                ));
            }
            let name = required_string(object, "name")?;
            let schema = object.get("input_schema").ok_or_else(|| {
                GatewayError::InvalidRequest(format!(
                    "Anthropic tool '{name}' is missing 'input_schema'"
                ))
            })?;
            let mut function = Map::new();
            function.insert("name".to_owned(), Value::String(name.to_owned()));
            function.insert("parameters".to_owned(), schema.clone());
            if let Some(description) = object.get("description") {
                function.insert("description".to_owned(), description.clone());
            }
            if let Some(strict) = object.get("strict") {
                function.insert("strict".to_owned(), strict.clone());
            }
            Ok(json!({"type": "function", "function": function}))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

fn convert_tool_choice(choice: &Value) -> Result<(Value, Option<bool>), GatewayError> {
    let object = choice.as_object().ok_or_else(|| {
        GatewayError::InvalidRequest("Anthropic 'tool_choice' must be an object".to_owned())
    })?;
    let kind = required_string(object, "type")?;
    let converted = match kind {
        "auto" => Value::String("auto".to_owned()),
        "any" => Value::String("required".to_owned()),
        "none" => Value::String("none".to_owned()),
        "tool" => json!({
            "type": "function",
            "function": {"name": required_string(object, "name")?}
        }),
        _ => {
            return Err(GatewayError::InvalidRequest(format!(
                "unsupported Anthropic tool_choice type '{kind}'"
            )));
        }
    };
    let parallel = object
        .get("disable_parallel_tool_use")
        .and_then(Value::as_bool)
        .map(|disabled| !disabled);
    Ok((converted, parallel))
}

pub(crate) fn convert_response(
    body: &[u8],
    public_model: &str,
    expose_thinking: bool,
) -> Result<Bytes, GatewayError> {
    let value: Value =
        serde_json::from_slice(body).map_err(|_| GatewayError::InvalidUpstreamResponse)?;
    let choice = value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .ok_or(GatewayError::InvalidUpstreamResponse)?;
    let message = choice
        .get("message")
        .and_then(Value::as_object)
        .ok_or(GatewayError::InvalidUpstreamResponse)?;
    let mut content = Vec::new();
    if expose_thinking {
        if let Some(reasoning) = message
            .get("reasoning")
            .or_else(|| message.get("reasoning_content"))
            .and_then(Value::as_str)
            .filter(|reasoning| !reasoning.is_empty())
        {
            content.push(json!({
                "type": "thinking",
                "thinking": reasoning,
                "signature": synthetic_signature(value.get("id").and_then(Value::as_str))
            }));
        }
    }
    if let Some(text) = message.get("content").and_then(Value::as_str) {
        if !text.is_empty() {
            content.push(json!({"type": "text", "text": text}));
        }
    }
    if let Some(calls) = message.get("tool_calls").and_then(Value::as_array) {
        for call in calls {
            let function = call
                .get("function")
                .and_then(Value::as_object)
                .ok_or(GatewayError::InvalidUpstreamResponse)?;
            let arguments = function
                .get("arguments")
                .and_then(Value::as_str)
                .unwrap_or("{}");
            let input: Value = serde_json::from_str(arguments)
                .ok()
                .filter(Value::is_object)
                .ok_or(GatewayError::InvalidUpstreamResponse)?;
            content.push(json!({
                "type": "tool_use",
                "id": call.get("id").and_then(Value::as_str).unwrap_or("toolu_unknown"),
                "name": function.get("name").and_then(Value::as_str).unwrap_or("unknown"),
                "input": input
            }));
        }
    }
    let finish = choice.get("finish_reason").and_then(Value::as_str);
    let has_tools = message
        .get("tool_calls")
        .and_then(Value::as_array)
        .is_some_and(|calls| !calls.is_empty());
    let usage = usage(value.get("usage"));
    let response = json!({
        "id": anthropic_id(value.get("id").and_then(Value::as_str)),
        "type": "message",
        "role": "assistant",
        "model": public_model,
        "content": content,
        "stop_reason": stop_reason(finish, has_tools),
        "stop_sequence": Value::Null,
        "usage": usage
    });
    serde_json::to_vec(&response)
        .map(Bytes::from)
        .map_err(|_| GatewayError::Internal)
}

pub(crate) fn rewrite_native_response(
    body: &[u8],
    public_model: &str,
    expose_thinking: bool,
) -> Result<Bytes, GatewayError> {
    let mut value: Value =
        serde_json::from_slice(body).map_err(|_| GatewayError::InvalidUpstreamResponse)?;
    let object = value
        .as_object_mut()
        .ok_or(GatewayError::InvalidUpstreamResponse)?;
    if object.get("type").and_then(Value::as_str) == Some("message") {
        object.insert("model".to_owned(), Value::String(public_model.to_owned()));
        if !expose_thinking {
            if let Some(content) = object.get_mut("content").and_then(Value::as_array_mut) {
                for block in content {
                    suppress_native_thinking(block);
                }
            }
        }
    } else if object.get("input_tokens").and_then(Value::as_u64).is_none() {
        return Err(GatewayError::InvalidUpstreamResponse);
    }
    serde_json::to_vec(&value)
        .map(Bytes::from)
        .map_err(|_| GatewayError::Internal)
}

fn anthropic_id(id: Option<&str>) -> String {
    let id = id.unwrap_or("unknown");
    if id.starts_with("msg_") {
        id.to_owned()
    } else {
        format!("msg_{id}")
    }
}

fn synthetic_signature(id: Option<&str>) -> String {
    format!("estuary_{}", id.unwrap_or("unknown"))
}

fn usage(value: Option<&Value>) -> Value {
    let input = value
        .and_then(|usage| usage.get("prompt_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output = value
        .and_then(|usage| usage.get("completion_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cached = value
        .and_then(|usage| usage.get("prompt_tokens_details"))
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    json!({
        "input_tokens": input.saturating_sub(cached),
        "output_tokens": output,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": cached
    })
}

fn stop_reason(finish: Option<&str>, has_tools: bool) -> &'static str {
    match finish {
        Some("length") => "max_tokens",
        Some("tool_calls") => "tool_use",
        Some("stop") if has_tools => "tool_use",
        _ => "end_turn",
    }
}

pub(crate) fn error_response(error: &GatewayError, request_id: &str) -> Response {
    let status = error.status();
    let error_type = match status {
        StatusCode::BAD_REQUEST | StatusCode::PAYLOAD_TOO_LARGE => "invalid_request_error",
        StatusCode::NOT_FOUND => "not_found_error",
        StatusCode::TOO_MANY_REQUESTS => "rate_limit_error",
        StatusCode::SERVICE_UNAVAILABLE => "overloaded_error",
        _ => "api_error",
    };
    let retryable = matches!(error, GatewayError::NoHealthyNode(_));
    let mut response = (
        status,
        Json(json!({
            "type": "error",
            "error": {"type": error_type, "message": error.to_string()},
            "request_id": request_id
        })),
    )
        .into_response();
    if retryable {
        response
            .headers_mut()
            .insert("retry-after", HeaderValue::from_static("1"));
    }
    response
}

pub(crate) fn convert_error_response(
    status: StatusCode,
    body: &[u8],
    request_id: &str,
) -> Response {
    let value = serde_json::from_slice::<Value>(body).ok();
    let message = value
        .as_ref()
        .and_then(|value| value.get("error"))
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .map_or_else(|| format!("upstream returned HTTP {status}"), str::to_owned);
    let error_type = match status {
        StatusCode::BAD_REQUEST | StatusCode::PAYLOAD_TOO_LARGE => "invalid_request_error",
        StatusCode::NOT_FOUND => "not_found_error",
        StatusCode::TOO_MANY_REQUESTS => "rate_limit_error",
        StatusCode::SERVICE_UNAVAILABLE => "overloaded_error",
        _ => "api_error",
    };
    (
        status,
        Json(json!({
            "type": "error",
            "error": {"type": error_type, "message": message},
            "request_id": request_id
        })),
    )
        .into_response()
}

pub(crate) struct StreamConverter {
    state: StreamState,
}

pub(crate) struct NativeStreamRewriter {
    public_model: String,
    expose_thinking: bool,
}

impl NativeStreamRewriter {
    pub(crate) fn new(public_model: String, expose_thinking: bool) -> Self {
        Self {
            public_model,
            expose_thinking,
        }
    }

    pub(crate) fn push_event(
        &mut self,
        mut event: sse::Event,
    ) -> Result<Vec<sse::Event>, GatewayError> {
        let rewrite = match event.name() {
            Some("message_start") | None => true,
            Some("content_block_start" | "content_block_delta") => !self.expose_thinking,
            Some(_) => false,
        };
        if !rewrite {
            return Ok(vec![event]);
        }
        let mut value: Value = serde_json::from_str(event.data())
            .map_err(|_| GatewayError::InvalidUpstreamResponse)?;
        if value.get("type").and_then(Value::as_str) == Some("message_start") {
            let message = value
                .get_mut("message")
                .and_then(Value::as_object_mut)
                .ok_or(GatewayError::InvalidUpstreamResponse)?;
            message.insert("model".to_owned(), Value::String(self.public_model.clone()));
        }
        if !self.expose_thinking {
            if value.get("type").and_then(Value::as_str) == Some("content_block_start") {
                if let Some(block) = value.get_mut("content_block") {
                    suppress_native_thinking(block);
                }
            } else if value.get("type").and_then(Value::as_str) == Some("content_block_delta") {
                if let Some(delta) = value.get_mut("delta") {
                    suppress_native_thinking(delta);
                }
            }
        }
        let name = event
            .name()
            .or_else(|| value.get("type").and_then(Value::as_str))
            .ok_or(GatewayError::InvalidUpstreamResponse)?
            .to_owned();
        event.set_name(name);
        event.set_data(serde_json::to_string(&value).map_err(|_| GatewayError::Internal)?);
        Ok(vec![event])
    }
}

fn suppress_native_thinking(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    if matches!(
        object.get("type").and_then(Value::as_str),
        Some("thinking" | "thinking_delta")
    ) {
        object.insert("thinking".to_owned(), Value::String(String::new()));
    }
}

impl StreamConverter {
    pub(crate) fn new(public_model: String, expose_thinking: bool) -> Self {
        Self {
            state: StreamState::new(public_model, expose_thinking),
        }
    }

    pub(crate) fn push_event(
        &mut self,
        event: &sse::Event,
    ) -> Result<Vec<sse::Event>, GatewayError> {
        let mut output = Vec::new();
        if event.data().trim() == "[DONE]" {
            self.state.finish(&mut output)?;
        } else {
            let chunk = serde_json::from_str::<Value>(event.data())
                .map_err(|_| GatewayError::InvalidUpstreamResponse)?;
            self.state.chunk(&chunk, &mut output)?;
        }
        Ok(output)
    }

    pub(crate) fn finish(&mut self) -> Result<Vec<sse::Event>, GatewayError> {
        let mut output = Vec::new();
        self.state.finish(&mut output)?;
        Ok(output)
    }
}

pub(crate) fn stream_error_event(message: &str) -> sse::Event {
    let mut output = Vec::new();
    let _ = event(
        &mut output,
        "error",
        json!({
            "type": "error",
            "error": {"type": "api_error", "message": message}
        }),
    );
    output.pop().expect("error event is emitted")
}

pub(crate) fn ping_event() -> sse::Event {
    let mut output = Vec::new();
    let _ = event(&mut output, "ping", json!({"type": "ping"}));
    output.pop().expect("ping event is emitted")
}

#[derive(Default)]
struct StreamState {
    public_model: String,
    id: String,
    started: bool,
    finished: bool,
    thinking_index: Option<u64>,
    text_index: Option<u64>,
    tools: BTreeMap<u64, ToolStream>,
    next_index: u64,
    finish_reason: Option<String>,
    usage: Option<Value>,
    expose_thinking: bool,
}

impl StreamState {
    fn new(public_model: String, expose_thinking: bool) -> Self {
        Self {
            public_model,
            expose_thinking,
            ..Self::default()
        }
    }

    fn chunk(&mut self, chunk: &Value, output: &mut Vec<sse::Event>) -> Result<(), GatewayError> {
        if self.finished {
            return Ok(());
        }
        if self.id.is_empty() {
            self.id = anthropic_id(chunk.get("id").and_then(Value::as_str));
        }
        self.start(output)?;
        if let Some(usage) = chunk.get("usage").filter(|value| !value.is_null()) {
            self.usage = Some(usage.clone());
        }
        let Some(choice) = chunk
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
        else {
            return Ok(());
        };
        if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
            self.finish_reason = Some(reason.to_owned());
        }
        let Some(delta) = choice.get("delta").and_then(Value::as_object) else {
            return Ok(());
        };
        if self.expose_thinking {
            if let Some(reasoning) = delta
                .get("reasoning")
                .or_else(|| delta.get("reasoning_content"))
                .and_then(Value::as_str)
            {
                if !reasoning.is_empty() {
                    self.finish_text(output)?;
                    let index = if let Some(index) = self.thinking_index {
                        index
                    } else {
                        let index = self.next_index;
                        self.next_index += 1;
                        self.thinking_index = Some(index);
                        event(
                            output,
                            "content_block_start",
                            json!({
                                "type": "content_block_start", "index": index,
                                "content_block": {"type": "thinking", "thinking": ""}
                            }),
                        )?;
                        index
                    };
                    event(
                        output,
                        "content_block_delta",
                        json!({
                            "type": "content_block_delta", "index": index,
                            "delta": {"type": "thinking_delta", "thinking": reasoning}
                        }),
                    )?;
                }
            }
        }
        if let Some(text) = delta.get("content").and_then(Value::as_str) {
            if !text.is_empty() {
                self.finish_thinking(output)?;
                let index = if let Some(index) = self.text_index {
                    index
                } else {
                    let index = self.next_index;
                    self.next_index += 1;
                    self.text_index = Some(index);
                    event(
                        output,
                        "content_block_start",
                        json!({
                            "type": "content_block_start", "index": index,
                            "content_block": {"type": "text", "text": ""}
                        }),
                    )?;
                    index
                };
                event(
                    output,
                    "content_block_delta",
                    json!({
                        "type": "content_block_delta", "index": index,
                        "delta": {"type": "text_delta", "text": text}
                    }),
                )?;
            }
        }
        if let Some(calls) = delta.get("tool_calls").and_then(Value::as_array) {
            if !calls.is_empty() {
                self.finish_thinking(output)?;
                self.finish_text(output)?;
            }
            for call in calls {
                self.tool_delta(call, output)?;
            }
        }
        Ok(())
    }

    fn start(&mut self, output: &mut Vec<sse::Event>) -> Result<(), GatewayError> {
        if self.started {
            return Ok(());
        }
        self.started = true;
        event(
            output,
            "message_start",
            json!({
                "type": "message_start",
                "message": {
                    "id": if self.id.is_empty() { "msg_unknown" } else { &self.id },
                    "type": "message", "role": "assistant", "model": self.public_model,
                    "content": [], "stop_reason": Value::Null, "stop_sequence": Value::Null,
                    "usage": {"input_tokens": 0, "output_tokens": 0,
                        "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0}
                }
            }),
        )
    }

    fn tool_delta(
        &mut self,
        call: &Value,
        output: &mut Vec<sse::Event>,
    ) -> Result<(), GatewayError> {
        let upstream_index = call.get("index").and_then(Value::as_u64).unwrap_or(0);
        let tool = self.tools.entry(upstream_index).or_insert_with(|| {
            let index = self.next_index;
            self.next_index += 1;
            ToolStream {
                index,
                ..ToolStream::default()
            }
        });
        if let Some(id) = call.get("id").and_then(Value::as_str) {
            if tool.id.is_empty() {
                id.clone_into(&mut tool.id);
            }
        }
        if let Some(function) = call.get("function").and_then(Value::as_object) {
            if let Some(name) = function.get("name").and_then(Value::as_str) {
                if tool.name.is_empty() {
                    name.clone_into(&mut tool.name);
                }
            }
            if let Some(arguments) = function.get("arguments").and_then(Value::as_str) {
                if tool.started {
                    if !arguments.is_empty() {
                        event(
                            output,
                            "content_block_delta",
                            json!({
                                "type": "content_block_delta", "index": tool.index,
                                "delta": {"type": "input_json_delta", "partial_json": arguments}
                            }),
                        )?;
                    }
                } else {
                    tool.arguments.push_str(arguments);
                }
            }
        }
        if !tool.started && !tool.name.is_empty() {
            tool.start(output)?;
            if !tool.arguments.is_empty() {
                event(
                    output,
                    "content_block_delta",
                    json!({
                        "type": "content_block_delta", "index": tool.index,
                        "delta": {"type": "input_json_delta", "partial_json": tool.arguments}
                    }),
                )?;
                tool.arguments.clear();
            }
        }
        Ok(())
    }

    fn finish_thinking(&mut self, output: &mut Vec<sse::Event>) -> Result<(), GatewayError> {
        let Some(index) = self.thinking_index.take() else {
            return Ok(());
        };
        event(
            output,
            "content_block_delta",
            json!({
                "type": "content_block_delta", "index": index,
                "delta": {
                    "type": "signature_delta",
                    "signature": synthetic_signature(Some(&self.id))
                }
            }),
        )?;
        event(
            output,
            "content_block_stop",
            json!({"type": "content_block_stop", "index": index}),
        )
    }

    fn finish_text(&mut self, output: &mut Vec<sse::Event>) -> Result<(), GatewayError> {
        let Some(index) = self.text_index.take() else {
            return Ok(());
        };
        event(
            output,
            "content_block_stop",
            json!({"type": "content_block_stop", "index": index}),
        )
    }

    fn finish(&mut self, output: &mut Vec<sse::Event>) -> Result<(), GatewayError> {
        if self.finished {
            return Ok(());
        }
        if !self.started {
            return Err(GatewayError::InvalidUpstreamResponse);
        }
        self.finish_thinking(output)?;
        self.finish_text(output)?;
        for tool in self.tools.values_mut() {
            tool.start(output)?;
            if !tool.arguments.is_empty() {
                event(
                    output,
                    "content_block_delta",
                    json!({
                        "type": "content_block_delta", "index": tool.index,
                        "delta": {
                            "type": "input_json_delta",
                            "partial_json": tool.arguments
                        }
                    }),
                )?;
            }
            event(
                output,
                "content_block_stop",
                json!({
                    "type": "content_block_stop", "index": tool.index
                }),
            )?;
        }
        let has_tools = !self.tools.is_empty();
        let usage = usage(self.usage.as_ref());
        event(
            output,
            "message_delta",
            json!({
                "type": "message_delta",
                "delta": {
                    "stop_reason": stop_reason(self.finish_reason.as_deref(), has_tools),
                    "stop_sequence": Value::Null
                },
                "usage": usage
            }),
        )?;
        event(output, "message_stop", json!({"type": "message_stop"}))?;
        self.finished = true;
        Ok(())
    }
}

#[derive(Default)]
struct ToolStream {
    index: u64,
    id: String,
    name: String,
    arguments: String,
    started: bool,
}

impl ToolStream {
    fn start(&mut self, output: &mut Vec<sse::Event>) -> Result<(), GatewayError> {
        if self.started {
            return Ok(());
        }
        self.started = true;
        event(
            output,
            "content_block_start",
            json!({
                "type": "content_block_start", "index": self.index,
                "content_block": {
                    "type": "tool_use",
                    "id": if self.id.is_empty() { "toolu_unknown" } else { &self.id },
                    "name": if self.name.is_empty() { "unknown" } else { &self.name },
                    "input": {}
                }
            }),
        )
    }
}

#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
fn event(output: &mut Vec<sse::Event>, name: &str, value: Value) -> Result<(), GatewayError> {
    output.push(sse::Event::json(name, &value));
    Ok(())
}

pub(crate) fn set_anthropic_content_type(response: &mut Response, streaming: bool) {
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static(if streaming {
            "text/event-stream; charset=utf-8"
        } else {
            "application/json"
        }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn rewrite_native_chunks(chunks: Vec<&[u8]>, expose_thinking: bool) -> String {
        let events = sse::parse_chunks(chunks.into_iter().map(Bytes::copy_from_slice).collect())
            .await
            .unwrap();
        let mut rewriter = NativeStreamRewriter::new("public".to_owned(), expose_thinking);
        let mut output = Vec::new();
        for event in events {
            output.extend(rewriter.push_event(event).unwrap());
        }
        String::from_utf8(sse::encode(output).await.to_vec()).unwrap()
    }

    async fn convert_chat_chunks(chunks: Vec<&[u8]>, expose_thinking: bool) -> String {
        let events = sse::parse_chunks(chunks.into_iter().map(Bytes::copy_from_slice).collect())
            .await
            .unwrap();
        let mut converter = StreamConverter::new("public-model".to_owned(), expose_thinking);
        let mut output = Vec::new();
        for event in events {
            output.extend(converter.push_event(&event).unwrap());
        }
        output.extend(converter.finish().unwrap());
        String::from_utf8(sse::encode(output).await.to_vec()).unwrap()
    }

    #[test]
    fn converts_tools_and_tool_results() {
        let converted = convert_request(&json!({
            "model": "claude-model", "max_tokens": 128,
            "system": [{"type": "text", "text": "Be precise", "cache_control": {"type": "ephemeral"}}],
            "messages": [
                {"role": "assistant", "content": [{"type": "tool_use", "id": "toolu_1", "name": "Read", "input": {"path": "a"}}]},
                {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_1", "content": "file"}]}
            ],
            "tools": [{"name": "Read", "description": "read", "input_schema": {"type": "object"}}],
            "tool_choice": {"type": "any", "disable_parallel_tool_use": true},
            "stream": true
        })).unwrap();
        assert_eq!(
            converted["messages"][0],
            json!({"role": "system", "content": "Be precise"})
        );
        assert_eq!(
            converted["messages"][1]["tool_calls"][0]["function"]["arguments"],
            "{\"path\":\"a\"}"
        );
        assert_eq!(converted["messages"][2]["role"], "tool");
        assert_eq!(
            converted["tools"][0]["function"]["parameters"]["type"],
            "object"
        );
        assert_eq!(converted["tool_choice"], "required");
        assert_eq!(converted["parallel_tool_calls"], false);
        assert_eq!(converted["stream_options"]["include_usage"], true);
    }

    #[test]
    fn maps_thinking_effort_and_structured_output() {
        let converted = convert_request(&json!({
            "model": "claude-model", "max_tokens": 4096,
            "messages": [{"role": "user", "content": "solve"}],
            "output_config": {
                "effort": "xhigh",
                "format": {"type": "json_schema", "schema": {
                    "type": "object", "properties": {"answer": {"type": "string"}}
                }}
            }
        }))
        .unwrap();
        assert_eq!(converted["reasoning_effort"], "xhigh");
        assert_eq!(converted["response_format"]["type"], "json_schema");
        assert_eq!(
            converted["response_format"]["json_schema"]["schema"]["type"],
            "object"
        );
    }

    #[test]
    fn rejects_chat_features_that_cannot_be_preserved() {
        let thinking = convert_request(&json!({
            "model": "claude-model", "max_tokens": 4096,
            "messages": [{"role": "user", "content": "solve"}],
            "thinking": {"type": "enabled", "budget_tokens": 2048}
        }));
        assert!(matches!(thinking, Err(GatewayError::UnsupportedFeature(_))));

        let adaptive = convert_request(&json!({
            "model": "claude-model", "max_tokens": 4096,
            "messages": [{"role": "user", "content": "solve"}],
            "thinking": {"type": "adaptive"}
        }));
        assert!(matches!(adaptive, Err(GatewayError::UnsupportedFeature(_))));

        let image_result = convert_request(&json!({
            "model": "claude-model", "max_tokens": 64,
            "messages": [{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool_1","content":[{"type":"image","source":{"type":"url","url":"https://example.test/a.png"}}]}]}]
        }));
        assert!(matches!(
            image_result,
            Err(GatewayError::UnsupportedFeature(_))
        ));
    }

    #[test]
    fn count_request_does_not_require_generation_fields() {
        let converted = convert_count_request(&json!({
            "model": "claude-model",
            "system": "system",
            "messages": [{"role": "user", "content": "count me"}],
            "tools": [{"name": "Read", "input_schema": {"type": "object"}}]
        }))
        .unwrap();
        assert!(converted.get("max_tokens").is_none());
        assert_eq!(converted["stream"], false);
        assert_eq!(converted["messages"][0]["role"], "system");
        assert_eq!(converted["tools"][0]["function"]["name"], "Read");
    }

    #[test]
    fn converts_non_streaming_tool_response() {
        let response = convert_response(
            br#"{"id":"chatcmpl_1","choices":[{"message":{"content":null,"tool_calls":[{"id":"call_1","function":{"name":"Read","arguments":"{\"path\":\"a\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":12,"completion_tokens":4}}"#,
            "public-model",
            true,
        )
        .unwrap();
        let value: Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(value["id"], "msg_chatcmpl_1");
        assert_eq!(value["model"], "public-model");
        assert_eq!(value["content"][0]["type"], "tool_use");
        assert_eq!(value["content"][0]["input"]["path"], "a");
        assert_eq!(value["stop_reason"], "tool_use");
        assert_eq!(value["usage"]["input_tokens"], 12);
    }

    #[test]
    fn preserves_native_anthropic_response_and_rewrites_model() {
        let response = rewrite_native_response(
            br#"{"id":"msg_1","type":"message","role":"assistant","model":"internal","content":[{"type":"thinking","thinking":"work","signature":"sig"},{"type":"text","text":"done"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":2},"future":true}"#,
            "public",
            true,
        )
        .unwrap();
        let value: Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(value["model"], "public");
        assert_eq!(value["content"][0]["signature"], "sig");
        assert_eq!(value["future"], true);

        let hidden = rewrite_native_response(
            br#"{"id":"msg_1","type":"message","role":"assistant","model":"internal","content":[{"type":"thinking","thinking":"private","signature":"sig"},{"type":"text","text":"done"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":2}}"#,
            "public",
            false,
        )
        .unwrap();
        let hidden: Value = serde_json::from_slice(&hidden).unwrap();
        assert_eq!(hidden["content"][0]["thinking"], "");
        assert_eq!(hidden["content"][0]["signature"], "sig");

        let count = rewrite_native_response(br#"{"input_tokens":42}"#, "public", true).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&count).unwrap()["input_tokens"],
            42
        );
    }

    #[tokio::test]
    async fn rewrites_fragmented_native_stream_model_without_losing_events() {
        let source = concat!(
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"internal\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n",
            "event: ping\ndata: { \"type\": \"ping\", \"future\": true }\n\n",
            "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
        );
        let split = source.len() / 2;
        let output = rewrite_native_chunks(
            vec![&source.as_bytes()[..split], &source.as_bytes()[split..]],
            true,
        )
        .await;
        assert!(output.contains(r#""model":"public""#));
        assert!(output.contains("event: ping"));
        assert!(output.contains(r#"data: { "type": "ping", "future": true }"#));
        assert!(output.contains("event: message_stop"));
        assert!(!output.contains("internal"));
    }

    #[tokio::test]
    async fn hides_native_stream_thinking_but_preserves_signature() {
        let source = concat!(
            "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"private\"}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"private delta\",\"estimated_tokens\":16}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"sig\"}}\n\n"
        );
        let output = rewrite_native_chunks(vec![source.as_bytes()], false).await;
        assert!(!output.contains("private"));
        assert!(output.contains(r#""thinking":"""#));
        assert!(output.contains(r#""estimated_tokens":16"#));
        assert!(output.contains(r#""signature":"sig""#));
    }

    #[test]
    fn preserves_text_and_tool_result_order() {
        let converted = convert_request(&json!({
            "model": "claude-model", "max_tokens": 64,
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "before"},
                {"type": "tool_result", "tool_use_id": "toolu_1", "content": "failed", "is_error": true},
                {"type": "text", "text": "after"}
            ]}]
        }))
        .unwrap();
        assert_eq!(converted["messages"][0]["content"], "before");
        assert_eq!(converted["messages"][1]["role"], "tool");
        assert_eq!(converted["messages"][1]["content"], "Error: failed");
        assert_eq!(converted["messages"][2]["content"], "after");
    }

    #[tokio::test]
    async fn converts_fragmented_stream() {
        let source = concat!(
            "data: {\"id\":\"chatcmpl_1\",\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl_1\",\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"Read\",\"arguments\":\"{\\\"path\\\":\"}}]},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"\\\"a\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":3}}\n\n",
            "data: [DONE]\n\n"
        );
        let split = source.len() / 3;
        let output = convert_chat_chunks(
            vec![
                &source.as_bytes()[..split],
                &source.as_bytes()[split..split * 2],
                &source.as_bytes()[split * 2..],
            ],
            false,
        )
        .await;
        assert!(output.contains("event: message_start"));
        assert!(output.contains("event: content_block_delta"));
        assert!(output.contains(r#""partial_json":"{\"path\":"#));
        assert!(output.contains(r#""stop_reason":"tool_use""#));
        assert!(output.ends_with("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"));
    }

    #[tokio::test]
    async fn streaming_thinking_blocks_close_before_text_and_tools() {
        let source = concat!(
            "data: {\"id\":\"chatcmpl_reasoning\",\"choices\":[{\"delta\":{\"reasoning\":\"work\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl_reasoning\",\"choices\":[{\"delta\":{\"content\":\"done\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"Read\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n"
        );
        let output = convert_chat_chunks(vec![source.as_bytes()], true).await;

        let thinking = output.find(r#""type":"thinking""#).unwrap();
        let thinking_delta = output.find(r#""type":"thinking_delta""#).unwrap();
        let signature = output.find(r#""type":"signature_delta""#).unwrap();
        let thinking_stop = output[signature..]
            .find("event: content_block_stop")
            .map(|offset| signature + offset)
            .unwrap();
        let text = output.find(r#""type":"text""#).unwrap();
        let text_stop = output[text..]
            .find("event: content_block_stop")
            .map(|offset| text + offset)
            .unwrap();
        let tool = output.find(r#""type":"tool_use""#).unwrap();
        assert!(thinking < thinking_delta);
        assert!(thinking_delta < signature && signature < thinking_stop);
        assert!(thinking_stop < text && text < text_stop && text_stop < tool);
        assert!(output.contains(r#""partial_json":"{}""#));
    }

    #[tokio::test]
    async fn streaming_text_closes_before_late_reasoning_and_repeated_tool_metadata_is_stable() {
        let source = concat!(
            "data: {\"id\":\"chatcmpl_reasoning\",\"choices\":[{\"delta\":{\"content\":\"answer\"},\"finish_reason\":null}]}\n\n",
            "data: {\"id\":\"chatcmpl_reasoning\",\"choices\":[{\"delta\":{\"reasoning\":\"late\"},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"Read\",\"arguments\":\"{\"}}]},\"finish_reason\":null}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"Read\",\"arguments\":\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
            "data: [DONE]\n\n"
        );
        let output = convert_chat_chunks(vec![source.as_bytes()], true).await;
        let text = output.find(r#""type":"text""#).unwrap();
        let text_stop = output[text..].find("event: content_block_stop").unwrap() + text;
        let thinking = output.find(r#""type":"thinking""#).unwrap();
        assert!(text < text_stop && text_stop < thinking);
        assert_eq!(output.matches(r#""id":"call_1""#).count(), 1);
        assert_eq!(output.matches(r#""name":"Read""#).count(), 1);
        assert!(output.contains(r#""partial_json":"{""#));
        assert!(output.contains(r#""partial_json":"}""#));
    }

    #[test]
    fn rejects_an_empty_upstream_stream() {
        let mut converter = StreamConverter::new("public-model".to_owned(), false);
        assert!(matches!(
            converter.finish(),
            Err(GatewayError::InvalidUpstreamResponse)
        ));
    }
}
