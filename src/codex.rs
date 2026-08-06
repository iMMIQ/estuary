use std::{collections::HashMap, sync::Arc};

use bytes::{Bytes, BytesMut};
use serde_json::Value;

use crate::error::GatewayError;

const NAMESPACE_SEPARATOR: &str = "__";

#[derive(Clone, Debug)]
struct NamespaceTarget {
    namespace: String,
    name: String,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NamespaceMap {
    targets: HashMap<String, NamespaceTarget>,
}

impl NamespaceMap {
    fn insert(
        &mut self,
        flattened: String,
        namespace: &str,
        name: &str,
    ) -> Result<(), GatewayError> {
        let target = NamespaceTarget {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        };
        if let Some(existing) = self.targets.get(&flattened) {
            if existing.namespace == target.namespace && existing.name == target.name {
                return Ok(());
            }
            return Err(GatewayError::InvalidRequest(format!(
                "Codex namespace tools produce the duplicate flattened name `{flattened}`"
            )));
        }
        self.targets.insert(flattened, target);
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }
}

pub(crate) fn is_request(headers: &http::HeaderMap, body: Option<&Value>) -> bool {
    let user_agent_is_codex = headers
        .get(http::header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("codex"));
    let metadata_is_codex = body
        .and_then(|value| value.get("client_metadata"))
        .and_then(Value::as_object)
        .is_some_and(|metadata| {
            metadata.contains_key("x-codex-turn-metadata")
                || metadata.contains_key("x-codex-installation-id")
        });
    user_agent_is_codex || metadata_is_codex
}

pub(crate) fn normalize_vllm_request(
    body: &mut Value,
) -> Result<Option<Arc<NamespaceMap>>, GatewayError> {
    let object = body.as_object_mut().ok_or_else(|| {
        GatewayError::InvalidRequest("Responses request body must be an object".to_owned())
    })?;
    let mut namespaces = NamespaceMap::default();

    normalize_input(object.get_mut("input"), &mut namespaces)?;
    normalize_tools(object.get_mut("tools"), &mut namespaces)?;

    Ok((!namespaces.is_empty()).then(|| Arc::new(namespaces)))
}

fn normalize_input(
    input: Option<&mut Value>,
    namespaces: &mut NamespaceMap,
) -> Result<(), GatewayError> {
    let Some(items) = input.and_then(Value::as_array_mut) else {
        return Ok(());
    };
    for item in items {
        let Some(object) = item.as_object_mut() else {
            continue;
        };
        match object.get("type").and_then(Value::as_str) {
            Some("additional_tools") => {
                return Err(GatewayError::InvalidRequest(
                    "Codex Responses Lite `additional_tools` is not supported by vLLM 0.25; use model metadata with `use_responses_lite: false`"
                        .to_owned(),
                ));
            }
            Some(
                "custom_tool_call"
                | "custom_tool_call_output"
                | "tool_search_call"
                | "tool_search_output",
            ) => {
                return Err(GatewayError::InvalidRequest(
                    "Codex custom and tool-search history is not supported by vLLM 0.25; use a non-Responses-Lite Codex model profile"
                    .to_owned(),
                ));
            }
            Some("web_search_call") => {
                return Err(GatewayError::InvalidRequest(
                    "Codex web-search history cannot be replayed by this vLLM 0.25 deployment; set `web_search = \"disabled\"` and start a new Codex thread"
                        .to_owned(),
                ));
            }
            Some("function_call") => normalize_function_call(object, namespaces)?,
            Some("message" | "reasoning" | "function_call_output") | None => {}
            Some(unsupported) => {
                return Err(GatewayError::InvalidRequest(format!(
                    "Codex input item type `{unsupported}` is not supported by the vLLM 0.25 Harmony path; use a non-Responses-Lite model profile and start a new Codex thread"
                )));
            }
        }
    }
    Ok(())
}

fn normalize_function_call(
    object: &mut serde_json::Map<String, Value>,
    namespaces: &mut NamespaceMap,
) -> Result<(), GatewayError> {
    let Some(namespace) = object
        .get("namespace")
        .and_then(Value::as_str)
        .map(str::to_owned)
    else {
        return Ok(());
    };
    let name = required_string(object, "name", "Codex namespaced function call")?.to_owned();
    let flattened = flattened_name(&namespace, &name);
    namespaces.insert(flattened.clone(), &namespace, &name)?;
    object.insert("name".to_owned(), Value::String(flattened));
    object.remove("namespace");
    Ok(())
}

fn normalize_tools(
    tools: Option<&mut Value>,
    namespaces: &mut NamespaceMap,
) -> Result<(), GatewayError> {
    let Some(tools) = tools else {
        return Ok(());
    };
    if tools.is_null() {
        return Ok(());
    }
    let entries = tools.as_array_mut().ok_or_else(|| {
        GatewayError::InvalidRequest("Responses `tools` must be an array".to_owned())
    })?;
    let original = std::mem::take(entries);
    let mut normalized = Vec::with_capacity(original.len());
    let mut function_owners = HashMap::<String, &'static str>::new();

    for tool in original {
        let object = tool.as_object().ok_or_else(|| {
            GatewayError::InvalidRequest("Responses tool entries must be objects".to_owned())
        })?;
        let tool_type = required_string(object, "type", "Responses tool")?;
        match tool_type {
            "function" => {
                let name = required_string(object, "name", "Responses function tool")?;
                if namespaces.targets.contains_key(name) {
                    return Err(duplicate_tool_name(name));
                }
                if function_owners
                    .insert(name.to_owned(), "function")
                    .is_some()
                {
                    return Err(duplicate_tool_name(name));
                }
                normalized.push(tool);
            }
            "namespace" => {
                let namespace = required_string(object, "name", "Codex namespace tool")?;
                let namespace_description = object
                    .get("description")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let nested = object
                    .get("tools")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        GatewayError::InvalidRequest(
                            "Codex namespace tool must contain a `tools` array".to_owned(),
                        )
                    })?;
                for nested_tool in nested {
                    let mut flattened_tool = nested_tool.as_object().cloned().ok_or_else(|| {
                        GatewayError::InvalidRequest(
                            "Codex namespace tool entries must be objects".to_owned(),
                        )
                    })?;
                    if flattened_tool.get("type").and_then(Value::as_str) != Some("function") {
                        return Err(GatewayError::InvalidRequest(
                            "vLLM 0.25 supports only function tools inside a Codex namespace"
                                .to_owned(),
                        ));
                    }
                    let name =
                        required_string(&flattened_tool, "name", "Codex namespaced function tool")?
                            .to_owned();
                    let flattened = flattened_name(namespace, &name);
                    if function_owners
                        .insert(flattened.clone(), "namespace")
                        .is_some()
                    {
                        return Err(duplicate_tool_name(&flattened));
                    }
                    namespaces.insert(flattened.clone(), namespace, &name)?;
                    flattened_tool.insert("name".to_owned(), Value::String(flattened));
                    merge_namespace_description(&mut flattened_tool, namespace_description);
                    normalized.push(Value::Object(flattened_tool));
                }
            }
            "web_search" => {
                return Err(GatewayError::InvalidRequest(
                    "Codex `web_search` has no vLLM 0.25 backend; set `web_search = \"disabled\"` in Codex config"
                        .to_owned(),
                ));
            }
            "web_search_preview" | "code_interpreter" | "container" => {
                normalized.push(tool);
            }
            unsupported => {
                return Err(GatewayError::InvalidRequest(format!(
                    "Codex tool type `{unsupported}` is not supported by the vLLM 0.25 Harmony path; use a non-Responses-Lite model profile"
                )));
            }
        }
    }
    *entries = normalized;
    Ok(())
}

fn required_string<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &str,
    context: &str,
) -> Result<&'a str, GatewayError> {
    object.get(field).and_then(Value::as_str).ok_or_else(|| {
        GatewayError::InvalidRequest(format!("{context} must contain a string `{field}`"))
    })
}

fn flattened_name(namespace: &str, name: &str) -> String {
    format!("{namespace}{NAMESPACE_SEPARATOR}{name}")
}

fn duplicate_tool_name(name: &str) -> GatewayError {
    GatewayError::InvalidRequest(format!(
        "Codex tools contain the duplicate upstream function name `{name}`"
    ))
}

fn merge_namespace_description(
    tool: &mut serde_json::Map<String, Value>,
    namespace_description: &str,
) {
    if namespace_description.is_empty() {
        return;
    }
    let nested = tool
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let description = if nested.is_empty() {
        namespace_description.to_owned()
    } else {
        format!("{namespace_description}\n\n{nested}")
    };
    tool.insert("description".to_owned(), Value::String(description));
}

pub(crate) fn rewrite_response(
    bytes: &[u8],
    namespaces: &NamespaceMap,
) -> Result<Bytes, GatewayError> {
    let mut value: Value =
        serde_json::from_slice(bytes).map_err(|_| GatewayError::InvalidUpstreamResponse)?;
    rewrite_value(&mut value, namespaces);
    serde_json::to_vec(&value)
        .map(Bytes::from)
        .map_err(|_| GatewayError::Internal)
}

fn rewrite_value(value: &mut Value, namespaces: &NamespaceMap) {
    match value {
        Value::Array(values) => {
            for value in values {
                rewrite_value(value, namespaces);
            }
        }
        Value::Object(object) => {
            let target = (object.get("type").and_then(Value::as_str) == Some("function_call"))
                .then(|| object.get("name").and_then(Value::as_str))
                .flatten()
                .and_then(|name| namespaces.targets.get(name))
                .cloned();
            if let Some(target) = target {
                object.insert("name".to_owned(), Value::String(target.name));
                object.insert("namespace".to_owned(), Value::String(target.namespace));
            }
            for value in object.values_mut() {
                rewrite_value(value, namespaces);
            }
        }
        _ => {}
    }
}

pub(crate) struct StreamRewriter {
    buffer: BytesMut,
    namespaces: Arc<NamespaceMap>,
}

impl StreamRewriter {
    pub(crate) fn new(namespaces: Arc<NamespaceMap>) -> Self {
        Self {
            buffer: BytesMut::new(),
            namespaces,
        }
    }

    pub(crate) fn push(&mut self, bytes: &[u8]) -> Result<Bytes, GatewayError> {
        self.buffer.extend_from_slice(bytes);
        let mut output = Vec::new();
        while let Some((end, delimiter)) = frame_end(&self.buffer) {
            let frame = self.buffer.split_to(end);
            let _ = self.buffer.split_to(delimiter);
            rewrite_sse_frame(&frame, delimiter, &self.namespaces, &mut output)?;
        }
        Ok(Bytes::from(output))
    }

    pub(crate) fn finish(&mut self) -> Result<Bytes, GatewayError> {
        if !self.buffer.iter().all(u8::is_ascii_whitespace) {
            return Err(GatewayError::InvalidUpstreamResponse);
        }
        self.buffer.clear();
        Ok(Bytes::new())
    }
}

fn frame_end(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = buffer.windows(2).position(|window| window == b"\n\n");
    let crlf = buffer.windows(4).position(|window| window == b"\r\n\r\n");
    match (lf, crlf) {
        (Some(lf), Some(crlf)) if lf < crlf => Some((lf, 2)),
        (_, Some(crlf)) => Some((crlf, 4)),
        (Some(lf), None) => Some((lf, 2)),
        (None, None) => None,
    }
}

fn rewrite_sse_frame(
    frame: &[u8],
    delimiter: usize,
    namespaces: &NamespaceMap,
    output: &mut Vec<u8>,
) -> Result<(), GatewayError> {
    let text = std::str::from_utf8(frame).map_err(|_| GatewayError::InvalidUpstreamResponse)?;
    let newline = if delimiter == 4 { "\r\n" } else { "\n" };
    let lines = text
        .split('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        .collect::<Vec<_>>();
    let data = lines
        .iter()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim_start)
        .collect::<Vec<_>>();
    if data.is_empty() || data == ["[DONE]"] {
        output.extend_from_slice(frame);
        output.extend_from_slice(newline.as_bytes());
        output.extend_from_slice(newline.as_bytes());
        return Ok(());
    }
    let mut value: Value = serde_json::from_str(&data.join("\n"))
        .map_err(|_| GatewayError::InvalidUpstreamResponse)?;
    rewrite_value(&mut value, namespaces);
    let encoded = serde_json::to_string(&value).map_err(|_| GatewayError::Internal)?;
    let mut emitted_data = false;
    for line in lines {
        if line.starts_with("data:") {
            if emitted_data {
                continue;
            }
            output.extend_from_slice(b"data: ");
            output.extend_from_slice(encoded.as_bytes());
            emitted_data = true;
        } else {
            output.extend_from_slice(line.as_bytes());
        }
        output.extend_from_slice(newline.as_bytes());
    }
    output.extend_from_slice(newline.as_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn flattens_namespace_tools_and_history() {
        let mut request = json!({
            "model": "gpt-oss-20b",
            "tools": [{
                "type": "namespace",
                "name": "collaboration",
                "description": "Manage workers.",
                "tools": [{
                    "type": "function",
                    "name": "spawn_agent",
                    "description": "Start a worker.",
                    "parameters": {"type": "object"}
                }]
            }],
            "input": [{
                "type": "function_call",
                "namespace": "collaboration",
                "name": "spawn_agent",
                "call_id": "call_1",
                "arguments": "{}"
            }]
        });
        let namespaces = normalize_vllm_request(&mut request)
            .expect("normalize request")
            .expect("namespace map");
        assert_eq!(request["tools"][0]["type"], "function");
        assert_eq!(request["tools"][0]["name"], "collaboration__spawn_agent");
        assert_eq!(
            request["tools"][0]["description"],
            "Manage workers.\n\nStart a worker."
        );
        assert_eq!(request["input"][0]["name"], "collaboration__spawn_agent");
        assert!(request["input"][0].get("namespace").is_none());

        let rewritten = rewrite_response(
            br#"{"output":[{"type":"function_call","name":"collaboration__spawn_agent","call_id":"call_2","arguments":"{}"}]}"#,
            &namespaces,
        )
        .expect("rewrite response");
        let response: Value = serde_json::from_slice(&rewritten).expect("response JSON");
        assert_eq!(response["output"][0]["namespace"], "collaboration");
        assert_eq!(response["output"][0]["name"], "spawn_agent");
    }

    #[test]
    fn rewrites_split_sse_frames() {
        let mut request = json!({
            "tools": [{"type":"namespace","name":"web","tools":[{
                "type":"function","name":"run","parameters":{"type":"object"}
            }]}],
            "input": "test"
        });
        let namespaces = normalize_vllm_request(&mut request)
            .expect("normalize")
            .expect("namespaces");
        let source = concat!(
            "event: response.output_item.added\r\n",
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"name\":\"web__run\"}}\r\n\r\n",
            "data: [DONE]\r\n\r\n"
        );
        let split = source.len() / 3;
        let mut rewriter = StreamRewriter::new(namespaces);
        let mut output = Vec::new();
        output.extend_from_slice(&rewriter.push(&source.as_bytes()[..split]).expect("part 1"));
        output.extend_from_slice(
            &rewriter
                .push(&source.as_bytes()[split..split * 2])
                .expect("part 2"),
        );
        output.extend_from_slice(
            &rewriter
                .push(&source.as_bytes()[split * 2..])
                .expect("part 3"),
        );
        output.extend_from_slice(&rewriter.finish().expect("finish"));
        let output = String::from_utf8(output).expect("UTF-8 output");
        assert!(output.contains("\"name\":\"run\",\"namespace\":\"web\""));
        assert!(output.contains("data: [DONE]"));
    }

    #[test]
    fn rejects_codex_tools_vllm_cannot_execute() {
        for (tool, expected) in [
            (json!({"type":"web_search"}), "web_search = \"disabled\""),
            (
                json!({"type":"custom","name":"exec","format":{"type":"text"}}),
                "non-Responses-Lite",
            ),
        ] {
            let mut request = json!({"input":"test","tools":[tool]});
            let error = normalize_vllm_request(&mut request).expect_err("must reject");
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn rejects_function_name_ambiguous_with_namespaced_history() {
        let mut request = json!({
            "tools": [{"type":"function","name":"web__run","parameters":{"type":"object"}}],
            "input": [{
                "type":"function_call",
                "namespace":"web",
                "name":"run",
                "call_id":"call_1",
                "arguments":"{}"
            }]
        });
        let error = normalize_vllm_request(&mut request).expect_err("must reject collision");
        assert!(
            error
                .to_string()
                .contains("duplicate upstream function name")
        );
    }

    #[test]
    fn processes_the_earliest_sse_frame_with_mixed_newlines() {
        let mut request = json!({
            "tools": [{"type":"namespace","name":"web","tools":[{
                "type":"function","name":"run","parameters":{"type":"object"}
            }]}]
        });
        let namespaces = normalize_vllm_request(&mut request)
            .expect("normalize")
            .expect("namespaces");
        let source = concat!(
            "data: {\"type\":\"function_call\",\"name\":\"web__run\"}\n\n",
            "data: [DONE]\r\n\r\n"
        );
        let mut rewriter = StreamRewriter::new(namespaces);
        let output = rewriter.push(source.as_bytes()).expect("rewrite frames");
        let output = String::from_utf8(output.to_vec()).expect("UTF-8 output");
        assert!(output.starts_with("data: {\"name\":\"run\",\"namespace\":\"web\""));
        assert!(output.ends_with("data: [DONE]\r\n\r\n"));
    }
}
