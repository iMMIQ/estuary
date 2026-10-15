use serde_json::Value;

use crate::error::GatewayError;

pub(super) const CLAUDE_CODE_BILLING_PREFIX: &str = "x-anthropic-billing-header:";

pub(super) fn normalize_context_management(body: &mut Value) -> Result<bool, GatewayError> {
    let object = body.as_object_mut().ok_or_else(|| {
        GatewayError::InvalidRequest("JSON request body must be an object".to_owned())
    })?;
    let Some(context) = object.get("context_management") else {
        return Ok(false);
    };
    if context.is_null() {
        object.remove("context_management");
        return Ok(true);
    }
    let context = context.as_object().ok_or_else(|| {
        GatewayError::InvalidRequest("Anthropic context_management must be an object".to_owned())
    })?;
    let edits = context
        .get("edits")
        .map(|edits| {
            edits.as_array().ok_or_else(|| {
                GatewayError::InvalidRequest(
                    "Anthropic context_management.edits must be an array".to_owned(),
                )
            })
        })
        .transpose()?
        .map(Vec::as_slice)
        .unwrap_or_default();
    let all_noop = edits.iter().all(|edit| {
        let Some(edit) = edit.as_object() else {
            return false;
        };
        if edit.get("type").and_then(Value::as_str) != Some("clear_thinking_20251015") {
            return false;
        }
        matches!(edit.get("keep"), Some(Value::String(keep)) if keep == "all")
            || edit
                .get("keep")
                .and_then(Value::as_object)
                .and_then(|keep| keep.get("type"))
                .and_then(Value::as_str)
                == Some("all")
    });
    if !all_noop {
        return Err(GatewayError::UnsupportedFeature(
            "vLLM 0.25 cannot apply Anthropic context-management edits",
        ));
    }
    object.remove("context_management");
    Ok(true)
}

pub(super) fn strip_claude_code_billing_blocks(body: &mut Value) -> bool {
    let Some(object) = body.as_object_mut() else {
        return false;
    };
    let mut changed = false;

    let remove_system = match object.get_mut("system") {
        Some(Value::String(text)) => is_claude_code_billing_text(text),
        Some(Value::Array(blocks)) => {
            let before = blocks.len();
            blocks.retain(|block| !is_claude_code_billing_block(block));
            changed |= blocks.len() != before;
            blocks.is_empty()
        }
        _ => false,
    };
    if remove_system {
        object.remove("system");
        changed = true;
    }

    if let Some(Value::Array(messages)) = object.get_mut("messages") {
        messages.retain_mut(|message| {
            if message.get("role").and_then(Value::as_str) != Some("system") {
                return true;
            }
            let Some(content) = message.get_mut("content") else {
                return true;
            };
            match content {
                Value::String(text) if is_claude_code_billing_text(text) => {
                    changed = true;
                    false
                }
                Value::Array(blocks) => {
                    let before = blocks.len();
                    blocks.retain(|block| !is_claude_code_billing_block(block));
                    changed |= blocks.len() != before;
                    !blocks.is_empty()
                }
                _ => true,
            }
        });
    }

    changed
}

pub(super) fn replace_unsupported_images(body: &mut Value, model: &str) -> usize {
    fn visit_content(value: &mut Value, model: &str) -> usize {
        match value {
            Value::Array(items) => items
                .iter_mut()
                .map(|item| visit_content(item, model))
                .sum(),
            Value::Object(object) => {
                let replacement_type =
                    object
                        .get("type")
                        .and_then(Value::as_str)
                        .and_then(|kind| match kind {
                            "image" | "image_url" => Some("text"),
                            "input_image" => Some("input_text"),
                            _ => None,
                        });
                if let Some(replacement_type) = replacement_type {
                    *value = serde_json::json!({
                        "type": replacement_type,
                        "text": format!(
                            "[Estuary gateway notice: this image was omitted because model {model:?} does not support image input. Do not claim to have inspected it; ask the user for a text description or suggest a multimodal model.]"
                        )
                    });
                    return 1;
                }
                object
                    .iter_mut()
                    .filter(|(key, _)| key.as_str() != "type")
                    .map(|(_, value)| visit_content(value, model))
                    .sum()
            }
            _ => 0,
        }
    }

    let Some(object) = body.as_object_mut() else {
        return 0;
    };
    let mut replaced = 0;
    for field in ["messages", "input", "system"] {
        if let Some(content) = object.get_mut(field) {
            replaced += visit_content(content, model);
        }
    }
    replaced
}

pub(super) fn is_claude_code_billing_block(value: &Value) -> bool {
    value.as_str().is_some_and(is_claude_code_billing_text)
        || value
            .as_object()
            .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
            .and_then(|block| block.get("text"))
            .and_then(Value::as_str)
            .is_some_and(is_claude_code_billing_text)
}

pub(super) fn is_claude_code_billing_text(text: &str) -> bool {
    let text = text.trim();
    text.starts_with(CLAUDE_CODE_BILLING_PREFIX) && !text.contains(['\r', '\n'])
}

pub(super) fn reject_stateful_responses(body: Option<&Value>) -> Result<(), GatewayError> {
    let Some(body) = body else {
        return Ok(());
    };
    if body.get("background").and_then(Value::as_bool) == Some(true) {
        return Err(GatewayError::UnsupportedFeature(
            "Responses background mode is deferred to the durable-affinity phase",
        ));
    }
    if body
        .get("previous_response_id")
        .is_some_and(|value| !value.is_null())
    {
        return Err(GatewayError::UnsupportedFeature(
            "Responses previous_response_id requires durable node affinity",
        ));
    }
    if body
        .get("conversation")
        .is_some_and(|value| !value.is_null())
    {
        return Err(GatewayError::UnsupportedFeature(
            "Responses conversation state requires durable node affinity",
        ));
    }
    Ok(())
}
