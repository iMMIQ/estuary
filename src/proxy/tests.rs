use super::headers::{connection_header_names, should_forward_request_header};
use super::request_compat::apply_vllm_native_thinking_compat;
use super::streaming::{LimitedSseInput, SseInputError};
use super::upstream::{NativeMessagesCompat, mapped_body};
use axum::http::{HeaderName, HeaderValue};
use std::io;

use bytes::Bytes;
use futures_util::{StreamExt, stream};
use serde_json::json;

use super::*;

#[tokio::test]
async fn request_body_reader_enforces_size_and_idle_timeouts() {
    let oversized = Body::from_stream(stream::iter([Ok::<_, io::Error>(Bytes::from_static(
        b"too large",
    ))]));
    assert!(matches!(
        read_request_body(oversized, 4, Duration::from_secs(1), Duration::from_secs(1)).await,
        Err(GatewayError::PayloadTooLarge)
    ));

    let stalled = Body::from_stream(stream::pending::<Result<Bytes, io::Error>>());
    assert!(matches!(
        read_request_body(
            stalled,
            16,
            Duration::from_millis(10),
            Duration::from_secs(1)
        )
        .await,
        Err(GatewayError::RequestTimeout)
    ));
}

#[tokio::test]
async fn sse_input_rejects_an_event_over_the_configured_limit() {
    let source = stream::iter([Ok::<_, io::Error>(Bytes::from_static(
        b"data: too large\n\n",
    ))]);
    let error = LimitedSseInput::new(source, 8)
        .next()
        .await
        .expect("one input chunk")
        .expect_err("oversized event must fail");
    assert!(matches!(error, SseInputError::EventTooLarge));
}

#[tokio::test]
async fn sse_input_resets_its_limit_for_all_line_endings_and_split_boundaries() {
    for chunks in [
        vec![
            Bytes::from_static(b"1234567\n"),
            Bytes::from_static(b"\n1234567\n\n"),
        ],
        vec![
            Bytes::from_static(b"1234567\r\n\r"),
            Bytes::from_static(b"\n1234567\r\n\r\n"),
        ],
        vec![
            Bytes::from_static(b"1234567\r"),
            Bytes::from_static(b"\r1234567\r\r"),
        ],
    ] {
        let source = stream::iter(chunks.into_iter().map(Ok::<_, io::Error>));
        let results = LimitedSseInput::new(source, 11).collect::<Vec<_>>().await;
        assert!(results.iter().all(Result::is_ok));
    }
}

#[test]
fn strips_client_credentials_and_hop_headers() {
    for name in [
        "authorization",
        "openai-organization",
        "openai-project",
        "connection",
        "host",
        "content-length",
    ] {
        assert!(!should_forward_request_header(
            &HeaderName::from_bytes(name.as_bytes()).unwrap()
        ));
    }
    assert!(should_forward_request_header(&HeaderName::from_static(
        "openai-beta"
    )));

    let headers = HeaderMap::from_iter([
        (
            HeaderName::from_static("connection"),
            HeaderValue::from_static("keep-alive, x-remove-me"),
        ),
        (
            HeaderName::from_static("x-remove-me"),
            HeaderValue::from_static("secret"),
        ),
    ]);
    assert!(connection_header_names(&headers).contains(&HeaderName::from_static("x-remove-me")));
}

#[test]
fn strips_real_claude_code_billing_blocks_without_touching_prompt_context() {
    let mut body = json!({
        "model": "claude-sonnet-4-5",
        "metadata": {
            "user_id": "{\"device_id\":\"device-hash\",\"session_id\":\"session-id\"}"
        },
        "system": [
            {
                "type": "text",
                "text": "x-anthropic-billing-header: cc_version=2.1.220.8a5; cc_entrypoint=sdk-cli;"
            },
            {
                "type": "text",
                "text": "You are a Claude agent, built on Anthropic's Claude Agent SDK.",
                "cache_control": {"type": "ephemeral"}
            },
            {
                "type": "text",
                "text": "# Environment\n - Primary working directory: /workspace\n - Is a git repository: true"
            }
        ],
        "messages": [{"role": "user", "content": "Implement the change"}]
    });

    assert!(strip_claude_code_billing_blocks(&mut body));
    assert_eq!(body["system"].as_array().unwrap().len(), 2);
    assert_eq!(
        body["system"][0]["text"],
        "You are a Claude agent, built on Anthropic's Claude Agent SDK."
    );
    assert!(
        body["system"][1]["text"]
            .as_str()
            .unwrap()
            .contains("# Environment")
    );
    assert!(
        body["metadata"]["user_id"]
            .as_str()
            .unwrap()
            .contains("device-hash")
    );
}

#[test]
fn replaces_image_parts_with_protocol_specific_text_parts() {
    let mut body = json!({
        "model": "text-only",
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "before"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,secret"}},
                {"type": "image", "source": {"type": "url", "url": "https://example.test/a.png"}}
            ]
        }],
        "input": [{"type": "message", "content": [{"type": "input_image", "image_url": "https://example.test/b.png"}]}]
        ,"tools": [{"type":"function","function":{"parameters":{"type":"image"}}}]
    });

    assert_eq!(replace_unsupported_images(&mut body, "text-only"), 3);
    let serialized = body.to_string();
    assert!(!serialized.contains("secret"));
    assert!(!serialized.contains("example.test"));
    assert_eq!(body["messages"][0]["content"][1]["type"], "text");
    assert_eq!(body["input"][0]["content"][0]["type"], "input_text");
    assert_eq!(body["tools"][0]["function"]["parameters"]["type"], "image");
    assert!(
        body["messages"][0]["content"][1]["text"]
            .as_str()
            .unwrap()
            .contains("text-only")
    );
}

#[test]
fn leaves_requests_without_images_unchanged() {
    let mut body = json!({
        "model": "text-only",
        "messages": [{"role": "user", "content": "hello"}]
    });
    let before = body.clone();
    assert_eq!(replace_unsupported_images(&mut body, "text-only"), 0);
    assert_eq!(body, before);
}

#[test]
fn replaces_images_nested_in_responses_tool_outputs() {
    let mut body = json!({
        "model": "text-only",
        "input": [{
            "type": "function_call_output",
            "call_id": "view-image-call",
            "output": [{
                "type": "input_image",
                "image_url": "data:image/png;base64,secret"
            }]
        }]
    });

    assert_eq!(replace_unsupported_images(&mut body, "text-only"), 1);
    let serialized = body.to_string();
    assert!(!serialized.contains("secret"));
    assert_eq!(body["input"][0]["output"][0]["type"], "input_text");
    assert!(
        body["input"][0]["output"][0]["text"]
            .as_str()
            .unwrap()
            .contains("text-only")
    );
}

#[test]
fn claude_code_versions_share_the_same_sanitized_prefix() {
    let config = crate::config::PrefixConfig::default();
    let request = |version: &str| {
        json!({
            "system": [
                {
                    "type": "text",
                    "text": format!("x-anthropic-billing-header: cc_version={version}; cc_entrypoint=sdk-cli;")
                },
                {"type": "text", "text": "Stable agent instructions"}
            ],
            "messages": [{"role": "user", "content": "shared task"}]
        })
    };
    let mut first = request("2.1.220.8a5");
    let mut second = request("2.1.221.9b6");
    assert!(strip_claude_code_billing_blocks(&mut first));
    assert!(strip_claude_code_billing_blocks(&mut second));

    let first = routing_text("chat/completions", Some("model"), Some(&first), &config);
    let second = routing_text("chat/completions", Some("model"), Some(&second), &config);
    let directory = crate::prefix::PrefixDirectory::new(&config);
    directory.record("node-a", &first);
    let matched = directory.best_match(&second);
    assert_eq!(matched.node_ids, ["node-a"]);
    assert_eq!(matched.matched_chars, matched.input_chars);
}

#[test]
fn does_not_strip_multiline_or_non_system_user_content() {
    let mut body = json!({
        "system": "x-anthropic-billing-header: explain this value\nDo not delete this instruction",
        "messages": [{
            "role": "user",
            "content": "x-anthropic-billing-header: cc_version=user-supplied;"
        }]
    });
    assert!(!strip_claude_code_billing_blocks(&mut body));
}

#[test]
fn rejects_stateful_responses_features() {
    assert!(reject_stateful_responses(Some(&json!({"background": true}))).is_err());
    assert!(reject_stateful_responses(Some(&json!({"previous_response_id": "resp_1"}))).is_err());
}

#[test]
fn removes_only_noop_claude_context_management() {
    let mut request = json!({
        "context_management": {
            "edits": [{"type": "clear_thinking_20251015", "keep": "all"}]
        }
    });
    assert!(normalize_context_management(&mut request).unwrap());
    assert!(request.get("context_management").is_none());

    let mut unsupported = json!({
        "context_management": {
            "edits": [{
                "type": "clear_tool_uses_20250919",
                "keep": {"type": "tool_uses", "value": 5}
            }]
        }
    });
    assert!(normalize_context_management(&mut unsupported).is_err());
}

#[test]
fn anthropic_adapter_payloads_are_built_only_when_selected() {
    let source = json!({
        "model": "claude",
        "max_tokens": 128,
        "messages": [{"role": "user", "content": "hello"}]
    });
    let mut payloads = AnthropicPayloads {
        allow_adapters: true,
        responses: None,
        chat: None,
        expose_thinking: false,
    };

    assert!(
        payloads
            .prepare(AnthropicProtocol::Native, &source)
            .unwrap()
            .is_none()
    );
    assert!(payloads.chat.is_none());
    assert!(payloads.responses.is_none());

    let endpoint = payloads
        .prepare(AnthropicProtocol::Responses, &source)
        .unwrap()
        .unwrap()
        .endpoint
        .clone();
    assert_eq!(endpoint, "responses");
    assert!(payloads.responses.is_some());
    assert!(payloads.chat.is_none());
}

#[test]
fn native_vllm_request_reuses_an_unchanged_body() {
    let original = Bytes::from_static(
        br#"{"model":"model","max_tokens":128,"messages":[{"role":"user","content":"hello"}]}"#,
    );
    let parsed = serde_json::from_slice(&original).unwrap();
    let (mapped, approximated, namespaces) = mapped_body(
        &original,
        Some(&parsed),
        Some("model"),
        Some("model"),
        NativeMessagesCompat::Legacy,
        false,
        false,
    )
    .unwrap();

    assert_eq!(mapped.as_ptr(), original.as_ptr());
    assert!(!approximated);
    assert!(namespaces.is_none());
}

#[test]
fn native_vllm_empty_tools_do_not_require_a_tool_parser() {
    for choice in [None, Some(json!(null)), Some(json!({"type":"auto"}))] {
        let mut request =
            json!({"model":"m","tools":[],"messages":[{"role":"user","content":"hello"}]});
        if let Some(choice) = choice {
            request["tool_choice"] = choice;
        }
        let original = Bytes::from(serde_json::to_vec(&request).unwrap());
        let (body, _, _) = mapped_body(
            &original,
            Some(&request),
            Some("m"),
            Some("m"),
            NativeMessagesCompat::Legacy,
            false,
            false,
        )
        .unwrap();
        let mapped: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(mapped.get("tools").is_none());
        assert!(mapped.get("tool_choice").is_none());
        assert_eq!(mapped["messages"], request["messages"]);
        let (passthrough, _, _) = mapped_body(
            &original,
            Some(&request),
            Some("m"),
            Some("m"),
            NativeMessagesCompat::None,
            false,
            false,
        )
        .unwrap();
        assert_eq!(passthrough.as_ptr(), original.as_ptr());
    }
    for request in [
        json!({"tools":[],"tool_choice":{"type":"any"}}),
        json!({"tools":[],"tool_choice":{"type":"tool","name":"Read"}}),
        json!({"tools":[],"tool_choice":{"type":"none"}}),
        json!({"tools":[{"name":"Read"}]}),
        json!({"tools":"invalid"}),
    ] {
        let original = Bytes::from(serde_json::to_vec(&request).unwrap());
        let (mapped, _, _) = mapped_body(
            &original,
            Some(&request),
            None,
            None,
            NativeMessagesCompat::Legacy,
            false,
            false,
        )
        .unwrap();
        assert_eq!(mapped.as_ptr(), original.as_ptr());
    }
}

#[test]
fn maps_anthropic_thinking_to_vllm_template_control() {
    let mut enabled = json!({
        "max_tokens": 32000,
        "thinking": {"type": "enabled", "budget_tokens": 31999, "display": "omitted"},
        "chat_template_kwargs": {"custom": true}
    });
    let approximated = apply_vllm_native_thinking_compat(enabled.as_object_mut().unwrap()).unwrap();
    assert!(approximated);
    assert!(enabled["thinking"].get("display").is_none());
    assert_eq!(enabled["chat_template_kwargs"]["enable_thinking"], true);
    assert_eq!(enabled["chat_template_kwargs"]["custom"], true);

    let mut disabled = json!({"thinking": {"type": "disabled"}});
    assert!(!apply_vllm_native_thinking_compat(disabled.as_object_mut().unwrap()).unwrap());
    assert_eq!(disabled["chat_template_kwargs"]["enable_thinking"], false);
}
