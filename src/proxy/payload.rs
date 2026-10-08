use bytes::Bytes;
use serde_json::Value;

use crate::{anthropic, anthropic_responses, config::AnthropicProtocol};

pub(super) struct PreparedPayload {
    pub(super) endpoint: String,
    pub(super) body: Bytes,
    pub(super) parsed: Value,
    pub(super) query: Option<String>,
}

pub(super) struct AnthropicPayloads {
    pub(super) allow_adapters: bool,
    pub(super) responses: Option<Result<PreparedPayload, String>>,
    pub(super) chat: Option<Result<PreparedPayload, String>>,
    pub(super) expose_thinking: bool,
}

impl AnthropicPayloads {
    pub(super) fn prepare(
        &mut self,
        protocol: AnthropicProtocol,
        source: &Value,
    ) -> Result<Option<&PreparedPayload>, String> {
        match protocol {
            AnthropicProtocol::Native => Ok(None),
            AnthropicProtocol::Responses => {
                if !self.allow_adapters {
                    return Err("count_tokens requires a native Anthropic upstream".to_owned());
                }
                let prepared = self.responses.get_or_insert_with(|| {
                    anthropic_responses::convert_request(source)
                        .map(|value| prepared_payload("responses", value))
                        .map_err(|error| error.to_string())
                });
                prepared.as_ref().map(Some).map_err(Clone::clone)
            }
            AnthropicProtocol::Chat => {
                if !self.allow_adapters {
                    return Err("count_tokens requires a native Anthropic upstream".to_owned());
                }
                let prepared = self.chat.get_or_insert_with(|| {
                    anthropic::convert_request(source)
                        .map(|value| prepared_payload("chat/completions", value))
                        .map_err(|error| error.to_string())
                });
                prepared.as_ref().map(Some).map_err(Clone::clone)
            }
            AnthropicProtocol::Auto => {
                unreachable!("Anthropic protocol must be resolved before payload preparation")
            }
        }
    }
}

pub(super) fn prepared_payload(endpoint: &str, parsed: Value) -> PreparedPayload {
    PreparedPayload {
        endpoint: endpoint.to_owned(),
        body: sonic_rs::to_vec(&parsed)
            .map(Bytes::from)
            .expect("serializing a JSON value cannot fail"),
        parsed,
        query: None,
    }
}
