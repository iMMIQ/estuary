//! Parsed model output chunks and protocol response chunk generators.

use std::sync::Arc;

use deepseek_recipe_core::special_tokens::{SpecialTokens, default_special_tokens};

pub use inference::{
    CompletionUsage, FinishReason, InferenceChunk, InferenceFinishReason, PromptUsage,
};
pub use logprobs::{InferenceTokenLogprob, TokenLogprob, TopLogprob};

mod inference;
mod logprobs;

/// The stage where parsing begins in thinking mode.
#[derive(Debug, Clone, Copy)]
pub enum ThinkingInitialStage {
    ReasoningStart,
    ReasoningContinuation,
    ContentContinuation,
}

/// Settings for parsing inference output derived during request conversion.
#[derive(Debug, Clone)]
pub struct ParsingOptions {
    pub parse_tool_calls: bool,
    pub starts_in_tool_calls: bool,
    pub parse_json_output: bool,
    pub thinking_initial_stage: Option<ThinkingInitialStage>,
    pub stop_sequences: Vec<String>,
    pub special_tokens: Arc<dyn SpecialTokens>,
}

impl Default for ParsingOptions {
    fn default() -> Self {
        Self {
            parse_tool_calls: true,
            starts_in_tool_calls: false,
            parse_json_output: false,
            thinking_initial_stage: None,
            stop_sequences: Vec::new(),
            special_tokens: default_special_tokens(),
        }
    }
}

/// A parsed piece of model output, before conversion into protocol response
/// chunks.
#[derive(Debug, Clone, PartialEq)]
pub enum OutputChunk {
    /// Output begins.
    Start {
        system_fingerprint: Option<String>,
        prompt_usage: PromptUsage,
    },
    /// Answer content.
    Content {
        content: String,
        logprobs: Option<Arc<[TokenLogprob]>>,
    },
    /// Reasoning content.
    Reasoning {
        content: String,
        logprobs: Option<Arc<[TokenLogprob]>>,
    },
    /// An `invoke` tag opened a tool call; its tool name is not complete yet.
    ToolCallBegin,
    /// The tool name of the open tool call is complete. The arguments follow
    /// in [`ToolCallArgumentsDelta`](Self::ToolCallArgumentsDelta) chunks.
    ToolCallNamed { tool_name: String },
    /// A piece of the JSON arguments of the open tool call.
    ToolCallArgumentsDelta { arguments: String },
    /// Output ends.
    Finish {
        finish_reason: FinishReason,
        /// The matched stop sequence, when one ended output.
        stop_sequence: Option<String>,
        completion_usage: CompletionUsage,
    },
}

/// Convert parsed output chunks into a protocol's response chunks.
pub trait ChunkGenerator: Send + 'static {
    /// The protocol's response chunk, such as an SSE event.
    type Chunk;

    /// Produce zero or more protocol chunks for one parsed output chunk.
    fn generate(&mut self, chunk: OutputChunk) -> impl Future<Output = Vec<Self::Chunk>> + Send;
}
