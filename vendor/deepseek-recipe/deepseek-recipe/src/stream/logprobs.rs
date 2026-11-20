//! Log probabilities of output tokens.

/// Backend log probability data for one generated token.
#[derive(Debug, Clone, PartialEq)]
pub struct InferenceTokenLogprob {
    /// Log probability of the generated token.
    pub logprob: f32,
    /// The most likely alternatives as token IDs and log probabilities.
    pub top_logprobs: Vec<(u32, f32)>,
}

/// The log probability of one output token, as protocols report it.
#[derive(Debug, Clone, PartialEq)]
pub struct TokenLogprob {
    /// The token text. A token whose bytes are not valid UTF-8 is written as
    /// `\xNN` escapes of its bytes.
    pub token: String,
    pub logprob: f32,
    /// The bytes the token stands for. They might not be valid UTF-8; see
    /// [`token`](Self::token).
    pub bytes: Vec<u8>,
    pub top_logprobs: Vec<TopLogprob>,
}

/// One of the most likely alternatives at an output token position.
#[derive(Debug, Clone, PartialEq)]
pub struct TopLogprob {
    pub token: String,
    pub logprob: f32,
    pub bytes: Vec<u8>,
}
