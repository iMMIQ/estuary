//! API protocol conversion and response event generation.
//!
//! [`request::ProtocolRequest::convert`] produces a shared
//! [`request::ConversationRequest`], and [`stream::ChunkGenerator`] turns
//! backend output chunks into protocol response events.

pub use protocol::anthropic;
pub use protocol::openai;

mod protocol;
pub mod request;
pub mod response;
pub mod stream;
