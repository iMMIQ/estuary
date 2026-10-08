mod anthropic;
mod anthropic_responses;
mod codex;
pub mod config;
mod deepseek;
pub mod error;
pub mod health;
mod inference_stats;
pub mod kv_cache;
pub mod lifecycle;
pub mod metrics;
pub mod node;
pub mod prefix;
pub mod proxy;
pub mod response_buffer;
pub mod scheduler;
pub mod server;
mod sse;
pub mod store;
pub mod supervisor;
pub mod vllm;

pub use config::Settings;
pub use server::Gateway;

/// The release version, including the commit hash for untagged builds.
pub const VERSION: &str = env!("ESTUARY_BUILD_VERSION");
