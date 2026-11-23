mod anthropic;
mod chat_bridge;
pub mod config;
mod connection_drain;
mod crypto;
pub mod error;
mod health;
mod inference_stats;
pub mod kv_cache;
pub mod lifecycle;
pub mod metrics;
pub mod node;
pub mod observability;
pub mod prefix;
mod proxy;
pub mod response_buffer;
pub mod scheduler;
mod server;
pub mod session_log;
mod sse;
mod store;
mod time;
pub mod vllm;

pub use config::Settings;
pub use estuary_version::VERSION;
pub use server::Gateway;

/// Ensures the `ring` crypto provider is installed for `rustls` before
/// constructing any `reqwest::Client`. Safe to call repeatedly.
pub fn ensure_crypto_provider() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}
