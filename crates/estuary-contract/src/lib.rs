//! Shared configuration schemas, admin DTOs, and TypeScript contract types.
//!
//! Kept in a lightweight leaf crate so generating or verifying
//! `web/src/generated/config.ts` does not require compiling or re-fingerprinting
//! the full gateway binary.

pub mod config;
pub mod node;
pub mod observability;
pub mod session_log;
