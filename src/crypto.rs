//! Optional authenticated field-level encryption for upstream node credentials.
//!
//! When `ESTUARY_CONFIG_ENCRYPTION_KEY` is set, node `api_key` and custom header
//! values are encrypted before being persisted to SQLite using BLAKE3 XOF stream
//! encryption with Encrypt-then-MAC authentication (`enc:v1:<base64>`). Plaintext
//! legacy records remain readable for seamless migration.

use std::{
    env,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use subtle::ConstantTimeEq;

use crate::config::NodeConfig;

pub const ENCRYPTION_KEY_ENV: &str = "ESTUARY_CONFIG_ENCRYPTION_KEY";
const PREFIX: &str = "enc:v1:";
const NONCE_LEN: usize = 16;
const TAG_LEN: usize = 32;
const KEY_CONTEXT: &str = "estuary config encryption v1";

static NONCE_COUNTER: AtomicU64 = AtomicU64::new(1);

fn derive_master_key(passphrase: &str) -> [u8; 32] {
    blake3::derive_key(KEY_CONTEXT, passphrase.as_bytes())
}

fn configured_key() -> Option<[u8; 32]> {
    let raw = env::var(ENCRYPTION_KEY_ENV).ok()?;
    if raw.is_empty() {
        return None;
    }
    Some(derive_master_key(&raw))
}

fn generate_nonce(key: &[u8; 32]) -> [u8; NONCE_LEN] {
    let counter = NONCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let uuid_bytes = uuid::Uuid::now_v7().into_bytes();
    let mut hasher = blake3::Hasher::new_keyed(key);
    hasher.update(b"nonce");
    hasher.update(&uuid_bytes);
    hasher.update(&counter.to_le_bytes());
    let hash = hasher.finalize();
    let mut nonce = [0u8; NONCE_LEN];
    nonce.copy_from_slice(&hash.as_bytes()[..NONCE_LEN]);
    nonce
}

fn xor_keystream(key: &[u8; 32], nonce: &[u8; NONCE_LEN], data: &mut [u8]) {
    let mut hasher = blake3::Hasher::new_keyed(key);
    hasher.update(b"stream");
    hasher.update(nonce);
    let mut xof = hasher.finalize_xof();
    let mut mask = vec![0u8; data.len()];
    xof.fill(&mut mask);
    for (byte, mask_byte) in data.iter_mut().zip(mask) {
        *byte ^= mask_byte;
    }
}

fn compute_tag(key: &[u8; 32], nonce: &[u8; NONCE_LEN], aad: &str, ciphertext: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_keyed(key);
    hasher.update(b"mac");
    hasher.update(nonce);
    hasher.update(&(aad.len() as u64).to_le_bytes());
    hasher.update(aad.as_bytes());
    hasher.update(&(ciphertext.len() as u64).to_le_bytes());
    hasher.update(ciphertext);
    *hasher.finalize().as_bytes()
}

pub fn encrypt_with_key(key: &[u8; 32], aad: &str, plaintext: &str) -> String {
    if plaintext.starts_with(PREFIX) {
        return plaintext.to_owned();
    }
    let nonce = generate_nonce(key);
    let mut ciphertext = plaintext.as_bytes().to_vec();
    xor_keystream(key, &nonce, &mut ciphertext);
    let tag = compute_tag(key, &nonce, aad, &ciphertext);

    let mut blob = Vec::with_capacity(NONCE_LEN + TAG_LEN + ciphertext.len());
    blob.extend_from_slice(&nonce);
    blob.extend_from_slice(&tag);
    blob.extend_from_slice(&ciphertext);
    format!("{PREFIX}{}", BASE64_STANDARD.encode(blob))
}

pub fn decrypt_with_key(key: &[u8; 32], aad: &str, encoded: &str) -> Result<String> {
    let Some(payload) = encoded.strip_prefix(PREFIX) else {
        return Ok(encoded.to_owned());
    };
    let blob = BASE64_STANDARD
        .decode(payload)
        .context("invalid base64 in encrypted secret")?;
    if blob.len() < NONCE_LEN + TAG_LEN {
        bail!("encrypted secret is truncated");
    }
    let mut nonce = [0u8; NONCE_LEN];
    nonce.copy_from_slice(&blob[..NONCE_LEN]);
    let tag = &blob[NONCE_LEN..NONCE_LEN + TAG_LEN];
    let mut plaintext = blob[NONCE_LEN + TAG_LEN..].to_vec();

    let expected_tag = compute_tag(key, &nonce, aad, &plaintext);
    if !bool::from(tag.ct_eq(&expected_tag)) {
        bail!("encrypted secret failed authentication check");
    }
    xor_keystream(key, &nonce, &mut plaintext);
    String::from_utf8(plaintext).context("decrypted secret is not valid UTF-8")
}

/// Encrypt sensitive node credentials if `ESTUARY_CONFIG_ENCRYPTION_KEY` is configured.
pub fn encrypt_node_secrets(config: &mut NodeConfig) {
    let Some(key) = configured_key() else {
        return;
    };
    if let Some(api_key) = &mut config.api_key {
        let aad = format!("node:{}:api_key", config.id);
        *api_key = encrypt_with_key(&key, &aad, api_key);
    }
    for (name, value) in &mut config.headers {
        let aad = format!("node:{}:header:{name}", config.id);
        *value = encrypt_with_key(&key, &aad, value);
    }
}

/// Decrypt sensitive node credentials when prefixed with `enc:v1:`.
pub fn decrypt_node_secrets(config: &mut NodeConfig) -> Result<()> {
    let needs_key = config
        .api_key
        .as_deref()
        .is_some_and(|v| v.starts_with(PREFIX))
        || config.headers.values().any(|v| v.starts_with(PREFIX));
    if !needs_key {
        return Ok(());
    }
    let Some(key) = configured_key() else {
        bail!(
            "node {:?} contains encrypted credentials but {ENCRYPTION_KEY_ENV} is not set",
            config.id
        );
    };
    if let Some(api_key) = &mut config.api_key {
        let aad = format!("node:{}:api_key", config.id);
        *api_key = decrypt_with_key(&key, &aad, api_key)?;
    }
    for (name, value) in &mut config.headers {
        let aad = format!("node:{}:header:{name}", config.id);
        *value = decrypt_with_key(&key, &aad, value)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_tamper_detection() {
        let key = derive_master_key("test-master-secret");
        let aad = "node:n1:api_key";
        let secret = "sk-upstream-super-secret-123";
        let encrypted = encrypt_with_key(&key, aad, secret);
        assert!(encrypted.starts_with(PREFIX));
        assert_ne!(encrypted, secret);

        let decrypted = decrypt_with_key(&key, aad, &encrypted).unwrap();
        assert_eq!(decrypted, secret);

        // Wrong AAD must fail authentication.
        assert!(decrypt_with_key(&key, "node:n2:api_key", &encrypted).is_err());

        // Wrong key must fail authentication.
        let other = derive_master_key("other-secret");
        assert!(decrypt_with_key(&other, aad, &encrypted).is_err());

        // Plaintext legacy values pass through unchanged.
        assert_eq!(decrypt_with_key(&key, aad, "sk-plain").unwrap(), "sk-plain");
    }
}
