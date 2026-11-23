//! Embedded admin web assets isolated in a dependency-free leaf crate.
//!
//! In release builds (`cfg(not(debug_assertions))`), `web/dist` is embedded at
//! compile time via `include_bytes!`. In debug builds, assets are served
//! directly from `web/dist` on disk so frontend updates do not invalidate Rust
//! incremental builds.

use std::borrow::Cow;
#[cfg(debug_assertions)]
use std::{
    fs,
    path::{Component, Path, PathBuf},
};

#[cfg(not(debug_assertions))]
include!(concat!(env!("OUT_DIR"), "/embedded_assets.rs"));

/// Static assets built from `web/dist`.
pub struct AdminAssets;

impl AdminAssets {
    /// Returns the raw bytes for a relative asset path, or `None` if absent or unsafe.
    pub fn get(path: &str) -> Option<Cow<'static, [u8]>> {
        #[cfg(not(debug_assertions))]
        {
            FILES
                .binary_search_by_key(&path, |(key, _)| *key)
                .ok()
                .map(|index| Cow::Borrowed(FILES[index].1))
        }
        #[cfg(debug_assertions)]
        {
            let target = resolve_debug_path(path)?;
            fs::read(target).ok().map(Cow::Owned)
        }
    }

    /// Lists all available relative asset paths in sorted order.
    pub fn iter() -> impl Iterator<Item = String> {
        #[cfg(not(debug_assertions))]
        {
            FILES.iter().map(|(key, _)| (*key).to_owned())
        }
        #[cfg(debug_assertions)]
        {
            let root = debug_dist_dir();
            let mut out = Vec::new();
            if root.is_dir() {
                collect_debug_paths(&root, &root, &mut out);
                out.sort();
            }
            out.into_iter()
        }
    }
}

/// Returns the HTTP `Content-Type` header value for a static web asset path.
pub fn content_type(path: &str) -> &'static str {
    let ext = path
        .rsplit('/')
        .next()
        .and_then(|name| name.rsplit_once('.'))
        .map(|(_, ext)| ext);
    match ext {
        Some(e) if e.eq_ignore_ascii_case("html") || e.eq_ignore_ascii_case("htm") => {
            "text/html; charset=utf-8"
        }
        Some(e) if e.eq_ignore_ascii_case("js") || e.eq_ignore_ascii_case("mjs") => {
            "text/javascript; charset=utf-8"
        }
        Some(e) if e.eq_ignore_ascii_case("css") => "text/css; charset=utf-8",
        Some(e) if e.eq_ignore_ascii_case("json") || e.eq_ignore_ascii_case("map") => {
            "application/json"
        }
        Some(e) if e.eq_ignore_ascii_case("svg") => "image/svg+xml",
        Some(e) if e.eq_ignore_ascii_case("png") => "image/png",
        Some(e) if e.eq_ignore_ascii_case("jpg") || e.eq_ignore_ascii_case("jpeg") => "image/jpeg",
        Some(e) if e.eq_ignore_ascii_case("webp") => "image/webp",
        Some(e) if e.eq_ignore_ascii_case("ico") => "image/x-icon",
        Some(e) if e.eq_ignore_ascii_case("woff2") => "font/woff2",
        Some(e) if e.eq_ignore_ascii_case("woff") => "font/woff",
        Some(e) if e.eq_ignore_ascii_case("ttf") => "font/ttf",
        Some(e) if e.eq_ignore_ascii_case("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

#[cfg(debug_assertions)]
fn debug_dist_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../web/dist")
}

#[cfg(debug_assertions)]
fn resolve_debug_path(rel: &str) -> Option<PathBuf> {
    if rel.is_empty() || rel.contains('\\') || rel.contains('\0') {
        return None;
    }
    let mut joined = debug_dist_dir();
    for component in Path::new(rel).components() {
        match component {
            Component::Normal(segment) => joined.push(segment),
            _ => return None,
        }
    }
    joined.is_file().then_some(joined)
}

#[cfg(debug_assertions)]
fn collect_debug_paths(root: &Path, dir: &Path, out: &mut Vec<String>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_debug_paths(root, &path, out);
        } else if path.is_file()
            && let Ok(rel) = path.strip_prefix(root)
        {
            let key = rel
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            out.push(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_type_maps_known_extensions_and_falls_back() {
        assert_eq!(content_type("index.html"), "text/html; charset=utf-8");
        assert_eq!(
            content_type("assets/app-123.js"),
            "text/javascript; charset=utf-8"
        );
        assert_eq!(content_type("assets/style.CSS"), "text/css; charset=utf-8");
        assert_eq!(content_type("logo.svg"), "image/svg+xml");
        assert_eq!(content_type("unknown.bin"), "application/octet-stream");
    }

    #[test]
    fn rejects_unsafe_asset_paths() {
        assert!(AdminAssets::get("").is_none());
        assert!(AdminAssets::get("../Cargo.toml").is_none());
        assert!(AdminAssets::get("/etc/passwd").is_none());
        assert!(AdminAssets::get("assets\\index.js").is_none());
    }
}
