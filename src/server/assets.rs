use std::io::Read;

use axum::{
    extract::Path,
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{
            ACCEPT_ENCODING, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_SECURITY_POLICY,
            CONTENT_TYPE, VARY,
        },
    },
    response::{IntoResponse, Redirect, Response},
};
pub(super) use estuary_assets::AdminAssets;
use estuary_assets::content_type;
use flate2::read::GzDecoder;

fn is_gzip_path(path: &str) -> bool {
    std::path::Path::new(path)
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("gz"))
}

fn has_asset(path: &str) -> bool {
    !is_gzip_path(path)
        && (AdminAssets::get(path).is_some() || AdminAssets::get(&format!("{path}.gz")).is_some())
}

pub(super) async fn admin_redirect() -> Redirect {
    Redirect::temporary("/admin/")
}

pub(super) async fn admin_index() -> Response {
    embedded_admin_response("index.html", false, false)
}

pub(super) async fn admin_asset(Path(asset): Path<String>, headers: HeaderMap) -> Response {
    if has_asset(&asset) {
        return embedded_admin_response(
            &asset,
            asset.starts_with("assets/"),
            accepts_gzip(&headers),
        );
    }
    if !asset.rsplit('/').next().unwrap_or_default().contains('.') {
        return embedded_admin_response("index.html", false, false);
    }
    StatusCode::NOT_FOUND.into_response()
}

pub(super) fn accepts_gzip(headers: &HeaderMap) -> bool {
    let mut wildcard = false;
    let mut gzip = None;
    for value in headers.get_all(ACCEPT_ENCODING) {
        let Ok(value) = value.to_str() else { continue };
        for item in value.split(',') {
            let mut parts = item.trim().split(';');
            let encoding = parts.next().unwrap_or_default().trim();
            let quality = parts
                .find_map(|part| part.trim().strip_prefix("q="))
                .map_or(Some(1.0), |value| value.parse::<f32>().ok());
            let allowed = quality.is_some_and(|quality| quality > 0.0 && quality <= 1.0);
            if encoding.eq_ignore_ascii_case("gzip") {
                gzip = Some(allowed);
            } else if encoding == "*" {
                wildcard = allowed;
            }
        }
    }
    gzip.unwrap_or(wildcard)
}

pub(super) fn embedded_admin_response(path: &str, immutable: bool, gzip: bool) -> Response {
    if is_gzip_path(path) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let compressed = AdminAssets::get(&format!("{path}.gz"));
    let (body, is_compressed) = match (gzip, compressed, AdminAssets::get(path)) {
        (true, Some(asset), _) => (asset.into_owned(), true),
        (_, _, Some(asset)) => (asset.into_owned(), false),
        (false, Some(asset), None) => {
            let mut decoder = GzDecoder::new(asset.as_ref());
            let mut plain = Vec::new();
            if decoder.read_to_end(&mut plain).is_err() {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
            (plain, false)
        }
        (_, None, None) => return StatusCode::NOT_FOUND.into_response(),
    };
    let content_type = content_type(path);
    let cache_control = if immutable {
        "public, max-age=31536000, immutable"
    } else {
        "no-store"
    };
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type)
        .header(CACHE_CONTROL, cache_control)
        .header(VARY, "Accept-Encoding")
        .header("x-content-type-options", "nosniff")
        .header("x-frame-options", "DENY")
        .header("referrer-policy", "no-referrer")
        .header("permissions-policy", "camera=(), microphone=(), geolocation=()")
        .header(
            CONTENT_SECURITY_POLICY,
            "default-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'self'; frame-ancestors 'none'",
        )
        .body(axum::body::Body::from(body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    if is_compressed {
        response
            .headers_mut()
            .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }
    response
}
