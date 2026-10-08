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
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "web/dist"]
pub(super) struct AdminAssets;

pub(super) async fn admin_redirect() -> Redirect {
    Redirect::temporary("/admin/")
}

pub(super) async fn admin_index() -> Response {
    embedded_admin_response("index.html", false, false)
}

pub(super) async fn admin_asset(Path(asset): Path<String>, headers: HeaderMap) -> Response {
    if AdminAssets::get(&asset).is_some() {
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
    let Some(asset) = AdminAssets::get(path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let content_type = mime_guess::from_path(path).first_or_octet_stream();
    let compressed = gzip
        .then(|| AdminAssets::get(&format!("{path}.gz")))
        .flatten();
    let is_compressed = compressed.is_some();
    let cache_control = if immutable {
        "public, max-age=31536000, immutable"
    } else {
        "no-store"
    };
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type.as_ref())
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
        .body(axum::body::Body::from(compressed.unwrap_or(asset).data.into_owned()))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    if is_compressed {
        response
            .headers_mut()
            .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }
    response
}
