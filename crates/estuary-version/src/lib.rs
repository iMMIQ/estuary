//! Build-version metadata isolated in a leaf crate so Git commits do not
//! invalidate compilation of the main `estuary` library.

/// The release version, including the commit hash for untagged builds.
///
/// Exposed as a `static` rather than a `const` so updating `ESTUARY_BUILD_VERSION`
/// does not alter this crate's `.rmeta` ABI hash and therefore avoids recompiling
/// downstream crates (only relinking the final binary).
pub static VERSION: &str = env!("ESTUARY_BUILD_VERSION");

/// The HTTP `User-Agent` header value (`estuary/<version>`).
pub static USER_AGENT: &str = concat!("estuary/", env!("ESTUARY_BUILD_VERSION"));
