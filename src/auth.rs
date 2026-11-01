//! Token checks shared by the feed routes and the admin routes
//! (`/status`, `/metrics`).
//!
//! Two rules hold everywhere:
//! - tokens are compared in constant time ([`ct_eq`]), so response timing
//!   reveals nothing about how much of a guess was right;
//! - a token is never logged. Nothing here logs anything.

use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use subtle::ConstantTimeEq;

/// Constant-time equality that also hides which of the two is longer: both
/// are padded to the same length before comparing, and the lengths are
/// compared separately.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let n = a.len().max(b.len());
    let pad = |s: &[u8]| {
        let mut v = vec![0u8; n];
        v[..s.len()].copy_from_slice(s);
        v
    };
    let same_bytes = pad(a).ct_eq(&pad(b));
    let same_len = (a.len() as u64).ct_eq(&(b.len() as u64));
    (same_bytes & same_len).into()
}

/// Does the request carry `Authorization: Bearer <expected>`?
///
/// - `expected == None` means no admin token is configured: always true.
/// - Exactly one `Authorization` header is accepted. Several are refused
///   outright, because proxies and servers disagree about which one counts,
///   and a request that is ambiguous should not be allowed through.
/// - The scheme name `Bearer` is case-insensitive (RFC 7235); the token
///   itself is compared exactly. Anything malformed is simply "not
///   authorized".
pub fn bearer_ok(expected: Option<&str>, headers: &HeaderMap) -> bool {
    let Some(expected) = expected else {
        return true;
    };
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return false; // none, or more than one
    };
    let Ok(text) = value.to_str() else {
        return false; // not plain ASCII
    };
    // "Bearer" + one space + token.
    let Some((scheme, token)) = text.split_once(' ') else {
        return false;
    };
    scheme.eq_ignore_ascii_case("Bearer") && ct_eq(token.as_bytes(), expected.as_bytes())
}

/// The one 401 for admin endpoints: empty body, `WWW-Authenticate: Bearer`.
/// Every refusal (no header, wrong token, malformed, several headers) goes
/// through here, so the responses are byte-identical and say nothing about
/// why the request was refused.
pub fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"))],
    )
        .into_response()
}
