//! Raw response cache: gzip helpers used by both store backends, which keep
//! bodies compressed (callers always see the original bytes).
//!
//! Only bodies that came back as HTTP 200 *and* parsed cleanly are ever
//! written (see `FetchLayer`), so a failed fetch can never replace a good
//! cached copy.

use std::io::{Read, Write};

use anyhow::{Context, Result};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;

/// Gzip-compress a body for storage.
pub(crate) fn gzip(body: &[u8]) -> Result<Vec<u8>> {
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(body)?;
    Ok(enc.finish()?)
}

/// Undo [`gzip`].
pub(crate) fn gunzip(data: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    GzDecoder::new(data)
        .read_to_end(&mut out)
        .context("stored raw response is not valid gzip")?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gzip_roundtrip_and_magic() {
        let body = b"{\"channels\":[]}".repeat(100);
        let z = gzip(&body).unwrap();
        assert_eq!(&z[..2], &[0x1f, 0x8b], "gzip magic bytes");
        assert!(z.len() < body.len());
        assert_eq!(gunzip(&z).unwrap(), body);
        assert!(gunzip(b"not gzip").is_err());
    }
}
