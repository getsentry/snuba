//! Minimal RowBinary serializer for inserts into ClickHouse.
//!
//! Vendored from the `clickhouse` crate (Apache-2.0 / MIT) and reduced to the
//! shape `rust_snuba` actually emits: fixed-width integers, f32/f64, bool, str,
//! raw byte slices (UUID / FixedString), length-prefixed sequences, tuples and
//! structs. We do this in-process so the typed rows can be serialized to bytes
//! inside the processor and dropped immediately, instead of riding the pipeline
//! all the way to the writer step.

mod ser;

// `Error` is part of the module's public surface (it's the error type of
// `serialize_into`). It isn't referenced by name anywhere in the crate today
// — callers propagate via `?` into `anyhow::Error` — but keep the re-export
// so consumers can match on the variant if they ever need to.
#[allow(unused_imports)]
pub use ser::{serialize_into, Error};

pub mod uuid {
    //! Serde adapter for `#[serde(with = "...")]` on `Uuid` fields. ClickHouse
    //! stores UUID as two little-endian u64 halves, so the canonical 16-byte
    //! UUID has each half reversed before being written.

    use serde::Serializer;
    use uuid::Uuid;

    pub fn serialize<S>(uuid: &Uuid, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut bytes = *uuid.as_bytes();
        bytes[..8].reverse();
        bytes[8..].reverse();
        serializer.serialize_bytes(&bytes)
    }
}
