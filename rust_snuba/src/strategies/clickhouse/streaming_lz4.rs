//! Incremental ClickHouse-native LZ4 compressor.
//!
//! Sister to `writer_v2::lz4_compress`. Both emit the same wire format
//! (concatenated CityHash128 + 9-byte header + LZ4 blocks chunked at
//! `LZ4_BLOCK_SIZE`), but this one accepts bytes incrementally and emits
//! a `Bytes` for each complete block as soon as it fills. Used by the
//! streaming writer so we never buffer a full uncompressed batch.
//!
//! Scaffolded ahead of `StreamingClickhouseWriter` — items below are
//! exercised by unit tests now and will be picked up by the strategy in
//! a follow-up commit.
#![allow(dead_code)]

use bytes::Bytes;

use super::writer_v2::{ch_compression_checksum, LZ4_BLOCK_SIZE, LZ4_METHOD_BYTE};

pub struct StreamingLz4Compressor {
    pending: Vec<u8>,
}

impl StreamingLz4Compressor {
    pub fn new() -> Self {
        Self {
            pending: Vec::with_capacity(LZ4_BLOCK_SIZE),
        }
    }

    /// Append `data` to the internal buffer. Returns one `Bytes` per
    /// complete block that filled. Each returned chunk is a self-contained
    /// ClickHouse native block ready to push onto an HTTP body stream.
    pub fn push(&mut self, data: &[u8]) -> Vec<Bytes> {
        self.pending.extend_from_slice(data);
        let mut out = Vec::new();
        while self.pending.len() >= LZ4_BLOCK_SIZE {
            let block: Vec<u8> = self.pending.drain(..LZ4_BLOCK_SIZE).collect();
            out.push(Bytes::from(encode_block(&block)));
        }
        out
    }

    /// Emit any buffered remainder as a final (partial) block. Returns
    /// `None` if the compressor saw no data — important because ClickHouse
    /// rejects an empty native body, so callers must not append `None`
    /// to the wire.
    pub fn finish(mut self) -> Option<Bytes> {
        if self.pending.is_empty() {
            None
        } else {
            let last = std::mem::take(&mut self.pending);
            Some(Bytes::from(encode_block(&last)))
        }
    }
}

impl Default for StreamingLz4Compressor {
    fn default() -> Self {
        Self::new()
    }
}

/// Encode `chunk` as a single ClickHouse native compressed block:
/// `[16 B CityHash128 || 0x82 || u32 LE compressed-with-header || u32 LE
/// uncompressed || LZ4 raw bytes]`. The checksum spans the 9-byte header
/// and the compressed payload.
fn encode_block(chunk: &[u8]) -> Vec<u8> {
    let compressed = lz4_flex::block::compress(chunk);
    let compressed_with_header = 9u32 + compressed.len() as u32;
    let uncompressed_size = chunk.len() as u32;

    let mut out = Vec::with_capacity(25 + compressed.len());
    out.extend_from_slice(&[0u8; 16]);
    out.push(LZ4_METHOD_BYTE);
    out.extend_from_slice(&compressed_with_header.to_le_bytes());
    out.extend_from_slice(&uncompressed_size.to_le_bytes());
    out.extend_from_slice(&compressed);

    let checksum = ch_compression_checksum(&out[16..]);
    out[..16].copy_from_slice(&checksum);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Walks a concatenation of ClickHouse-native compressed blocks and
    /// returns the decoded payload. Mirrors the decoder in `writer_v2`'s
    /// tests so the streaming output is verified against the same wire
    /// expectations the buffered path is.
    fn decode_native_blocks(buf: &[u8]) -> Vec<u8> {
        let mut decoded = Vec::new();
        let mut pos = 0;
        while pos < buf.len() {
            assert!(buf.len() - pos >= 25, "truncated block header");
            let stored_checksum: [u8; 16] = buf[pos..pos + 16].try_into().unwrap();
            assert_eq!(buf[pos + 16], LZ4_METHOD_BYTE, "wrong compression method");
            let compressed_with_header =
                u32::from_le_bytes(buf[pos + 17..pos + 21].try_into().unwrap()) as usize;
            let uncompressed_size =
                u32::from_le_bytes(buf[pos + 21..pos + 25].try_into().unwrap()) as usize;
            let block_end = pos + 16 + compressed_with_header;
            assert!(block_end <= buf.len(), "block size overruns buffer");

            let computed = ch_compression_checksum(&buf[pos + 16..block_end]);
            assert_eq!(computed, stored_checksum, "checksum mismatch");

            let chunk = lz4_flex::block::decompress(&buf[pos + 25..block_end], uncompressed_size)
                .expect("decompress");
            assert_eq!(chunk.len(), uncompressed_size);
            decoded.extend_from_slice(&chunk);
            pos = block_end;
        }
        decoded
    }

    fn drain_to_bytes(chunks: Vec<Bytes>) -> Vec<u8> {
        let mut out = Vec::new();
        for c in chunks {
            out.extend_from_slice(&c);
        }
        out
    }

    #[test]
    fn push_under_block_size_buffers_until_finish() {
        let mut c = StreamingLz4Compressor::new();
        assert!(c.push(b"hello, world").is_empty());
        let last = c.finish().expect("partial final block");
        assert_eq!(decode_native_blocks(&last), b"hello, world");
    }

    #[test]
    fn empty_compressor_finishes_to_none() {
        assert!(StreamingLz4Compressor::new().finish().is_none());
    }

    #[test]
    fn exactly_one_full_block_emits_immediately_no_remainder() {
        let input = vec![0xABu8; LZ4_BLOCK_SIZE];
        let mut c = StreamingLz4Compressor::new();
        let emitted = c.push(&input);
        assert_eq!(emitted.len(), 1);
        assert!(c.finish().is_none(), "should not have a partial tail");
        assert_eq!(decode_native_blocks(&drain_to_bytes(emitted)), input);
    }

    #[test]
    fn cross_boundary_push_splits_into_full_block_plus_remainder() {
        let input: Vec<u8> = (0..(LZ4_BLOCK_SIZE + LZ4_BLOCK_SIZE / 2))
            .map(|i| (i % 251) as u8)
            .collect();
        let mut c = StreamingLz4Compressor::new();
        let mut combined = drain_to_bytes(c.push(&input));
        combined.extend_from_slice(&c.finish().expect("half-block remainder"));
        assert_eq!(decode_native_blocks(&combined), input);
    }

    #[test]
    fn many_small_pushes_recombine_at_block_boundary() {
        // Stream 2.25 blocks worth of bytes one byte at a time. Two full
        // blocks should emit during pushes (after byte LZ4_BLOCK_SIZE and
        // byte 2*LZ4_BLOCK_SIZE), the rest comes out on finish.
        let total = LZ4_BLOCK_SIZE * 2 + LZ4_BLOCK_SIZE / 4;
        let mut c = StreamingLz4Compressor::new();
        let mut expected = Vec::with_capacity(total);
        let mut wire = Vec::new();
        let mut full_blocks_seen = 0;
        for i in 0..total {
            let byte = (i % 251) as u8;
            expected.push(byte);
            let chunks = c.push(&[byte]);
            full_blocks_seen += chunks.len();
            wire.extend_from_slice(&drain_to_bytes(chunks));
        }
        assert_eq!(
            full_blocks_seen, 2,
            "should emit at each full-block boundary"
        );
        wire.extend_from_slice(&c.finish().expect("trailing partial block"));
        assert_eq!(decode_native_blocks(&wire), expected);
    }

    #[test]
    fn single_push_of_multiple_blocks_emits_all_at_once() {
        // 3 full blocks in one push call — all three should emit; no remainder.
        let input: Vec<u8> = (0..(LZ4_BLOCK_SIZE * 3)).map(|i| (i % 251) as u8).collect();
        let mut c = StreamingLz4Compressor::new();
        let emitted = c.push(&input);
        assert_eq!(emitted.len(), 3);
        assert!(c.finish().is_none());
        assert_eq!(decode_native_blocks(&drain_to_bytes(emitted)), input);
    }
}
