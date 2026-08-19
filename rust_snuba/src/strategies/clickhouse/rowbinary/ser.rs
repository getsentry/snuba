use std::fmt;

use serde::ser::{
    self, Impossible, Serialize, SerializeMap, SerializeSeq, SerializeStruct, SerializeTuple,
    SerializeTupleStruct,
};

/// Append the RowBinary encoding of `value` to `buf`.
pub fn serialize_into<T>(buf: &mut Vec<u8>, value: &T) -> Result<(), Error>
where
    T: Serialize + ?Sized,
{
    let mut serializer = Serializer { buf };
    value.serialize(&mut serializer)
}

#[derive(Debug)]
pub enum Error {
    Message(String),
    /// Sequences and maps must report a known length so we can emit the
    /// LEB128 length prefix up front.
    SequenceLengthRequired,
    Unsupported(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Message(s) => f.write_str(s),
            Error::SequenceLengthRequired => {
                f.write_str("rowbinary: sequences and maps must have a known length")
            }
            Error::Unsupported(what) => write!(f, "rowbinary: unsupported serde type: {what}"),
        }
    }
}

impl std::error::Error for Error {}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

struct Serializer<'a> {
    buf: &'a mut Vec<u8>,
}

fn write_uvarint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

macro_rules! impl_le {
    ($($name:ident => $ty:ty),+ $(,)?) => {$(
        #[inline]
        fn $name(self, v: $ty) -> Result<(), Error> {
            self.buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
    )+};
}

impl<'a, 'b> ser::Serializer for &'a mut Serializer<'b> {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeTupleVariant = Impossible<(), Error>;
    type SerializeStructVariant = Impossible<(), Error>;

    #[inline]
    fn serialize_bool(self, v: bool) -> Result<(), Error> {
        self.buf.push(v as u8);
        Ok(())
    }

    #[inline]
    fn serialize_u8(self, v: u8) -> Result<(), Error> {
        self.buf.push(v);
        Ok(())
    }

    #[inline]
    fn serialize_i8(self, v: i8) -> Result<(), Error> {
        self.buf.push(v as u8);
        Ok(())
    }

    impl_le! {
        serialize_i16  => i16,
        serialize_i32  => i32,
        serialize_i64  => i64,
        serialize_i128 => i128,
        serialize_u16  => u16,
        serialize_u32  => u32,
        serialize_u64  => u64,
        serialize_u128 => u128,
        serialize_f32  => f32,
        serialize_f64  => f64,
    }

    fn serialize_char(self, v: char) -> Result<(), Error> {
        let mut tmp = [0u8; 4];
        self.serialize_str(v.encode_utf8(&mut tmp))
    }

    fn serialize_str(self, v: &str) -> Result<(), Error> {
        write_uvarint(self.buf, v.len() as u64);
        self.buf.extend_from_slice(v.as_bytes());
        Ok(())
    }

    /// Raw byte write — no length prefix. Matches RowBinary's fixed-width
    /// encoding for UUID and FixedString(N). Variable-length byte sequences
    /// should be modeled as `Vec<u8>` and hit `serialize_seq`.
    fn serialize_bytes(self, v: &[u8]) -> Result<(), Error> {
        self.buf.extend_from_slice(v);
        Ok(())
    }

    fn serialize_none(self) -> Result<(), Error> {
        self.buf.push(1);
        Ok(())
    }

    fn serialize_some<T>(self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        self.buf.push(0);
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), Error> {
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), Error> {
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
    ) -> Result<(), Error> {
        Err(Error::Unsupported("enum variant"))
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        Err(Error::Unsupported("enum variant"))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        let len = len.ok_or(Error::SequenceLengthRequired)?;
        write_uvarint(self.buf, len as u64);
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Error> {
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Error> {
        Ok(self)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        Err(Error::Unsupported("enum variant"))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Error> {
        let len = len.ok_or(Error::SequenceLengthRequired)?;
        write_uvarint(self.buf, len as u64);
        Ok(self)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Error> {
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        Err(Error::Unsupported("enum variant"))
    }
}

impl<'a, 'b> SerializeSeq for &'a mut Serializer<'b> {
    type Ok = ();
    type Error = Error;
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a, 'b> SerializeTuple for &'a mut Serializer<'b> {
    type Ok = ();
    type Error = Error;
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a, 'b> SerializeTupleStruct for &'a mut Serializer<'b> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a, 'b> SerializeMap for &'a mut Serializer<'b> {
    type Ok = ();
    type Error = Error;
    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        key.serialize(&mut **self)
    }
    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a, 'b> SerializeStruct for &'a mut Serializer<'b> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    fn encode<T: Serialize>(v: &T) -> Vec<u8> {
        let mut buf = Vec::new();
        serialize_into(&mut buf, v).unwrap();
        buf
    }

    #[test]
    fn primitives_are_little_endian() {
        assert_eq!(encode(&0x01u8), vec![0x01]);
        assert_eq!(encode(&0x0102u16), vec![0x02, 0x01]);
        assert_eq!(encode(&0x01020304u32), vec![0x04, 0x03, 0x02, 0x01]);
        assert_eq!(
            encode(&0x0102030405060708u64),
            vec![0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01],
        );
        assert_eq!(encode(&true), vec![1]);
        assert_eq!(encode(&false), vec![0]);
    }

    #[test]
    fn strings_get_uvarint_length_prefix() {
        assert_eq!(encode(&""), vec![0]);
        assert_eq!(encode(&"abc"), vec![3, b'a', b'b', b'c']);
        // 130-char string crosses the single-byte LEB128 boundary (0x80).
        let s = "x".repeat(130);
        let out = encode(&s);
        assert_eq!(&out[..2], &[0x82, 0x01]); // 130 = 0b1_0000010 → 0x82 0x01
        assert_eq!(out.len(), 132);
    }

    #[test]
    fn bytes_are_raw_no_prefix() {
        // serialize_bytes is the FixedString path — no length prefix.
        // (We use this for UUIDs.)
        struct Raw(Vec<u8>);
        impl Serialize for Raw {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_bytes(&self.0)
            }
        }
        let out = encode(&Raw(vec![0xaa, 0xbb, 0xcc]));
        assert_eq!(out, vec![0xaa, 0xbb, 0xcc]);
    }

    #[test]
    fn sequences_prefix_then_elements() {
        let v: Vec<u16> = vec![1, 2, 3];
        // 3 elements, then each u16 LE.
        assert_eq!(encode(&v), vec![3, 1, 0, 2, 0, 3, 0]);
    }

    #[test]
    fn map_like_vec_of_pairs_matches_clickhouse_map_shape() {
        // Map(String, UInt8) is wire-equivalent to Array(Tuple(String, UInt8)):
        // uvarint length, then concatenated (string-prefix, key, u8) tuples.
        let v: Vec<(String, u8)> = vec![("a".into(), 7), ("bb".into(), 9)];
        assert_eq!(
            encode(&v),
            vec![
                2, // len
                1, b'a', 7, // ("a", 7)
                2, b'b', b'b', 9, // ("bb", 9)
            ],
        );
    }

    #[test]
    fn uuid_adapter_emits_16_bytes_with_halves_reversed() {
        use uuid::Uuid;
        // 11223344-5566-7788-99aa-bbccddeeff00
        let id = Uuid::from_bytes([
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
            0xff, 0x00,
        ]);
        #[derive(Serialize)]
        struct W {
            #[serde(with = "super::super::uuid")]
            id: Uuid,
        }
        let out = encode(&W { id });
        assert_eq!(
            out,
            vec![
                0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, // first half reversed
                0x00, 0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, // second half reversed
            ],
        );
    }

    #[test]
    fn struct_fields_serialize_in_declaration_order() {
        #[derive(Serialize)]
        struct S {
            a: u8,
            b: u16,
            c: bool,
        }
        let out = encode(&S {
            a: 1,
            b: 2,
            c: true,
        });
        assert_eq!(out, vec![1, 2, 0, 1]);
    }
}
