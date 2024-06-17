use std::ops::Deref;

use bytes::{Buf, BytesMut};

use crate::{extract_fixed_data, parse_length, RespDecode, RespEncode, RespError};

use super::CRLF_LEN;

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct BulkString(pub(crate) Vec<u8>);

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct RespNullBulkString;

impl RespDecode for RespNullBulkString {
    const PREFIX: &'static str = "$";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, "$-1\r\n", "NullBulkString")?;
        Ok(RespNullBulkString)
    }

    fn expect_length(_buf: &[u8]) -> Result<usize, RespError> {
        Ok(5)
    }
}

impl RespDecode for BulkString {
    const PREFIX: &'static str = "$";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        let remained = &buf[end + CRLF_LEN..];
        if remained.len() < len + CRLF_LEN {
            return Err(RespError::NotComplete);
        }
        buf.advance(end + CRLF_LEN);
        let data = buf.split_to(len + CRLF_LEN);
        Ok(BulkString::new(data[..len].to_vec()))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN + len + CRLF_LEN)
    }
}
impl Deref for BulkString {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
// - bulk string: "$<length>\r\n<data>\r\n"
impl RespEncode for BulkString {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.0.len() + 16);
        buf.extend_from_slice(format!("${}\r\n", self.len()).as_bytes());
        buf.extend_from_slice(&self);
        buf.extend_from_slice(b"\r\n");
        buf
    }
}
// - null bulk string: "$-1\r\n"
impl RespEncode for RespNullBulkString {
    fn encode(self) -> Vec<u8> {
        b"$-1\r\n".to_vec()
    }
}

impl BulkString {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        BulkString(s.into())
    }
}

impl From<&str> for BulkString {
    fn from(value: &str) -> Self {
        BulkString(value.as_bytes().to_vec())
    }
}

impl From<&[u8]> for BulkString {
    fn from(s: &[u8]) -> Self {
        BulkString(s.to_vec())
    }
}
impl<const N: usize> From<&[u8; N]> for BulkString {
    fn from(s: &[u8; N]) -> Self {
        BulkString(s.to_vec())
    }
}
impl AsRef<[u8]> for BulkString {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
#[cfg(test)]
mod tests {
    use crate::{RespEncode, RespFrame};

    use super::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"$6\r\nfoobar\r\n");
        let frame = BulkString::decode(&mut buf)?;
        assert_eq!(frame, BulkString::new(b"foobar".to_vec()));

        buf.extend_from_slice(b"$5\r\nhello");

        let ret = BulkString::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespError::NotComplete);

        buf.extend_from_slice(b"\r\n");
        let frame = BulkString::decode(&mut buf)?;
        assert_eq!(frame, BulkString::new(b"hello".to_vec()));
        Ok(())
    }
    #[test]
    fn test_null_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"$-1\r\n");
        let frame = RespNullBulkString::decode(&mut buf)?;
        assert_eq!(frame, RespNullBulkString);
        Ok(())
    }
    #[test]
    fn test_bulk_string_encode() {
        let frame: RespFrame = BulkString::new("hello".as_bytes().to_vec()).into();
        assert_eq!(frame.encode(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_null_bulk_string_encode() {
        let frame: RespFrame = RespNullBulkString.into();
        assert_eq!(frame.encode(), b"$-1\r\n");
    }
}
