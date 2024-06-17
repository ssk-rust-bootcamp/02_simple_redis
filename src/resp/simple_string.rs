use std::ops::Deref;

use bytes::BytesMut;

use crate::{exttra_simple_frame_data, RespDecode, RespEncode, RespError};

use super::CRLF_LEN;

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct SimpleString(pub(crate) String);

impl RespDecode for SimpleString {
    const PREFIX: &'static str = "+";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = exttra_simple_frame_data(buf, Self::PREFIX)?;
        //split the buffer
        let data = buf.split_to(end + CRLF_LEN);
        let s = String::from_utf8_lossy(&data[Self::PREFIX.len()..end]);
        Ok(SimpleString::new(s.to_string()))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = exttra_simple_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

// - simple string: "+OK\r\n"
impl RespEncode for SimpleString {
    fn encode(self) -> Vec<u8> {
        format!("+{}\r\n", self.0).as_bytes().to_vec()
    }
}

impl SimpleString {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl From<&str> for SimpleString {
    fn from(s: &str) -> Self {
        SimpleString(s.to_string())
    }
}
impl Deref for SimpleString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::RespFrame;

    use super::*;
    use anyhow::Result;
    use bytes::{BufMut, BytesMut};
    #[test]
    fn test_simple_string_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"+OK\r\n");

        let frame = SimpleString::decode(&mut buf)?;
        assert_eq!(frame, SimpleString::new("OK".to_string()));

        buf.extend_from_slice(b"+hello\r");

        let ret = SimpleString::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespError::NotComplete);

        buf.put_u8(b'\n');
        let frame = SimpleString::decode(&mut buf)?;
        assert_eq!(frame, SimpleString::new("hello".to_string()));
        Ok(())
    }

    #[test]
    fn test_simple_string_encode() {
        let frame: RespFrame = SimpleString::new("OK".to_string()).into();
        assert_eq!(frame.encode(), b"+OK\r\n");
    }
}
