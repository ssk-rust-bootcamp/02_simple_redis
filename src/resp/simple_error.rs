use std::ops::Deref;

use bytes::BytesMut;

use crate::{exttra_simple_frame_data, RespDecode, RespEncode, RespError};

use super::CRLF_LEN;

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct SimpleError(pub(crate) String);

impl RespDecode for SimpleError {
    const PREFIX: &'static str = "-";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let end = exttra_simple_frame_data(buf, Self::PREFIX)?;
        // split the buffer
        let data = buf.split_to(end + CRLF_LEN);
        let s = String::from_utf8_lossy(&data[Self::PREFIX.len()..end]);
        Ok(SimpleError::new(s.to_string()))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let end = exttra_simple_frame_data(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN)
    }
}

// - error: "-Error message\r\n"
impl RespEncode for SimpleError {
    fn encode(self) -> Vec<u8> {
        format!("-{}\r\n", self.0).as_bytes().to_vec()
    }
}

impl From<&str> for SimpleError {
    fn from(value: &str) -> Self {
        SimpleError(value.to_string())
    }
}

impl SimpleError {
    pub fn new(s: impl Into<String>) -> Self {
        SimpleError(s.into())
    }
}

impl Deref for SimpleError {
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
    use bytes::BytesMut;

    #[test]
    fn test_error_encode() {
        let frame: RespFrame = SimpleError::new("Error message".to_string()).into();
        assert_eq!(frame.encode(), b"-Error message\r\n")
    }

    #[test]
    fn test_simple_error_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"-ERR invalid password\r\n");

        let frame = SimpleError::decode(&mut buf)?;
        assert_eq!(frame, SimpleError::new("ERR invalid password".to_string()));
        Ok(())
    }
}
