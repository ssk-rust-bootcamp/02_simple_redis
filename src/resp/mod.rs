mod array;
mod bool;
mod bulk_string;
mod doubble;
mod frame;
mod interger;
mod map;
mod null;
mod set;
mod simple_error;
mod simple_string;
use bytes::Buf;
use bytes::BytesMut;
use enum_dispatch::enum_dispatch;

pub use self::{
    array::{RespArray, RespNullArray},
    bulk_string::{BulkString, RespNullBulkString},
    frame::RespFrame,
    map::RespMap,
    null::RespNull,
    set::RespSet,
    simple_error::SimpleError,
    simple_string::SimpleString,
};
use thiserror::Error;
/*
- 如何解析 Frame
    - simple string: "+OK\r\n"
    - error: "-Error message\r\n"
    - bulk error: "!<length>\r\n<error>\r\n"
    - integer: ":[<+|->]<value>\r\n"
    - bulk string: "$<length>\r\n<data>\r\n"
    - null bulk string: "$-1\r\n"
    - array: "*<number-of-elements>\r\n<element-1>...<element-n>"
        - "*2\r\n$3\r\nget\r\n$5\r\nhello\r\n"
    - null array: "*-1\r\n"
    - null: "_\r\n"
    - boolean: "#<t|f>\r\n"
    - double: ",[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n"
    - map: "%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>"
    - set: "~<number-of-elements>\r\n<element-1>...<element-n>"
 */

const CRLF: &[u8] = b"\r\n";
const CRLF_LEN: usize = CRLF.len();
const BUF_CAP: usize = 4096;
#[enum_dispatch]
pub trait RespEncode {
    fn encode(self) -> Vec<u8>;
}

pub trait RespDecode: Sized {
    const PREFIX: &'static str;
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError>;
    fn expect_length(buf: &[u8]) -> Result<usize, RespError>;
}

#[derive(Error, Debug, PartialEq, Eq)]
pub enum RespError {
    #[error("Invalid frame: {0}")]
    InvalidFrame(String),
    #[error("Invalid frame type: {0}")]
    InvalidFrameType(String),
    #[error("Invalid frame length: {0}")]
    InvalidFrameLength(isize),

    #[error("Frame not complete")]
    NotComplete,
    #[error("Parse error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("Utf8 error: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error("Parse float error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),
}

pub fn exttra_simple_frame_data(buf: &[u8], prefix: &str) -> Result<usize, RespError> {
    if buf.len() < 3 {
        return Err(RespError::NotComplete);
    }
    if !buf.starts_with(prefix.as_bytes()) {
        return Err(RespError::InvalidFrameType(format!(
            "expect : SimpleString({}), but got {:?}",
            prefix, buf
        )));
    }
    let end = find_crlf(buf, 1).ok_or(RespError::NotComplete)?;
    Ok(end)
}
pub fn extract_fixed_data(buf: &mut BytesMut, expect: &str, expect_type: &str) -> Result<(), RespError> {
    println!(
        "extract_fixed_data buf: {}, expect: {}, expect_type: {}",
        buf.len(),
        expect.len(),
        expect_type
    );
    if buf.len() < expect.len() {
        return Err(RespError::NotComplete);
    }
    if !buf.starts_with(expect.as_bytes()) {
        return Err(RespError::InvalidFrameType(format!(
            "expect :{},got {:?}",
            expect_type, buf
        )));
    }
    buf.advance(expect.len());
    Ok(())
}

pub fn find_crlf(buf: &[u8], nth: isize) -> Option<usize> {
    let mut count = 0;
    for i in 1..buf.len() - 1 {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            count += 1;
            if count == nth {
                return Some(i);
            }
        }
    }
    None
}
pub fn parse_length(buf: &[u8], prefix: &str) -> Result<(usize, usize), RespError> {
    let end = exttra_simple_frame_data(buf, prefix)?;
    let s = String::from_utf8_lossy(&buf[prefix.len()..end]);
    Ok((end, s.parse()?))
}

pub fn calc_total_length(buf: &[u8], end: usize, len: usize, prefix: &str) -> Result<usize, RespError> {
    let mut total = end + CRLF_LEN;
    let mut data = &buf[total..];
    match prefix {
        "*" | "~" => {
            // find nth CRLF in the buffer ,for array and set ,we need to find 1 CRLF for search element
            for _ in 0..len {
                let len = RespFrame::expect_length(data)?;
                data = &data[len..];
                total += len;
            }
            Ok(total)
        }
        "%" => {
            // find nth CRLF in the buffer. For map, we need to find 2 CRLF for each key-value pair
            for _ in 0..len {
                let len = RespFrame::expect_length(data)?;
                data = &data[len..];
                total += len;

                let len = RespFrame::expect_length(data)?;
                data = &data[len..];
                total += len;
            }
            Ok(total)
        }
        _ => Ok(len + CRLF_LEN),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use anyhow::Result;

    #[test]
    fn test_calc_array_length() -> Result<()> {
        let buf = b"*2\r\n$3\r\nset\r\n$5\r\nhello\r\n";
        let (end, len) = parse_length(buf, "*")?;
        let total_len = calc_total_length(buf, end, len, "*")?;
        assert_eq!(total_len, buf.len());

        let buf = b"*2\r\n$3\r\nset\r\n";
        let (end, len) = parse_length(buf, "*")?;
        let ret = calc_total_length(buf, end, len, "*");
        assert_eq!(ret.unwrap_err(), RespError::NotComplete);

        Ok(())
    }
}
