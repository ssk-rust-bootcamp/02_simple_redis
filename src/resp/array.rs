use std::ops::Deref;

use bytes::{Buf, BytesMut};

use crate::{
    calc_total_length, extract_fixed_data, parse_length, resp::CRLF_LEN, RespDecode, RespEncode, RespError, RespFrame,
};

use super::BUF_CAP;

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct RespArray(pub(crate) Vec<RespFrame>);
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct RespNullArray;

// - array: "*<number-of-elements>\r\n<element-1>...<element-n>"
// - "*2\r\n$3\r\nget\r\n$5\r\nhello\r\n"
// FIXME: need to handle incomplete
impl RespDecode for RespArray {
    const PREFIX: &'static str = "*";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        println!("decode buf: {}", buf.len());
        let (end, len) = parse_length(buf, Self::PREFIX)?;

        println!("decode end :{} ,array len: {}", end, len);
        let total_len = calc_total_length(buf, end, len, Self::PREFIX)?;
        println!("total_len: {}", total_len);
        if buf.len() < total_len {
            return Err(RespError::NotComplete);
        }
        buf.advance(end + CRLF_LEN);
        println!("advance end :{} ", end + CRLF_LEN);
        println!("buf : {:?}", buf);

        let mut frames = Vec::with_capacity(len);
        for _ in 0..len {
            println!("decode frame buf {:?}", buf);
            let frame = RespFrame::decode(buf)?;
            println!("frame: {:?}, buf: {:?}", frame, buf);
            frames.push(frame);
        }
        Ok(RespArray::new(frames))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        calc_total_length(buf, end, len, Self::PREFIX)
    }
}
impl RespDecode for RespNullArray {
    const PREFIX: &'static str = "*";

    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, "*-1\r\n", "NullArray")?;
        Ok(RespNullArray)
    }

    fn expect_length(_buf: &[u8]) -> Result<usize, RespError> {
        Ok(4)
    }
}
// - array: "*<number-of-elements>\r\n<element-1>...<element-n>"

impl RespEncode for RespArray {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUF_CAP);
        buf.extend_from_slice(format!("*{}\r\n", self.0.len()).as_bytes());
        for item in self.0 {
            buf.extend_from_slice(item.encode().as_slice());
        }
        buf
    }
}
// - null array: "*-1\r\n"
impl RespEncode for RespNullArray {
    fn encode(self) -> Vec<u8> {
        b"*-1\r\n".to_vec()
    }
}
impl Deref for RespArray {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl RespArray {
    pub fn new(s: impl Into<Vec<RespFrame>>) -> Self {
        RespArray(s.into())
    }
}
impl From<Vec<RespFrame>> for RespArray {
    fn from(s: Vec<RespFrame>) -> Self {
        RespArray(s)
    }
}
#[cfg(test)]
mod tests {
    use crate::{BulkString, SimpleString};
    use bytes::BytesMut;

    use super::*;
    use anyhow::Result;

    #[test]
    fn test_array_encode() {
        let frame: RespFrame = RespArray::new(vec![
            SimpleString::new("set".to_string()).into(),
            BulkString::new("hello".as_bytes().to_vec()).into(),
            BulkString::new("world".as_bytes().to_vec()).into(),
        ])
        .into();
        assert_eq!(frame.encode(), b"*3\r\n+set\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
    }
    #[test]
    fn test_null_array_encode() {
        let frame: RespFrame = RespNullArray.into();
        assert_eq!(frame.encode(), b"*-1\r\n");
    }
    #[test]
    fn test_null_array_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*-1\r\n");
        let frame = RespNullArray::decode(&mut buf)?;
        assert_eq!(frame, RespNullArray);
        Ok(())
    }
    /// 测试解析RESP数组的函数。
    /// 该函数模拟了一个简单的Redis协议解析场景，其中解析器需要从字节缓冲区中提取出一个RESP数组。
    /// 它首先构造了一个包含Redis命令的字节缓冲区，然后尝试使用`RespArray::decode`方法解析这个缓冲区。
    /// 最后，它将解码结果与预期值进行比较，以确保解码过程正确无误。
    #[test]
    fn test_array_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$3\r\nset\r\n$5\r\nhello\r\n");

        let frame = RespArray::decode(&mut buf)?;
        assert_eq!(frame, RespArray::new([b"set".into(), b"hello".into()]));

        buf.extend_from_slice(b"*2\r\n$3\r\nset\r\n");
        let ret = RespArray::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespError::NotComplete);

        buf.extend_from_slice(b"$5\r\nhello\r\n");
        let frame = RespArray::decode(&mut buf)?;
        assert_eq!(frame, RespArray::new([b"set".into(), b"hello".into()]));

        Ok(())
    }
}
