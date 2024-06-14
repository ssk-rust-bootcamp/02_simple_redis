use crate::{Backend, RespArray, RespFrame, RespNull};

use super::{extract_args, validate_command, CommandError, CommandExecutor, Get, Set, RESP_OK};

impl CommandExecutor for Get {
    fn execute(self, backend: &Backend) -> RespFrame {
        match backend.get(&self.key) {
            Some(value) => value,
            None => RespFrame::Null(RespNull),
        }
    }
}
impl CommandExecutor for Set {
    fn execute(self, backend: &Backend) -> RespFrame {
        backend.set(self.key, self.value);
        RESP_OK.clone()
    }
}

impl TryFrom<RespArray> for Set {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["set"], 2)?;
        let mut args = extract_args(value, 1)?.into_iter();
        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(value)) => Ok(Set {
                key: String::from_utf8(key.0)?,
                value,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid Set  key or value".to_string())),
        }
    }
}

impl TryFrom<RespArray> for Get {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, &["get"], 1)?;
        let mut args = extract_args(value, 1)?.into_iter();
        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(Get {
                key: String::from_utf8(key.0)?,
            }),

            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::RespDecode;

    use super::*;
    use anyhow::Result;
    use bytes::BytesMut;
    #[test]
    fn test_get_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let result: Get = Get::try_from(frame)?;
        assert_eq!(result.key, "key");
        Ok(())
    }

    #[test]
    fn test_set_from_resp_array() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
        let frame = RespArray::decode(&mut buf)?;
        // println!("{:#?}", frame);
        let result: Set = frame.try_into()?;
        println!("{:#?}", result);
        assert_eq!(result.key, "key");
        assert_eq!(result.value, RespFrame::BulkString(b"value".into()));
        Ok(())
    }
    #[test]
    fn test_set_get_command() -> Result<()> {
        let backend = Backend::new();
        let cmd = Set {
            key: "key".to_string(),
            value: RespFrame::BulkString(b"value".into()),
        };
        let result = cmd.execute(&backend);
        assert_eq!(result, RESP_OK.clone());

        let cmd = Get { key: "key".to_string() };
        let result = cmd.execute(&backend);

        assert_eq!(result, RespFrame::BulkString(b"value".into()));

        Ok(())
    }
}
