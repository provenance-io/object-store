use base64::{DecodeError, Engine, prelude::BASE64_STANDARD};

pub trait VecUtil {
    fn encoded(&self) -> String;
}

impl VecUtil for Vec<u8> {
    fn encoded(&self) -> String {
        BASE64_STANDARD.encode(self)
    }
}

pub trait StringUtil {
    fn decoded(&self) -> Result<Vec<u8>, DecodeError>;
}

impl StringUtil for String {
    fn decoded(&self) -> Result<Vec<u8>, DecodeError> {
        BASE64_STANDARD.decode(self)
    }
}
