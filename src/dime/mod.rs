use quick_error::quick_error;

mod aes;
mod dime;

pub use dime::*;

quick_error! {
    #[derive(Debug, PartialEq)]
    pub enum DimeError {
        BufferSizeError(message: String) { }
        InvalidMagicBytesError(message: String) { }
        InvalidVersionError(version: u16) { }
        InvalidUuidSizeError(size: u32) { }
        Utf8Error(err: std::string::FromUtf8Error) {
            from()
        }
        ProstDecodeError(err: prost::DecodeError) {
            from()
        }
        SerdeDecodeError(message: String) {
            from()
        }
    }
}

pub type Result<T> = std::result::Result<T, DimeError>;
