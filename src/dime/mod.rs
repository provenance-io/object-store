use quick_error::quick_error;

mod dime;

// forwarding declarations
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
        // serde_json::Error is converted to a String because it does not implement PartialEq
        SerdeDecodeError(message: String) { }
        // serde_json::Error is converted to a String because it does not implement PartialEq
        SerdeEncodeError(message: String) { }
        InternalInvalidState(message: String) { }
    }
}

pub type Result<T> = std::result::Result<T, DimeError>;
