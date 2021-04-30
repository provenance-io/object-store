use crate::dime::DimeError;

use quick_error::quick_error;

quick_error! {
    #[derive(Debug)]
    pub enum OsError {
        AddrParseError(err: std::net::AddrParseError) {
            from()
        }
        ProstEncodeError(err: prost::EncodeError) {
            from()
        }
        SqlError(err: sqlx::Error) {
            from()
        }
        SqlMigrateError(err: sqlx::migrate::MigrateError) {
            from()
        }
        TonicTransportError(err: tonic::transport::Error) {
            from()
        }
        DimeError(err: crate::dime::DimeError) {
            from()
        }
        Utf8Error(err: std::string::FromUtf8Error) {
            from()
        }
        Base64DecodeError(err: base64::DecodeError) {
            from()
        }
        NotFound(message: String) { }
        InvalidSignatureState(message: String) { }
        InvalidApplicationState(message: String) { }
        StorageError(err: crate::storage::StorageError) {
            from()
        }
    }
}

pub type Result<T> = std::result::Result<T, OsError>;
pub type GrpcResult<T> = std::result::Result<T, tonic::Status>;

impl From<OsError> for tonic::Status {
    fn from(error: OsError) -> Self {
        let code = match error {
            OsError::AddrParseError(_) => tonic::Code::Internal,
            OsError::ProstEncodeError(_) => tonic::Code::Internal,
            OsError::SqlError(_) => tonic::Code::Internal,
            OsError::SqlMigrateError(_) => tonic::Code::Internal,
            OsError::TonicTransportError(_) => tonic::Code::Internal,
            OsError::DimeError(ref inner_error) => match inner_error {
                DimeError::BufferSizeError(_) => tonic::Code::InvalidArgument,
                DimeError::InvalidMagicBytesError(_) => tonic::Code::InvalidArgument,
                DimeError::InvalidVersionError(_) => tonic::Code::InvalidArgument,
                DimeError::InvalidUuidSizeError(_) => tonic::Code::InvalidArgument,
                DimeError::Utf8Error(_) => tonic::Code::InvalidArgument,
                DimeError::ProstDecodeError(_) => tonic::Code::InvalidArgument,
                DimeError::SerdeDecodeError(_) => tonic::Code::InvalidArgument,
                DimeError::SerdeEncodeError(_) => tonic::Code::Internal,
                DimeError::InternalInvalidState(_) => tonic::Code::Internal,
            },
            OsError::Utf8Error(_) => tonic::Code::InvalidArgument,
            OsError::Base64DecodeError(_) => tonic::Code::Internal,
            OsError::NotFound(_) => tonic::Code::NotFound,
            OsError::InvalidSignatureState(_) => tonic::Code::Internal,
            OsError::InvalidApplicationState(_) => tonic::Code::Internal,
            OsError::StorageError(_) => tonic::Code::Internal,
        };

        tonic::Status::new(code, format!("{:?}", error))
    }
}
