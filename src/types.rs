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
        DimeInputError(err: crate::dime::DimeInputError) {
            from()
        }
        DimeOutputError(err: crate::dime::DimeOutputError) {
            from()
        }
        Utf8Error(err: std::string::FromUtf8Error) {
            from()
        }
        NotFound(err: String) { }
        InvalidSignatureState(err: String) { }
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
            OsError::DimeInputError(_) => tonic::Code::InvalidArgument,
            OsError::DimeOutputError(_) => tonic::Code::Internal,
            OsError::Utf8Error(_) => tonic::Code::InvalidArgument,
            OsError::NotFound(_) => tonic::Code::NotFound,
            OsError::InvalidSignatureState(_) => tonic::Code::Internal,
        };

        tonic::Status::new(code, format!("{:?}", error))
    }
}
