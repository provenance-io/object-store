use quick_error::quick_error;

use crate::types::OsError;

quick_error! {
    #[derive(Debug)]
    pub enum ReplicationError {
        CrateError(err: OsError) {
            from()
        }
        TonicTransportError(err: tonic::transport::Error) {
            from()
        }
        TonicStatusError(err: tonic::Status) {
            from()
        }
        ClientCacheError(url: String) {
            display("no cached client found for {}", url)
        }
    }
}

pub type Result<T> = std::result::Result<T, ReplicationError>;
