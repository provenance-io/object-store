use quick_error::quick_error;

use crate::types::OsError;

quick_error! {
    #[derive(Debug)]
    pub enum ReplicationError {
        Crate(err: OsError) {
            from()
        }
        TonicTransport(err: tonic::transport::Error) {
            from()
        }
        TonicStatus(err: tonic::Status) {
            from()
        }
        ClientCache(url: String) {
            display("no cached client found for {}", url)
        }
    }
}

pub type Result<T> = std::result::Result<T, ReplicationError>;
