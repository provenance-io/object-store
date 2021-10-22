use quick_error::quick_error;

mod file_system;
mod google_cloud;
mod storage;

// forwarding declarations
pub use file_system::FileSystem;
pub use google_cloud::GoogleCloud;
pub use storage::{Storage, StoragePath};

quick_error! {
    #[derive(Debug, PartialEq)]
    pub enum StorageError {
        IoError(message: String) { }
        ContentLengthError(message: String) { }
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;
