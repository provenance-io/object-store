use quick_error::quick_error;

mod file_system;
mod storage;

// forwarding declarations
pub use storage::{Storage, StoragePath};
pub use file_system::FileSystem;

quick_error! {
    #[derive(Debug, PartialEq)]
    pub enum StorageError {
        IoError(message: String) { }
        ContentLengthError(message: String) { }
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;
