use quick_error::quick_error;

quick_error! {
    #[derive(Debug, PartialEq)]
    pub enum StorageError {
        IoError(message: String) { }
        ContentLengthError(message: String) { }
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;
