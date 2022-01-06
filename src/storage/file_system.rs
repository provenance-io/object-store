use std::path::PathBuf;

use minitrace_macro::trace;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use crate::storage::{Result, Storage, StorageError, StoragePath};

#[derive(Debug)]
pub struct FileSystem {
    base_url: PathBuf,
}

impl FileSystem {
    pub fn new(base_url: &str) -> Self {
        FileSystem { base_url: PathBuf::from(base_url) }
    }

    fn get_path(&self, path: &StoragePath) -> PathBuf {
        let mut path_buf = self.base_url.clone();
        path_buf.push(&path.dir);
        path_buf.push(&path.file);
        path_buf
    }

    #[trace("file_system::create_dir")]
    async fn create_dir(&self, path: &StoragePath) -> Result<()> {
        let mut path_buf = self.base_url.clone();
        path_buf.push(&path.dir);

        match tokio::fs::create_dir(&path_buf).await {
            Ok(_) => Ok(()),
            Err(e) => match e.kind() {
                std::io::ErrorKind::AlreadyExists => Ok(()),
                _ => Err(StorageError::IoError(format!("{:?}", e))),
            }
        }
    }
}

#[async_trait::async_trait]
impl Storage for FileSystem {
    #[trace("file_system::store")]
    async fn store(&self, path: &StoragePath, content_length: u64, data: &[u8]) -> Result<()> {
        if let Err(e) = self.validate_content_length(&path, content_length, &data) {
            log::warn!("{:?}", e);
        }

        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.get_path(path))
            .await;

        match file {
            Ok(mut file) =>  {
                file.write_all(data).await
                    .map_err(|e| StorageError::IoError(format!("{:?}", e)))
            },
            Err(e) => match e.kind() {
                std::io::ErrorKind::AlreadyExists => Ok(()),
                std::io::ErrorKind::NotFound => {
                    self.create_dir(&path).await?;
                    self.store(&path, content_length, data).await
                },
                _ => Err(StorageError::IoError(format!("{:?}", e))),
            },
        }
    }

    #[trace("file_system::fetch")]
    async fn fetch(&self, path: &StoragePath, content_length: u64) -> Result<Vec<u8>> {
        let data = tokio::fs::read(self.get_path(path)).await
            .map_err(|e| StorageError::IoError(format!("{:?}", e)))?;

        if let Err(e) = self.validate_content_length(&path, content_length, &data) {
            log::warn!("{:?}", e);
        }

        Ok(data)
    }
}

#[cfg(test)]
mod tests {

    use crate::storage::*;

    use rand::{thread_rng, Rng};
    use rand::distributions::Alphanumeric;

    #[tokio::test]
    async fn store_file() {
        let storage = FileSystem { base_url: std::env::temp_dir() };
        let rand_string: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let path = StoragePath { dir: rand_string.clone(), file: rand_string };
        let data = b"hello world";

        assert!(storage.store(&path, 11_u64, data).await.is_ok());
    }

    #[tokio::test]
    async fn idempotent_store_file() {
        let storage = FileSystem { base_url: std::env::temp_dir() };
        let rand_string: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let path = StoragePath { dir: rand_string.clone(), file: rand_string };
        let data = b"hello world";
        let data_overwrite = b"world hello";

        assert!(storage.store(&path, 11_u64, data).await.is_ok());
        assert!(storage.store(&path, 11_u64, data_overwrite).await.is_ok());
        assert_eq!(storage.fetch(&path, 11_u64).await, Ok(data.to_vec()));
    }

    // #[tokio::test]
    // async fn store_invalid_length() {
    //     let storage = FileSystem { base_url: std::env::temp_dir() };
    //     let rand_string: String = thread_rng()
    //         .sample_iter(&Alphanumeric)
    //         .take(10)
    //         .map(char::from)
    //         .collect();
    //     let path = StoragePath { dir: rand_string.clone(), file: rand_string };
    //     let data = b"hello world";

    //     let result = storage.store(&path, 20_u64, data).await;
    //     let message = format!(
    //         "expected content length of {} and fetched content length of {} for {}/{}",
    //         20_u64,
    //         11_u64,
    //         &path.dir,
    //         &path.file,
    //     );
    //     assert_eq!(result, Err(StorageError::ContentLengthError(message)));
    // }

    // #[tokio::test]
    // async fn fetch_invalid_length() {
    //     let storage = FileSystem { base_url: std::env::temp_dir() };
    //     let rand_string: String = thread_rng()
    //         .sample_iter(&Alphanumeric)
    //         .take(10)
    //         .map(char::from)
    //         .collect();
    //     let path = StoragePath { dir: rand_string.clone(), file: rand_string };
    //     let data = b"hello world";

    //     assert!(storage.store(&path, 11_u64, data).await.is_ok());
    //     let result = storage.fetch(&path, 20_u64).await;
    //     let message = format!(
    //         "expected content length of {} and fetched content length of {} for {}/{}",
    //         20_u64,
    //         11_u64,
    //         &path.dir,
    //         &path.file,
    //     );
    //     assert_eq!(result, Err(StorageError::ContentLengthError(message)));
    // }

    #[tokio::test]
    async fn fetch_nonexistent_file() {
        let storage = FileSystem { base_url: std::env::temp_dir() };
        let rand_string: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let path = StoragePath { dir: rand_string, file: "nonexistent".to_owned() };

        assert!(storage.fetch(&path, 10_u64).await.is_err());
        assert!(storage.create_dir(&path).await.is_ok());
        assert!(storage.fetch(&path, 10_u64).await.is_err());
    }

    #[tokio::test]
    async fn fetch_file() {
        let storage = FileSystem { base_url: std::env::temp_dir() };
        let rand_string: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let path = StoragePath { dir: rand_string.clone(), file: rand_string };
        let data = b"hello world";

        assert!(storage.store(&path, 11_u64, data).await.is_ok());
        let result = storage.fetch(&path, 11_u64).await;
        assert_eq!(result, Ok(data.to_vec()));
    }

    #[tokio::test]
    async fn create_dir_that_exists() {
        let storage = FileSystem { base_url: std::env::temp_dir() };
        let rand_string: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let path = StoragePath { dir: rand_string.clone(), file: rand_string };

        assert!(storage.create_dir(&path).await.is_ok());
        assert!(storage.create_dir(&path).await.is_ok());
        assert!(storage.create_dir(&path).await.is_ok());
    }
}
