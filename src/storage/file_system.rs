use std::path::PathBuf;

use fastrace_macro::trace;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

use crate::storage::{Result, Storage, StorageError, StoragePath};

#[derive(Debug)]
pub struct FileSystem {
    base_url: PathBuf,
}

impl FileSystem {
    pub fn new(base_url: PathBuf) -> Self {
        FileSystem { base_url }
    }

    fn get_path(&self, path: &StoragePath) -> PathBuf {
        self.base_url.clone().join(&path.dir).join(&path.file)
    }

    #[trace(name = "file_system::create_dir")]
    async fn create_dir(&self, path: &StoragePath) -> Result<()> {
        let path_buf = self.base_url.clone().join(&path.dir);

        match tokio::fs::create_dir(&path_buf).await {
            Ok(_) => Ok(()),
            Err(e) => match e.kind() {
                std::io::ErrorKind::AlreadyExists => Ok(()),
                _ => Err(StorageError::IoError(format!("{:?} - {:?}", e, path_buf))),
            },
        }
    }
}

#[async_trait::async_trait]
impl Storage for FileSystem {
    #[trace(name = "file_system::health_check")]
    async fn health_check(&self) -> Result<()> {
        let test_file = StoragePath {
            dir: "test".to_owned(),
            file: "test.txt".to_owned(),
        };

        self.store(&test_file, 12, b"fs connected").await?;

        let response = self.fetch(&test_file, 12).await?;

        let contents = String::from_utf8(response)?;

        log::debug!("health check - {} contents: {}", test_file, contents);

        // Ignore delete errors
        let _ = self.delete(&test_file).await;

        Ok(())
    }

    #[trace(name = "file_system::store")]
    async fn store(&self, path: &StoragePath, content_length: usize, data: &[u8]) -> Result<()> {
        if let Err(e) = self.validate_content_length(path, content_length, data) {
            log::warn!("{:?}", e);
        }

        let file_path = self.get_path(path);

        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&file_path)
            .await;

        match file {
            Ok(mut file) => file
                .write_all(data)
                .await
                .map_err(|e| StorageError::IoError(format!("{:?}: {:?}", e, file_path))),
            Err(e) => match e.kind() {
                std::io::ErrorKind::AlreadyExists => Ok(()),
                std::io::ErrorKind::NotFound => {
                    self.create_dir(path).await?;
                    self.store(path, content_length, data).await
                }
                _ => Err(StorageError::IoError(format!("{:?}: {:?}", e, file_path))),
            },
        }
    }

    #[trace(name = "file_system::fetch")]
    async fn fetch(&self, path: &StoragePath, content_length: usize) -> Result<Vec<u8>> {
        let full_path = self.get_path(path);
        let absolute_path = full_path.canonicalize().unwrap_or(full_path.clone());

        let data = tokio::fs::read(full_path).await.map_err(|e| {
            StorageError::IoError(format!("Unable to fetch file: {:?} {:?}", absolute_path, e))
        })?;

        if let Err(e) = self.validate_content_length(path, content_length, &data) {
            log::warn!("{:?}", e);
        }

        Ok(data)
    }

    #[trace(name = "file_system::delete")]
    async fn delete(&self, path: &StoragePath) -> Result<()> {
        let full_path = self.get_path(path);
        let absolute_path = full_path.canonicalize().unwrap_or(full_path.clone());

        tokio::fs::remove_file(full_path).await.map_err(|e| {
            StorageError::IoError(format!(
                "Unable to delete file: {:?} {:?}",
                absolute_path, e
            ))
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::storage::*;

    use rand::distr::Alphanumeric;
    use rand::{Rng, rng};

    #[tokio::test]
    async fn store_file() {
        let storage = FileSystem {
            base_url: std::env::temp_dir(),
        };
        let rand_string: String = rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let path = StoragePath {
            dir: rand_string.clone(),
            file: rand_string,
        };
        let data = b"hello world";

        assert!(storage.store(&path, 11, data).await.is_ok());
    }

    #[tokio::test]
    async fn idempotent_store_file() {
        let storage = FileSystem {
            base_url: std::env::temp_dir(),
        };
        let rand_string: String = rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let path = StoragePath {
            dir: rand_string.clone(),
            file: rand_string,
        };
        let data = b"hello world";
        let data_overwrite = b"world hello";

        assert!(storage.store(&path, 11, data).await.is_ok());
        assert!(storage.store(&path, 11, data_overwrite).await.is_ok());
        assert_eq!(storage.fetch(&path, 11).await, Ok(data.to_vec()));
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
        let storage = FileSystem {
            base_url: std::env::temp_dir(),
        };
        let rand_string: String = rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let path = StoragePath {
            dir: rand_string,
            file: "nonexistent".to_owned(),
        };

        assert!(storage.fetch(&path, 10).await.is_err());
        assert!(storage.create_dir(&path).await.is_ok());
        assert!(storage.fetch(&path, 10).await.is_err());
    }

    #[tokio::test]
    async fn fetch_file() {
        let storage = FileSystem {
            base_url: std::env::temp_dir(),
        };
        let rand_string: String = rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let path = StoragePath {
            dir: rand_string.clone(),
            file: rand_string,
        };
        let data = b"hello world";

        assert!(storage.store(&path, 11, data).await.is_ok());
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let result = storage.fetch(&path, 11).await;
        assert_eq!(result, Ok(data.to_vec()));
    }

    #[tokio::test]
    async fn create_dir_that_exists() {
        let storage = FileSystem {
            base_url: std::env::temp_dir(),
        };
        let rand_string: String = rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        let path = StoragePath {
            dir: rand_string.clone(),
            file: rand_string,
        };

        assert!(storage.create_dir(&path).await.is_ok());
        assert!(storage.create_dir(&path).await.is_ok());
        assert!(storage.create_dir(&path).await.is_ok());
    }
}
